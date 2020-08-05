"""
sgr commands related to getting information out of / about images
"""

from collections import Counter, defaultdict
from typing import List, Optional, Tuple, Union, Dict, cast, TYPE_CHECKING

import click

from .common import ImageType, RepositoryType, remote_switch_option, emit_sql_results
from ..core._drawing import format_image_hash, format_tags, format_time

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository


@click.command(name="log")
@click.argument("image_spec", type=ImageType(get_image=False, default="HEAD"))
@click.option("-t", "--tree", is_flag=True)
@remote_switch_option()
def log_c(image_spec, tree):
    """
    Show the history of a Splitgraph repository/image.

    By default, this shows the history of the current branch, starting from the HEAD pointer and following its
    parent chain.

    Alternatively, it can follow the parent chain of any other image.

    If ``-t`` or ``--tree`` is passed, this instead renders the full image tree. The repository doesn't need to have
    been checked out in this case.
    """
    from splitgraph.core._drawing import render_tree
    from ..core.output import truncate_line
    from tabulate import tabulate

    repository, hash_or_tag = image_spec

    if tree:
        render_tree(repository)
    else:
        latest = repository.images["latest"]

        tag_dict = defaultdict(list)
        for img, img_tag in repository.get_all_hashes_tags():
            tag_dict[img].append(img_tag)
        tag_dict[latest.image_hash].append("latest")

        image = repository.images[hash_or_tag]
        log = image.get_log()
        table = []
        for entry in log:
            table.append(
                (
                    format_image_hash(entry.image_hash),
                    format_tags(tag_dict[entry.image_hash]),
                    format_time(entry.created),
                    truncate_line(entry.comment or ""),
                )
            )
        click.echo(tabulate(table, tablefmt="plain"))


def _get_all_tables(repository: "Repository", image_hash: Optional[str]):
    if image_hash is None:
        return repository.engine.get_all_tables(repository.to_schema())
    return repository.images[image_hash].get_tables()


@click.command(name="diff")
@click.option(
    "-v",
    "--verbose",
    default=False,
    is_flag=True,
    help="Include the actual differences rather than just the total number of updated rows.",
)
@click.option("-t", "--table-name", help="Show the differences for a single table.")
@click.argument("repository", type=RepositoryType())
@click.argument("tag_or_hash_1", required=False)
@click.argument("tag_or_hash_2", required=False)
def diff_c(verbose, table_name, repository, tag_or_hash_1, tag_or_hash_2):
    """
    Show differences between two Splitgraph images.

    The two images must be in the same repository. The actual targets of this command depend
    on the number of arguments passed:

    ``sgr diff REPOSITORY``

    Return the differences between the current HEAD image and the checked out schema.

    ``sgr diff REPOSITORY TAG_OR_HASH``

    Return the differences between the image and its parent.

    ``sgr diff REPOSITORY TAG_OR_HASH_1 TAG_OR_HASH_2``

    Return the differences from the first (earlier) image to the second image.
    """
    tag_or_hash_1, tag_or_hash_2 = _get_actual_hashes(repository, tag_or_hash_1, tag_or_hash_2)

    if table_name:
        all_tables = [table_name]
    else:
        all_tables = sorted(
            set(_get_all_tables(repository, tag_or_hash_1)).union(
                set(_get_all_tables(repository, tag_or_hash_2))
            )
        )

    diffs: Dict[str, Union[bool, Tuple[int, int, int], List[Tuple[bool, Tuple]]]] = {
        table_name: repository.diff(table_name, tag_or_hash_1, tag_or_hash_2, aggregate=not verbose)
        for table_name in all_tables
    }

    if tag_or_hash_2 is None:
        click.echo("Between %s and the current working copy: " % tag_or_hash_1[:12])
    else:
        click.echo("Between %s and %s: " % (tag_or_hash_1[:12], tag_or_hash_2[:12]))

    for table, diff_result in diffs.items():
        _emit_table_diff(table, diff_result, verbose)


def _emit_table_diff(
    table_name: str,
    diff_result: Union[bool, Tuple[int, int, int], List[Tuple[bool, Tuple]], None],
    verbose: bool,
) -> None:
    from ..core.output import pluralise

    to_print = "%s: " % table_name
    if isinstance(diff_result, (list, tuple)):
        if verbose:
            change_count = dict(
                Counter(d[0] for d in cast(List[Tuple[bool, Tuple]], diff_result)).most_common()
            )
            added = change_count.get(True, 0)
            removed = change_count.get(False, 0)
            updated = 0
        else:
            added, removed, updated = cast(Tuple[int, int, int], diff_result)

        count = []
        if added:
            count.append("added " + pluralise("row", added))
        if removed:
            count.append("removed " + pluralise("row", removed))
        if updated:
            count.append("updated " + pluralise("row", updated))
        if added + removed + updated == 0:
            count = ["no changes"]
        click.echo(to_print + ", ".join(count) + ".")

        if verbose:
            for added, row in cast(List[Tuple[bool, Tuple]], diff_result):
                click.echo(("+" if added else "-") + " %r" % (row,))
    elif diff_result is None:
        click.echo(to_print + "untracked")
    else:
        # Whole table was either added or removed
        click.echo(to_print + ("table added" if diff_result else "table removed"))


def _get_actual_hashes(
    repository: "Repository", image_1: Optional[str], image_2: Optional[str]
) -> Tuple[str, Optional[str]]:
    if image_1 is None and image_2 is None:
        # Comparing current working copy against the last commit
        image_1 = repository.head_strict.image_hash
    elif image_1 is not None and image_2 is None:
        image_1_obj = repository.images[image_1]
        image_1 = image_1_obj.image_hash
        # One parameter: diff from that and its parent.
        image_2 = image_1_obj.parent_id
        if image_2 is None:
            raise ValueError("%s has no parent to compare to!" % image_1)
        image_1, image_2 = image_2, image_1  # snap_1 has to come first
    else:
        assert image_1 is not None
        assert image_2 is not None
        image_1 = repository.images[image_1].image_hash
        image_2 = repository.images[image_2].image_hash
    return image_1, image_2


@click.command(name="show")
@remote_switch_option()
@click.argument("image_spec", type=ImageType(default="HEAD", get_image=True))
def show_c(image_spec):
    """
    Show information about a Splitgraph image. This includes its parent, comment, size and creation time.

    Note that the size isn't the on-disk footprint, as the image might share some objects with other images
    or if some of the image's objects have not been downloaded.

    Image spec must be of the format ``[NAMESPACE/]REPOSITORY[:HASH_OR_TAG]``. If no tag is specified, ``HEAD`` is used.
    """
    from ..core.output import pretty_size

    repository, image = image_spec

    click.echo("Image %s:%s" % (repository.to_schema(), image.image_hash))
    click.echo(image.comment or "")
    click.echo("Created at %s" % image.created.isoformat())
    click.echo("Size: %s" % pretty_size(image.get_size()))
    if image.parent_id:
        click.echo("Parent: %s" % image.parent_id)
    else:
        click.echo("No parent (root image)")
    click.echo()
    click.echo("Tables:")
    for table in image.get_tables():
        click.echo("  %s" % table)


@click.command(name="table")
@remote_switch_option()
@click.argument("image_spec", type=ImageType(default="HEAD", get_image=True))
@click.argument("table_name", type=str)
@click.option(
    "-v", "--verbose", type=bool, is_flag=True, default=False, help="Show all of table's objects."
)
def table_c(image_spec, table_name, verbose):
    """
    Show information about a table in a Splitgraph image.

    Image spec must be of the format ``[NAMESPACE/]REPOSITORY[:HASH_OR_TAG]``. If no tag is specified, ``HEAD`` is used.
    """
    from ..core.output import pretty_size

    repository, image = image_spec
    table = image.get_table(table_name)
    click.echo("Table %s:%s/%s" % (repository.to_schema(), image.image_hash, table_name))
    click.echo()
    click.echo("Size: %s" % pretty_size(table.get_size()))
    click.echo("Rows: %d" % table.get_length())
    click.echo("Columns: ")
    for column in table.table_schema:
        column_str = "  " + column.name + " (" + column.pg_type
        if column.is_pk:
            column_str += ", PK"
        column_str += ")"
        if column.comment:
            column_str += ": " + column.comment
        click.echo(column_str)

    click.echo()
    click.echo("Objects: ")
    if len(table.objects) > 10 and not verbose:
        click.echo("  " + "\n  ".join(table.objects[:10] + ["..."]))
    else:
        click.echo("  " + "\n  ".join(table.objects))


@click.command(name="object")
@remote_switch_option()
@click.argument("object_id", type=str)
def object_c(object_id):
    """
    Show information about a Splitgraph object.

    Objects, or fragments, are building blocks of Splitgraph tables: each table consists of multiple immutable fragments
    that can partially overwrite each other. Each fragment might have a parent that it depends on. In addition,
    the smallest and largest values for every column are stored in the fragment's metadata. This information is used
    to choose which objects to download in order to execute a query against a table.
    """
    from splitgraph.core.object_manager import ObjectManager
    from splitgraph.engine import get_engine, ResultShape
    from ..core.output import pretty_size
    from ..core.sql import select
    from splitgraph.core.indexing.bloom import describe

    object_manager = ObjectManager(get_engine())
    object_meta = object_manager.get_object_meta([object_id])
    if not object_meta:
        raise click.BadParameter("Object %s does not exist!" % object_id)

    sg_object = object_meta[object_id]
    click.echo("Object ID: %s" % object_id)
    click.echo()
    click.echo("Namespace: %s" % sg_object.namespace)
    click.echo("Format: %s" % sg_object.format)
    click.echo("Size: %s" % pretty_size(sg_object.size))
    click.echo("Created: %s" % sg_object.created)
    click.echo("Rows inserted: %s" % sg_object.rows_inserted)
    click.echo("Insertion hash: %s" % sg_object.insertion_hash)
    click.echo("Rows deleted: %s" % sg_object.rows_deleted)
    click.echo("Deletion hash: %s" % sg_object.deletion_hash)
    click.echo("Column index:")
    for col_name, col_range in sg_object.object_index["range"].items():
        click.echo("  %s: [%r, %r]" % (col_name, col_range[0], col_range[1]))
    if "bloom" in sg_object.object_index:
        click.echo("Bloom index: ")
        for col_name, col_bloom in sg_object.object_index["bloom"].items():
            click.echo("  %s: %s" % (col_name, describe(col_bloom)))

    if object_manager.object_engine.registry:
        # Don't try to figure out the object's location if we're talking
        # to the registry.
        return

    click.echo()
    object_in_cache = object_manager.object_engine.run_sql(
        select("object_cache_status", "1", "object_id = %s"),
        (object_id,),
        return_shape=ResultShape.ONE_ONE,
    )
    object_downloaded = object_id in object_manager.get_downloaded_objects(limit_to=[object_id])
    object_external = object_manager.get_external_object_locations([object_id])

    if object_downloaded and not object_in_cache:
        click.echo("Location: created locally")
    else:
        original_location = (
            ("%s (%s)" % (object_external[0][1], object_external[0][2]))
            if object_external
            else "remote engine"
        )
        if object_in_cache:
            click.echo("Location: cached locally")
            click.echo("Original location: " + original_location)
        else:
            click.echo("Location: " + original_location)


@click.command(name="objects")
@click.option(
    "--local", is_flag=True, help="Show only objects that are physically present on this engine"
)
def objects_c(local):
    """
    List objects known to this engine.
    """
    from splitgraph.core.object_manager import ObjectManager
    from splitgraph.engine import get_engine

    om = ObjectManager(get_engine())

    if local:
        objects = om.get_downloaded_objects()
    else:
        objects = om.get_all_objects()

    click.echo("\n".join(sorted(objects)))


@click.command(name="sql")
@click.argument("sql")
@remote_switch_option()
@click.option("-s", "--schema", help="Run SQL against this schema.")
@click.option(
    "-i",
    "--image",
    help="Run SQL against this image.",
    type=ImageType(default="latest", get_image=True),
)
@click.option("-a", "--show-all", is_flag=True, help="Return all results of the query.")
@click.option("-j", "--json", is_flag=True, help="Return results as JSON")
@click.option("-n", "--no-transaction", is_flag=True, help="Don't wrap the SQL in a transaction.")
def sql_c(sql, schema, image, show_all, json, no_transaction):
    """
    Run an SQL statement against the Splitgraph engine.

    There are no restrictions on the contents of the statement: this is the same as running it
    from any other PostgreSQL client.

    If ``--schema`` is specified, the statement is run with the ``search_path`` set to that schema. This means
    that these statements are equivalent:

    \b
    ```
    sgr sql "SELECT * FROM \"noaa/climate\".table"
    sgr sql -s noaa/climate "SELECT * FROM table"
    ```

    If `--image` is specified, this will run the statement against that image using layered querying.
    Only read-only statements are supported. For example:

    \b
    ```
    sgr sql -i noaa/climate:latest "SELECT * FROM table"
    ```
    """
    from splitgraph.engine import get_engine

    if schema and image:
        raise click.UsageError("Only one of --schema and --image can be specified!")

    engine = get_engine()
    if no_transaction:
        engine.autocommit = True

    if not image:
        if schema:
            engine.run_sql("SET search_path TO %s", (schema,))
        results = engine.run_sql(sql)
    else:
        repo, image = image
        with image.query_schema() as s:
            results = engine.run_sql_in(s, sql)

    emit_sql_results(results, use_json=json, show_all=show_all)


def _emit_repository_data(repositories, engine):
    from splitgraph.engine import ResultShape
    from tabulate import tabulate
    from ..core.output import pretty_size

    click.echo("Local repositories: \n")

    table = []
    for repo, head in repositories:
        no_images = len(repo.images())
        all_tags = repo.get_all_hashes_tags()
        no_tags = len([i for i, t in all_tags if t != "HEAD"])
        metadata_size = pretty_size(repo.get_size())
        actual_size = pretty_size(repo.get_local_size())
        if head:
            head = head.image_hash[:10]
            if (
                engine.run_sql(
                    "SELECT 1 FROM pg_foreign_server WHERE srvname = %s",
                    (("%s_lq_checkout_server" % repo.to_schema())[:63],),
                    return_shape=ResultShape.ONE_ONE,
                )
                is not None
            ):
                head += " (LQ)"
        else:
            head = "--"

        upstream = repo.upstream
        if upstream:
            upstream_text = "%s (%s)" % (upstream.to_schema(), upstream.engine.name)
        else:
            upstream_text = "--"

        table.append(
            (repo.to_schema(), no_images, no_tags, metadata_size, actual_size, head, upstream_text)
        )

    click.echo(
        tabulate(
            table,
            headers=[
                "Repository",
                "Images",
                "Tags",
                "Size (T)",
                "Size (A)",
                "Checkout",
                "Upstream",
            ],
        )
    )
    click.echo("\nUse sgr status REPOSITORY to get information about a given repository.")
    click.echo("Use sgr show REPOSITORY:[HASH_OR_TAG] to get information about a given image.")


@click.command(name="status")
@click.argument("repository", required=False, type=RepositoryType(exists=True))
def status_c(repository):
    """
    Show the status of the Splitgraph engine.

    If a repository is passed, show in-depth information about a repository.

    If not, show information about all repositories local to the engine. This will show a list
    of all repositories, number of local images and tags, total repository size (theoretical
    maximum size and current on-disk footprint of cached objects) and the current checked
    out image (with LQ if the image is checked out using read-only layered querying).
    """
    from splitgraph.core.engine import get_current_repositories
    from splitgraph.engine import get_engine

    if repository is None:
        engine = get_engine()
        repositories = get_current_repositories(engine)
        _emit_repository_data(repositories, engine)
    else:
        head = repository.head
        if not head:
            click.echo("%s: nothing checked out." % str(repository))
            return
        parent, children = head.get_parent_children()
        click.echo("%s: on image %s." % (str(repository), head.image_hash))
        if parent is not None:
            click.echo("Parent: %s" % parent)
        if len(children) > 1:
            click.echo("Children: " + "\n".join(children))
        elif len(children) == 1:
            click.echo("Child: %s" % children[0])
