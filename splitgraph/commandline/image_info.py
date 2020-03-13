"""
sgr commands related to getting information out of / about images
"""

from collections import Counter
from typing import List, Optional, Tuple, Union, Dict, cast, TYPE_CHECKING, Any

import click

from splitgraph.core.common import pretty_size
from .common import ImageType, pluralise, RepositoryType

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository


@click.command(name="log")
@click.argument("repository", type=RepositoryType(exists=True))
@click.option("-t", "--tree", is_flag=True)
def log_c(repository, tree):
    """
    Show the history of a Splitgraph repository.

    By default, this shows the history of the current branch, starting from the HEAD pointer and following its
    parent chain.

    If ``-t`` or ``--tree`` is passed, this instead renders the full image tree. The repository doesn't need to have
    been checked out in this case.
    """
    from splitgraph.core._drawing import render_tree

    if tree:
        render_tree(repository)
    else:
        head = repository.head
        log = head.get_log()
        for entry in log:
            click.echo(
                "%s %s %s %s"
                % (
                    "H->" if entry == head else "   ",
                    entry.image_hash,
                    entry.created,
                    entry.comment or "",
                )
            )


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

    diffs: Dict[str, Union[bool, Tuple[int, int, int], List[Tuple[bool, Tuple]]]] = {
        table_name: repository.diff(table_name, tag_or_hash_1, tag_or_hash_2, aggregate=not verbose)
        for table_name in (
            [table_name]
            if table_name
            else sorted(repository.engine.get_all_tables(repository.to_schema()))
        )
    }

    if tag_or_hash_2 is None:
        click.echo("Between %s and the current working copy: " % tag_or_hash_1[:12])
    else:
        click.echo("Between %s and %s: " % (tag_or_hash_1[:12], tag_or_hash_2[:12]))

    for table, diff_result in diffs.items():
        _emit_table_diff(table, diff_result, verbose)


def _emit_table_diff(
    table_name: str,
    diff_result: Union[bool, Tuple[int, int, int], List[Tuple[bool, Tuple]]],
    verbose: bool,
) -> None:
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
            click.echo("%s has no parent to compare to!" % image_1)
        image_1, image_2 = image_2, image_1  # snap_1 has to come first
    else:
        assert image_1 is not None
        assert image_2 is not None
        image_1 = repository.images[image_1].image_hash
        image_2 = repository.images[image_2].image_hash
    return image_1, image_2


@click.command(name="show")
@click.argument("image_spec", type=ImageType(default="HEAD", get_image=True))
def show_c(image_spec):
    """
    Show information about a Splitgraph image. This includes its parent, comment, size and creation time.

    Note that the size isn't the on-disk footprint, as the image might share some objects with other images
    or if some of the image's objects have not been downloaded.

    Image spec must be of the format ``[NAMESPACE/]REPOSITORY[:HASH_OR_TAG]``. If no tag is specified, ``HEAD`` is used.
    """
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
    repository, image = image_spec
    table = image.get_table(table_name)
    click.echo("Table %s:%s/%s" % (repository.to_schema(), image.image_hash, table_name))
    click.echo()
    click.echo("Size: %s" % pretty_size(table.get_size()))
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
    from splitgraph.core.common import pretty_size
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
    click.echo("Insertion hash: %s" % sg_object.insertion_hash)
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


def _to_str(results: List[Tuple[Any]]) -> str:
    return "\n".join("\t".join(str(t) for t in ts) for ts in results)


@click.command(name="sql")
@click.argument("sql")
@click.option("-s", "--schema", help="Run SQL against this schema.")
@click.option(
    "-i",
    "--image",
    help="Run SQL against this image.",
    type=ImageType(default="latest", get_image=True),
)
@click.option("-a", "--show-all", is_flag=True, help="Returns all results of the query.")
def sql_c(sql, schema, image, show_all):
    """
    Run an SQL statement against the Splitgraph engine.

    There are no restrictions on the contents of the statement: this is the same as running it
    from any other PostgreSQL client.

    If ``--schema`` is specified, the statement is run with the ``search_path`` set to that schema. This means
    that these statements are equivalent::

        sgr sql "SELECT * FROM \"noaa/climate\".table"
        sgr sql -s noaa/climate "SELECT * FROM table"

    If `--image` is specified, this will run the statement against that image using layered querying.
    Only read-only statements are supported. For example:

        sgr sql -i noaa/climate:latest "SELECT * FROM table"
    """
    from splitgraph.engine import get_engine

    if schema and image:
        raise click.UsageError("Only one of --schema and --image can be specified!")

    if not image:
        if schema:
            get_engine().run_sql("SET search_path TO %s", (schema,))
        results = get_engine().run_sql(sql)
    else:
        repo, image = image
        with image.query_schema() as s:
            results = get_engine().run_sql_in(s, sql)

    if results is None:
        return

    if len(results) > 10 and not show_all:
        click.echo(_to_str(results[:10]))
        click.echo("...")
    else:
        click.echo(_to_str(results))


@click.command(name="status")
@click.argument("repository", required=False, type=RepositoryType(exists=True))
def status_c(repository):
    """
    Show the status of the Splitgraph engine. If a repository is passed, show information about
    the repository. If not, show information about all repositories local to the engine.
    """
    from splitgraph.core.engine import get_current_repositories
    from splitgraph.engine import get_engine

    if repository is None:
        repositories = get_current_repositories(get_engine())
        click.echo("Local repositories and checked out images: ")
        for mp_name, mp_head in repositories:
            # Maybe should also show the remote DB address/server
            click.echo("%s: \t %s" % (mp_name, mp_head.image_hash if mp_head else None))
        click.echo("\nUse sgr status repository to get information about a given repository.")
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
