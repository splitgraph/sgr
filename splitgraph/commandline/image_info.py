"""
sgr commands related to getting information out of / about images
"""

from collections import Counter
from pprint import pprint

import click

from splitgraph import get_engine, select, ResultShape
from splitgraph.core._common import pretty_size
from splitgraph.core._drawing import render_tree
from splitgraph.core.engine import get_current_repositories
from splitgraph.core.object_manager import ObjectManager
from ._common import ImageType, pluralise, RepositoryType


@click.command(name="log")
@click.argument("repository", type=RepositoryType())
@click.option("-t", "--tree", is_flag=True)
def log_c(repository, tree):
    """
    Show the history of a Splitgraph repository.

    By default, this shows the history of the current branch, starting from the HEAD pointer and following its
    parent chain.

    If ``-t`` or ``--tree`` is passed, this instead renders the full image tree. The repository doesn't need to have
    been checked out in this case.
    """
    if tree:
        render_tree(repository)
    else:
        head = repository.head
        log = head.get_log()
        for entry in log:
            print(
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

    diffs = {
        table_name: repository.diff(table_name, tag_or_hash_1, tag_or_hash_2, aggregate=not verbose)
        for table_name in (
            [table_name]
            if table_name
            else sorted(repository.engine.get_all_tables(repository.to_schema()))
        )
    }

    if tag_or_hash_2 is None:
        print("Between %s and the current working copy: " % tag_or_hash_1[:12])
    else:
        print("Between %s and %s: " % (tag_or_hash_1[:12], tag_or_hash_2[:12]))

    for table, diff_result in diffs.items():
        _emit_table_diff(table, diff_result, verbose)


def _emit_table_diff(table_name, diff_result, verbose):
    to_print = "%s: " % table_name
    if isinstance(diff_result, (list, tuple)):
        if verbose:
            change_count = dict(Counter(d[0] for d in diff_result).most_common())
            added = change_count.get(True, 0)
            removed = change_count.get(False, 0)
            updated = 0
        else:
            added, removed, updated = diff_result

        count = []
        if added:
            count.append("added " + pluralise("row", added))
        if removed:
            count.append("removed " + pluralise("row", removed))
        if updated:
            count.append("updated " + pluralise("row", removed))
        if added + removed + updated == 0:
            count = ["no changes"]
        print(to_print + ", ".join(count) + ".")

        if verbose:
            for added, row in diff_result:
                print(("+" if added else "-") + " %r" % (row,))
    else:
        # Whole table was either added or removed
        print(to_print + ("table added" if diff_result else "table removed"))


def _get_actual_hashes(repository, image_1, image_2):
    if image_1 is None and image_2 is None:
        # Comparing current working copy against the last commit
        image_1 = repository.head.image_hash
    elif image_2 is None:
        image_1_obj = repository.images[image_1]
        image_1 = image_1_obj.image_hash
        # One parameter: diff from that and its parent.
        image_2 = image_1_obj.parent_id
        if image_2 is None:
            print("%s has no parent to compare to!" % image_1)
        image_1, image_2 = image_2, image_1  # snap_1 has to come first
    else:
        image_1 = repository.images[image_1].image_hash
        image_2 = repository.images[image_2].image_hash
    return image_1, image_2


@click.command(name="show")
@click.argument("image_spec", type=ImageType(default="HEAD"))
@click.option(
    "-v",
    "--verbose",
    default=False,
    is_flag=True,
    help="Also show all tables in this image and the objects they map to.",
)
def show_c(image_spec, verbose):
    """
    Show information about a Splitgraph image. This includes its parent, comment and creation time.

    Image spec must be of the format ``[NAMESPACE/]REPOSITORY[:HASH_OR_TAG]``. If no tag is specified, ``HEAD`` is used.
    """
    repository, image = image_spec
    image = repository.images[image]

    print("Image %s:%s" % (repository.to_schema(), image.image_hash))
    print(image.comment or "")
    print("Created at %s" % image.created.isoformat())
    if image.parent_id:
        print("Parent: %s" % image.parent_id)
    else:
        print("No parent (root image)")
    if verbose:
        print()
        print("Tables:")
        for table in image.get_tables():
            table_objects = image.get_table(table).objects
            if len(table_objects) == 1:
                print("  %s: %s" % (table, table_objects[0]))
            else:
                print("  %s:" % table)
                for obj in table_objects:
                    print("    %s" % obj)


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
    object_manager = ObjectManager(get_engine())
    object_meta = object_manager.get_object_meta([object_id])
    if not object_meta:
        raise click.BadParameter("Object %s does not exist!" % object_id)

    sg_object = object_meta[object_id]
    print("Object ID: %s" % object_id)
    print()
    print("Parent: %s" % sg_object.parent_id)
    print("Namespace: %s" % sg_object.namespace)
    print("Format: %s" % sg_object.format)
    print("Size: %s" % pretty_size(sg_object.size))
    print("Insertion hash: %s" % sg_object.insertion_hash)
    print("Deletion hash: %s" % sg_object.deletion_hash)
    print("Column index:")
    for col_name, col_range in sg_object.index.items():
        print("  %s: [%r, %r]" % (col_name, col_range[0], col_range[1]))
    print()
    object_in_cache = object_manager.object_engine.run_sql(
        select("object_cache_status", "1", "object_id = %s"),
        (object_id,),
        return_shape=ResultShape.ONE_ONE,
    )
    object_downloaded = object_id in object_manager.get_downloaded_objects(limit_to=[object_id])
    object_external = object_manager.get_external_object_locations([object_id])

    if object_downloaded and not object_in_cache:
        print("Location: created locally")
    else:
        original_location = (
            ("%s (%s)" % (object_external[0][1], object_external[0][2]))
            if object_external
            else "remote engine"
        )
        if object_in_cache:
            print("Location: cached locally")
            print("Original location: " + original_location)
        else:
            print("Location: " + original_location)


@click.command(name="sql")
@click.argument("sql")
@click.option("-s", "--schema", help="Run SQL against this schema.")
@click.option("-a", "--show-all", is_flag=True, help="Returns all results of the query.")
def sql_c(sql, schema, show_all):
    """
    Run an SQL statement against the Splitgraph engine.

    There are no restrictions on the contents of the statement: this is the same as running it
    from any other PostgreSQL client.

    If ``--schema`` is specified, the statement is run with the ``search_path`` set to that schema. This means
    that these statements are equivalent::

        sgr sql "SELECT * FROM \"noaa/climate\".table"
        sgr sql -s noaa/climate "SELECT * FROM table"
    """
    if schema:
        get_engine().run_sql("SET search_path TO %s", (schema,))
    results = get_engine().run_sql(sql)
    if results is None:
        return

    if len(results) > 10 and not show_all:
        pprint(results[:10])
        print("...")
    else:
        pprint(results)


@click.command(name="status")
@click.argument("repository", required=False, type=RepositoryType())
def status_c(repository):
    """
    Show the status of the Splitgraph engine. If a repository is passed, show information about
    the repository. If not, show information about all repositories local to the engine.
    """
    if repository is None:
        repositories = get_current_repositories(get_engine())
        print("Local repositories: ")
        for mp_name, mp_head in repositories:
            # Maybe should also show the remote DB address/server
            print("%s: \t %s" % (mp_name, mp_head.image_hash if mp_head else None))
        print("\nUse sgr status repository to get information about a given repository.")
    else:
        head = repository.head
        if not head:
            print("%s: nothing checked out." % str(repository))
            return
        parent, children = head.get_parent_children()
        print("%s: on image %s." % (str(repository), head.image_hash))
        if parent is not None:
            print("Parent: %s" % parent)
        if len(children) > 1:
            print("Children: " + "\n".join(children))
        elif len(children) == 1:
            print("Child: %s" % children[0])
