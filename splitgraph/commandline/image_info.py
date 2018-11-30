from collections import Counter
from pprint import pprint

import click
from psycopg2 import ProgrammingError

import splitgraph as sg
from splitgraph.commandline.common import parse_image_spec, pluralise


@click.command(name='log')
@click.argument('repository', type=sg.to_repository)
@click.option('-t', '--tree', is_flag=True)
def log_c(repository, tree):
    """
    Show the full log of the current branch, starting from the current HEAD and following the
    parent chain up. There must be an image checked out into a schema for this to work.

    If -t or --tree is passed, instead render the full image tree. The repository doesn't need to have been
    checked out in this case.
    """
    if tree:
        sg.render_tree(repository)
    else:
        head = sg.get_current_head(repository)
        log = sg.get_log(repository, head)
        for entry in log:
            image_info = sg.get_image(repository, entry)
            print("%s %s %s %s" % ("H->" if entry == head else "   ", entry, image_info.created,
                                   image_info.comment or ""))


@click.command(name='diff')
@click.option('-v', '--verbose', default=False, is_flag=True,
              help='Include the actual differences rather than just the total number of updated rows.')
@click.option('-t', '--table-name', help='Show the differences for a single table.')
@click.argument('repository', type=sg.to_repository)
@click.argument('tag_or_hash_1', required=False)
@click.argument('tag_or_hash_2', required=False)
def diff_c(verbose, table_name, repository, tag_or_hash_1, tag_or_hash_2):
    """
    Calculate the differences between two Splitgraph images in the same repository.

    The actual targets of this command depend on the number of arguments passed:

    `sgr diff REPOSITORY`: returns the differences between the current HEAD image and the checked out schema.

    `sgr diff REPOSITORY TAG_OR_HASH`: returns the differences between the image and its parent.

    `sgr diff REPOSITORY TAG_OR_HASH_1 TAG_OR_HASH_2`: returns the differences from the first image to the second image.
    """
    tag_or_hash_1, tag_or_hash_2 = _get_actual_hashes(repository, tag_or_hash_1, tag_or_hash_2)

    diffs = {table_name: sg.diff(repository, table_name, tag_or_hash_1, tag_or_hash_2, aggregate=not verbose)
             for table_name in
             ([table_name] if table_name else sorted(sg.get_all_tables(sg.get_connection(), repository.to_schema())))}

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
            change_count = dict(Counter(d[1] for d in diff_result).most_common())
            added = change_count.get(0, 0)
            removed = change_count.get(1, 0)
            updated = change_count.get(2, 0)
        else:
            added, removed, updated = diff_result

        count = []
        if added:
            count.append("added " + pluralise('row', added))
        if removed:
            count.append("removed " + pluralise('row', removed))
        if updated:
            count.append("updated " + pluralise('row', removed))
        if added + removed + updated == 0:
            count = ['no changes']
        print(to_print + ', '.join(count) + '.')

        if verbose:
            for pk, kind, change in diff_result:
                print("%r: " % (pk,) + ['+', '-', 'U'][kind] + " %r" % change)
    else:
        # Whole table was either added or removed
        print(to_print + ("table added" if diff_result else "table removed"))


def _get_actual_hashes(repository, image_1, image_2):
    if image_1 is None and image_2 is None:
        # Comparing current working copy against the last commit
        image_1 = sg.get_current_head(repository)
    elif image_2 is None:
        image_1 = sg.resolve_image(repository, image_1)
        # One parameter: diff from that and its parent.
        image_2 = sg.get_image(repository, image_1).parent_id
        if image_2 is None:
            print("%s has no parent to compare to!" % image_1)
        image_1, image_2 = image_2, image_1  # snap_1 has to come first
    else:
        image_1 = sg.resolve_image(repository, image_1)
        image_2 = sg.resolve_image(repository, image_2)
    return image_1, image_2


@click.command(name='show')
@click.argument('image_spec', type=parse_image_spec)
@click.option('-v', '--verbose', default=False, is_flag=True,
              help='Also show all tables in this image and the objects they map to.')
def show_c(image_spec, verbose):
    """
    Show information about a Splitgraph image, including its parent, comment and creation time.

    Image spec must be of the format [NAMESPACE/]REPOSITORY[:HASH_OR_TAG].
    """
    repository, image = image_spec
    image = sg.resolve_image(repository, image)

    print("Commit %s:%s" % (repository.to_schema(), image))
    image_info = sg.get_image(repository, image)
    print(image_info.comment or "")
    print("Created at %s" % image_info.created.isoformat())
    if image_info.parent_id:
        print("Parent: %s" % image_info.parent_id)
    else:
        print("No parent (root commit)")
    if verbose:
        print()
        print("Tables:")
        for t in sg.get_tables_at(repository, image):
            table_objects = sg.get_table(repository, t, image)
            if len(table_objects) == 1:
                print("  %s: %s (%s)" % (t, table_objects[0][0], table_objects[0][1]))
            else:
                print("  %s:" % t)
                for obj in table_objects:
                    print("    %s (%s)" % obj)


@click.command(name='sql')
@click.argument('sql')
@click.option('-a', '--show-all', is_flag=True, help='Returns all results of the query.')
def sql_c(sql, show_all):
    """
    Run an arbitrary SQL statement against the current Splitgraph driver.

    There are no restrictions on the contents of the statement: this is the same as running it
    from any other PostgreSQL client.
    """
    with sg.get_connection().cursor() as cur:
        cur.execute(sql)
        try:
            results = cur.fetchmany(10) if not show_all else cur.fetchall()
            pprint(results)
            if cur.rowcount > 10 and not show_all:
                print("...")
        except ProgrammingError:
            pass  # sql wasn't a SELECT statement


@click.command(name='status')
@click.argument('repository', required=False, type=sg.to_repository)
def status_c(repository):
    """
    Show the status of the Splitgraph driver. If a repository is passed, show information about
    the repository. If not, show information about all repositories local to the driver.
    """
    if repository is None:
        repositories = sg.get_current_repositories()
        print("Local repositories: ")
        for mp_name, mp_hash in repositories:
            # Maybe should also show the remote DB address/server
            print("%s: \t %s" % (mp_name, mp_hash))
        print("\nUse sgr status repository to get information about a given repository.")
    else:
        current_snap = sg.get_current_head(repository, raise_on_none=False)
        if not current_snap:
            print("%s: nothing checked out." % str(repository))
            return
        parent, children = sg.get_parent_children(repository, current_snap)
        print("%s: on image %s." % (str(repository), current_snap))
        if parent is not None:
            print("Parent: %s" % parent)
        if len(children) > 1:
            print("Children: " + "\n".join(children))
        elif len(children) == 1:
            print("Child: %s" % children[0])
