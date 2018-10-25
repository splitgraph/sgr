import json
import re
import sys
from collections import Counter, defaultdict
from pprint import pprint

import click
import psycopg2
from psycopg2 import ProgrammingError
from splitgraph.commands import mount, unmount, commit, checkout, diff, get_log, init, get_parent_children, pull, \
    clone, push, import_tables
from splitgraph.commands.misc import cleanup_objects
from splitgraph.commands.provenance import provenance, image_hash_to_sgfile
from splitgraph.commands.publish import publish
from splitgraph.config.repo_lookups import get_remote_connection_params
from splitgraph.constants import POSTGRES_CONNECTION, SplitGraphException, serialize_connection_string, to_repository
from splitgraph.drawing import render_tree
from splitgraph.meta_handler.images import get_canonical_image_id, get_image
from splitgraph.meta_handler.misc import get_current_repositories, get_remote_for
from splitgraph.meta_handler.tables import get_tables_at, get_table
from splitgraph.pg_utils import get_all_tables
from splitgraph.meta_handler.tags import get_current_head, get_all_hashes_tags, set_tag, tag_or_hash_to_actual_hash
from splitgraph.sgfile.execution import execute_commands, rerun_image_with_replacement


def _conn():
    return psycopg2.connect(POSTGRES_CONNECTION)


@click.group()
def cli():
    # Toplevel click command group to allow us to invoke e.g. "sgr checkout" / "sgr commit" etc.
    pass


@click.command(name='status')
@click.argument('repository', required=False, type=to_repository)
def status_c(repository):
    conn = _conn()
    if repository is None:
        repositories = get_current_repositories(conn)
        print("Currently mounted databases: ")
        for mp_name, mp_hash in repositories:
            # Maybe should also show the remote DB address/server
            print("%s: \t %s" % (mp_name, mp_hash))
        print("\nUse sgr status repository to get information about a given repository.")
    else:
        current_snap = get_current_head(conn, repository, raise_on_none=False)
        if not current_snap:
            print("%s: nothing checked out." % str(repository))
            return
        parent, children = get_parent_children(conn, repository, current_snap)
        print("%s: on snapshot %s." % (str(repository), current_snap))
        if parent is not None:
            print("Parent: %s" % parent)
        if len(children) > 1:
            print("Children: " + "\n".join(children))
        elif len(children) == 1:
            print("Child: %s" % children[0])


@click.command(name='log')
@click.argument('repository', type=to_repository)
@click.option('-t', '--tree', is_flag=True)
def log_c(repository, tree):
    conn = _conn()
    if tree:
        render_tree(conn, repository)
    else:
        head = get_current_head(conn, repository)
        log = get_log(conn, repository, head)
        for entry in log:
            image_info = get_image(conn, repository, entry)
            print("%s %s %s %s" % ("H->" if entry == head else "   ", entry, image_info.created,
                                   image_info.comment or ""))


@click.command(name='diff')
@click.option('-v', '--verbose', default=False, is_flag=True)
@click.option('-t', '--table-name')
@click.argument('repository', type=to_repository)
@click.argument('tag_or_hash_1', required=False)
@click.argument('tag_or_hash_2', required=False)
def diff_c(verbose, table_name, repository, tag_or_hash_1, tag_or_hash_2):
    conn = _conn()
    tag_or_hash_1, tag_or_hash_2 = _get_actual_hashes(conn, repository, tag_or_hash_1, tag_or_hash_2)

    diffs = {table_name: diff(conn, repository, table_name, tag_or_hash_1, tag_or_hash_2, aggregate=not verbose)
             for table_name in ([table_name] if table_name else sorted(get_all_tables(conn, repository.to_schema())))}

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
            count.append("added %d rows" % added)
        if removed:
            count.append("removed %d rows" % removed)
        if updated:
            count.append("updated %d rows" % updated)
        if added + removed + updated == 0:
            count = ['no changes']
        print(to_print + ', '.join(count) + '.')

        if verbose:
            for pk, kind, change in diff_result:
                print("%r: " % (pk,) + ['+', '-', 'U'][kind] + " %r" % change)
    else:
        # Whole table was either added or removed
        print(to_print + ("table added" if diff_result else "table removed"))


def _get_actual_hashes(conn, repository, image_1, image_2):
    if image_1 is None and image_2 is None:
        # Comparing current working copy against the last commit
        image_1 = get_current_head(conn, repository)
    elif image_2 is None:
        image_1 = tag_or_hash_to_actual_hash(conn, repository, image_1)
        # One parameter: diff from that and its parent.
        image_2 = get_image(conn, repository, image_1).parent_id
        if image_2 is None:
            print("%s has no parent to compare to!" % image_1)
        image_1, image_2 = image_2, image_1  # snap_1 has to come first
    else:
        image_1 = tag_or_hash_to_actual_hash(conn, repository, image_1)
        image_2 = tag_or_hash_to_actual_hash(conn, repository, image_2)
    return image_1, image_2


@click.command(name='mount')
@click.argument('schema')
@click.option('--connection', '-c', help='Connection string in the form username:password@server:port')
@click.option('--handler', '-h', help='Mount handler, one of mongo_fdw or postgres_fdw.')
@click.option('--handler-options', '-o', help='JSON-encoded list of handler options. For postgres_fdw, use '
                                              '{"dbname": <dbname>, "remote_schema": <remote schema>, '
                                              '"tables": <tables to mount (optional)>}. For mongo_fdw, use '
                                              '{"table_name": {"db": <dbname>, "coll": <collection>, "schema": '
                                              '{"col1": "type1"...}}}',
              default='{}')
def mount_c(schema, connection, handler, handler_options):
    # Parse the connection string
    match = re.match(r'(\S+):(\S+)@(.+):(\d+)', connection)
    conn = _conn()
    handler_options = json.loads(handler_options)
    handler_options.update(dict(server=match.group(3), port=int(match.group(4)),
                                username=match.group(1), password=match.group(2)))
    mount(conn, schema, mount_handler=handler, handler_kwargs=handler_options)
    conn.commit()


@click.command(name='unmount')
@click.argument('repository', type=to_repository)
def unmount_c(repository):
    conn = _conn()
    unmount(conn, repository)
    conn.commit()


@click.command(name='checkout')
@click.argument('repository', type=to_repository)
@click.argument('snapshot_or_tag')
def checkout_c(repository, snapshot_or_tag):
    conn = _conn()
    snapshot = tag_or_hash_to_actual_hash(conn, repository, snapshot_or_tag)
    checkout(conn, repository, snapshot)
    print("Checked out %s:%s." % (str(repository), snapshot[:12]))
    conn.commit()


@click.command(name='commit')
@click.argument('repository', type=to_repository)
@click.option('-h', '--commit-hash')
@click.option('-s', '--include-snap', default=False, is_flag=True)
@click.option('-m', '--message')
def commit_c(repository, commit_hash, include_snap, message):
    conn = _conn()
    if commit_hash and (len(commit_hash) != 64 or any([x not in 'abcdef0123456789' for x in set(commit_hash)])):
        print("Commit hash must be of the form [a-f0-9] x 64!")
        return

    new_hash = commit(conn, repository, commit_hash, include_snap=include_snap, comment=message)
    print("Committed %s as %s." % (str(repository), new_hash[:12]))
    conn.commit()


@click.command(name='show')
@click.argument('repository', type=to_repository)
@click.argument('commit_tag_or_hash')
@click.option('-v', '--verbose', default=False, is_flag=True)
def show_c(repository, commit_tag_or_hash, verbose):
    conn = _conn()
    commit_tag_or_hash = tag_or_hash_to_actual_hash(conn, repository, commit_tag_or_hash)

    print("Commit %s" % commit_tag_or_hash)
    image_info = get_image(conn, repository, commit_tag_or_hash)
    print(image_info.comment or "")
    print("Created at %s" % image_info.created.isoformat())
    if image_info.parent_id:
        print("Parent: %s" % image_info.parent_id)
    else:
        print("No parent (root commit)")
    if verbose:
        print()
        print("Tables:")
        for t in get_tables_at(conn, repository, commit_tag_or_hash):
            table_objects = get_table(conn, repository, t, commit_tag_or_hash)
            if len(table_objects) == 1:
                print("  %s: %s (%s)" % (t, table_objects[0][0], table_objects[0][1]))
            else:
                print("  %s:" % t)
                for obj in table_objects:
                    print("    %s (%s)" % obj)


@click.command(name='file')
@click.argument('sgfile', type=click.File('r'))
@click.option('-a', '--sgfile-args', multiple=True, type=(str, str))
@click.option('-o', '--output-repository', help='Repository to store the result in.', type=to_repository)
def file_c(sgfile, sgfile_args, output_repository):
    conn = _conn()
    sgfile_args = {k: v for k, v in sgfile_args}
    print("Executing SGFile %s with arguments %r" % (sgfile.name, sgfile_args))
    execute_commands(conn, sgfile.read(), sgfile_args, output=output_repository)
    conn.commit()
    conn.close()


@click.command(name='sql')
@click.argument('sql')
@click.option('-a', '--show-all', is_flag=True)
def sql_c(sql, show_all):
    conn = _conn()
    with conn.cursor() as cur:
        cur.execute(sql)
        try:
            results = cur.fetchmany(10) if not show_all else cur.fetchall()
            pprint(results)
            if cur.rowcount > 10 and not show_all:
                print("...")
        except ProgrammingError:
            pass  # sql wasn't a SELECT statement
    conn.commit()


@click.command(name='init')
@click.argument('repository', type=to_repository)
def init_c(repository):
    conn = _conn()
    init(conn, repository)
    print("Initialized empty repository %s" % str(repository))
    conn.commit()


@click.command(name='pull')
@click.argument('repository', type=to_repository)
@click.argument('remote', default='origin')
@click.option('-d', '--download-all', help='Download all objects immediately instead on checkout.')
def pull_c(repository, remote, download_all):
    conn = _conn()
    pull(conn, repository, remote, download_all)
    conn.commit()


@click.command(name='clone')
@click.argument('remote_repository', type=to_repository)
@click.argument('local_repository', required=False, type=to_repository)
@click.option('-d', '--download-all', help='Download all objects immediately instead on checkout.')
def clone_c(remote_repository, local_repository, download_all):
    conn = _conn()
    clone(conn, remote_repository, local_repository=local_repository, download_all=download_all)
    conn.commit()


@click.command(name='push')
@click.argument('repository', type=to_repository)
@click.argument('remote', default='origin')  # origin (must be specified in the config or remembered by the repo)
@click.argument('remote_repository', required=False, type=to_repository)
@click.option('-h', '--upload-handler', help='Where to upload objects (FILE or DB for the remote itself)', default='DB')
@click.option('-o', '--upload-handler-options', help="""For FILE, e.g. '{"path": /mnt/sgobjects}'""", default="{}")
def push_c(repository, remote, remote_repository, upload_handler, upload_handler_options):
    conn = _conn()
    if not remote_repository:
        # Get actual connection string and remote repository
        remote_info = get_remote_for(conn, repository, remote)
        if not remote_info:
            raise SplitGraphException("No remote found for %s!" % str(repository))
        remote, remote_repository = remote_info
    else:
        remote = serialize_connection_string(*get_remote_connection_params(remote))
    push(conn, repository, remote, remote_repository, handler=upload_handler,
         handler_options=json.loads(upload_handler_options))
    conn.commit()


@click.command(name='tag')
@click.argument('repository', type=to_repository)
@click.option('-i', '--image')
@click.argument('tag', required=False)
@click.option('-f', '--force', required=False, is_flag=True)
def tag_c(repository, image, tag, force):
    conn = _conn()
    if tag is None:
        # List all tags
        tag_dict = defaultdict(list)
        for img, img_tag in get_all_hashes_tags(conn, repository):
            tag_dict[img].append(img_tag)
        if image is None:
            for img, tags in tag_dict.items():
                # Sometimes HEAD is none (if we've just cloned the repo)
                if img:
                    print("%s: %s" % (img[:12], ', '.join(tags)))
        else:
            print(', '.join(tag_dict[get_canonical_image_id(conn, repository, image)]))
        return

    if tag == 'HEAD':
        raise SplitGraphException("HEAD is a reserved tag!")

    if image is None:
        image = get_current_head(conn, repository)
    else:
        image = get_canonical_image_id(conn, repository, image)
    set_tag(conn, repository, image, tag, force)
    print("Tagged %s:%s with %s." % (str(repository), image, tag))
    conn.commit()


@click.command(name='import')
@click.argument('repository', type=to_repository)
@click.argument('table_or_query')
@click.argument('target_repository', type=to_repository)
@click.argument('target_table', required=False)
@click.argument('image', required=False)
@click.option('-q', '--is-query', is_flag=True, default=False)
@click.option('-f', '--foreign-tables', is_flag=True, default=False)
def import_c(repository, table_or_query, target_repository, target_table, image, is_query, foreign_tables):
    if is_query and not target_table:
        print("TARGET_TABLE is required when is_query is True!")
        sys.exit(1)

    conn = _conn()
    if not foreign_tables:
        if not image:
            image = get_current_head(conn, repository)
        else:
            image = get_canonical_image_id(conn, repository, image)
    else:
        image = None
    import_tables(conn, repository, [table_or_query], target_repository, [target_table] if target_table else [],
                  image_hash=image, foreign_tables=foreign_tables, table_queries=[] if not is_query else [True])

    print("%s:%s has been imported from %s:%s%s" % (str(target_repository), target_table, str(repository),
                                                    table_or_query, (' (%s)' % image[:12] if image else '')))

    conn.commit()


@click.command(name='cleanup')
def cleanup_c():
    conn = _conn()
    deleted = cleanup_objects(conn)
    print("Deleted %d physical object(s)" % len(deleted))
    conn.commit()
    conn.close()


@click.command(name='provenance')
@click.argument('repository', type=to_repository)
@click.argument('snapshot_or_tag')
@click.option('-f', '--full', required=False, is_flag=True, help='Recreate the sgfile used to create this image')
@click.option('-e', '--error-on-end', required=False, default=True, is_flag=True,
              help='If False, bases the recreated sgfile on the last image where the provenance chain breaks')
def provenance_c(repository, snapshot_or_tag, full, error_on_end):
    conn = _conn()
    snapshot = tag_or_hash_to_actual_hash(conn, repository, snapshot_or_tag)

    if full:
        sgfile_commands = image_hash_to_sgfile(conn, repository, snapshot, error_on_end)
        print("# sgfile commands used to recreate %s:%s" % (str(repository), snapshot))
        print('\n'.join(sgfile_commands))
    else:
        result = provenance(conn, repository, snapshot)
        print("%s:%s depends on:" % (str(repository), snapshot))
        print('\n'.join("%s:%s" % rs for rs in result))
    conn.close()


@click.command(name='rerun')
@click.argument('repository', type=to_repository)
@click.argument('snapshot_or_tag')
@click.option('-u', '--update', is_flag=True, help='Rederive the image against the latest version of all dependencies.')
@click.option('-i', '--repo-image', multiple=True, type=(to_repository, str))
def rerun_c(repository, snapshot_or_tag, update, repo_image):
    conn = _conn()
    snapshot = tag_or_hash_to_actual_hash(conn, repository, snapshot_or_tag)

    # Replace the sources used to construct the image with either the latest ones or the images specified by the user.
    # This doesn't require us at this point to have pulled all the dependencies: the sgfile executor will do it
    # after we feed in the reconstructed and patched sgfile.
    deps = {k: v for k, v in provenance(conn, repository, snapshot)}
    new_images = {repo: image for repo, image in repo_image} if not update \
        else {repo: 'latest' for repo, _ in deps.items()}
    deps.update(new_images)

    print("Rerunning %s:%s against:" % (str(repository), snapshot))
    print('\n'.join("%s:%s" % rs for rs in new_images.items()))

    rerun_image_with_replacement(conn, repository, snapshot, new_images)

    conn.commit()
    conn.close()


@click.command(name='publish')
@click.argument('repository', type=to_repository)
@click.argument('tag')
@click.option('-r', '--readme', type=click.File('r'))
@click.option('--skip-provenance', is_flag=True, help='Don''t include provenance in the published information.')
@click.option('--skip-previews', is_flag=True, help='Don''t include table previews in the published information.')
def publish_c(repository, tag, readme, skip_provenance, skip_previews):
    conn = _conn()
    if readme:
        readme = readme.read()
    else:
        readme = ""
    publish(conn, repository, tag, readme, include_provenance=not skip_provenance,
            include_table_previews=not skip_previews)
    conn.commit()
    conn.close()


cli.add_command(status_c)
cli.add_command(log_c)
cli.add_command(mount_c)
cli.add_command(unmount_c)
cli.add_command(checkout_c)
cli.add_command(diff_c)
cli.add_command(commit_c)
cli.add_command(show_c)
cli.add_command(file_c)
cli.add_command(sql_c)
cli.add_command(init_c)
cli.add_command(clone_c)
cli.add_command(pull_c)
cli.add_command(push_c)
cli.add_command(tag_c)
cli.add_command(import_c)
cli.add_command(cleanup_c)
cli.add_command(provenance_c)
cli.add_command(rerun_c)
cli.add_command(publish_c)
