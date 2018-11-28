import json
import re
import sys
from collections import Counter, defaultdict
from pprint import pprint

import click
from psycopg2 import ProgrammingError

import splitgraph as sg


def _commit_connection(result):
    """Commit and close the PG connection when the application finishes."""
    conn = sg.get_connection()
    if conn:
        conn.commit()
        conn.close()


@click.group(result_callback=_commit_connection)
def cli():
    # Toplevel click command group to allow us to invoke e.g. "sgr checkout" / "sgr commit" etc.
    pass


@click.command(name='status')
@click.argument('repository', required=False, type=sg.to_repository)
def status_c(repository):
    if repository is None:
        repositories = sg.get_current_repositories()
        print("Currently mounted databases: ")
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
        print("%s: on snapshot %s." % (str(repository), current_snap))
        if parent is not None:
            print("Parent: %s" % parent)
        if len(children) > 1:
            print("Children: " + "\n".join(children))
        elif len(children) == 1:
            print("Child: %s" % children[0])


@click.command(name='log')
@click.argument('repository', type=sg.to_repository)
@click.option('-t', '--tree', is_flag=True)
def log_c(repository, tree):
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
@click.option('-v', '--verbose', default=False, is_flag=True)
@click.option('-t', '--table-name')
@click.argument('repository', type=sg.to_repository)
@click.argument('tag_or_hash_1', required=False)
@click.argument('tag_or_hash_2', required=False)
def diff_c(verbose, table_name, repository, tag_or_hash_1, tag_or_hash_2):
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


def _get_actual_hashes(repository, image_1, image_2):
    if image_1 is None and image_2 is None:
        # Comparing current working copy against the last commit
        image_1 = sg.get_current_head(repository)
    elif image_2 is None:
        image_1 = sg.tag_or_hash_to_actual_hash(repository, image_1)
        # One parameter: diff from that and its parent.
        image_2 = sg.get_image(repository, image_1).parent_id
        if image_2 is None:
            print("%s has no parent to compare to!" % image_1)
        image_1, image_2 = image_2, image_1  # snap_1 has to come first
    else:
        image_1 = sg.tag_or_hash_to_actual_hash(repository, image_1)
        image_2 = sg.tag_or_hash_to_actual_hash(repository, image_2)
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
    handler_options = json.loads(handler_options)
    handler_options.update(dict(server=match.group(3), port=int(match.group(4)),
                                username=match.group(1), password=match.group(2)))
    sg.mount(schema, mount_handler=handler, handler_kwargs=handler_options)


@click.command(name='unmount')
@click.argument('repository', type=sg.to_repository)
def unmount_c(repository):
    sg.unmount(repository)


@click.command(name='checkout')
@click.argument('repository', type=sg.to_repository)
@click.argument('snapshot_or_tag')
def checkout_c(repository, snapshot_or_tag):
    snapshot = sg.tag_or_hash_to_actual_hash(repository, snapshot_or_tag)
    sg.checkout(repository, snapshot)
    print("Checked out %s:%s." % (str(repository), snapshot[:12]))


@click.command(name='commit')
@click.argument('repository', type=sg.to_repository)
@click.option('-h', '--commit-hash')
@click.option('-s', '--include-snap', default=False, is_flag=True)
@click.option('-m', '--message')
def commit_c(repository, commit_hash, include_snap, message):
    if commit_hash and (len(commit_hash) != 64 or any([x not in 'abcdef0123456789' for x in set(commit_hash)])):
        print("Commit hash must be of the form [a-f0-9] x 64!")
        return

    new_hash = sg.commit(repository, commit_hash, include_snap=include_snap, comment=message)
    print("Committed %s as %s." % (str(repository), new_hash[:12]))


@click.command(name='show')
@click.argument('repository', type=sg.to_repository)
@click.argument('commit_tag_or_hash')
@click.option('-v', '--verbose', default=False, is_flag=True)
def show_c(repository, commit_tag_or_hash, verbose):
    commit_tag_or_hash = sg.tag_or_hash_to_actual_hash(repository, commit_tag_or_hash)

    print("Commit %s" % commit_tag_or_hash)
    image_info = sg.get_image(repository, commit_tag_or_hash)
    print(image_info.comment or "")
    print("Created at %s" % image_info.created.isoformat())
    if image_info.parent_id:
        print("Parent: %s" % image_info.parent_id)
    else:
        print("No parent (root commit)")
    if verbose:
        print()
        print("Tables:")
        for t in sg.get_tables_at(repository, commit_tag_or_hash):
            table_objects = sg.get_table(repository, t, commit_tag_or_hash)
            if len(table_objects) == 1:
                print("  %s: %s (%s)" % (t, table_objects[0][0], table_objects[0][1]))
            else:
                print("  %s:" % t)
                for obj in table_objects:
                    print("    %s (%s)" % obj)


@click.command(name='file')
@click.argument('sgfile', type=click.File('r'))
@click.option('-a', '--sgfile-args', multiple=True, type=(str, str))
@click.option('-o', '--output-repository', help='Repository to store the result in.', type=sg.to_repository)
def file_c(sgfile, sgfile_args, output_repository):
    sgfile_args = {k: v for k, v in sgfile_args}
    print("Executing SGFile %s with arguments %r" % (sgfile.name, sgfile_args))
    sg.execute_commands(sgfile.read(), sgfile_args, output=output_repository)


@click.command(name='sql')
@click.argument('sql')
@click.option('-a', '--show-all', is_flag=True)
def sql_c(sql, show_all):
    with sg.get_connection().cursor() as cur:
        cur.execute(sql)
        try:
            results = cur.fetchmany(10) if not show_all else cur.fetchall()
            pprint(results)
            if cur.rowcount > 10 and not show_all:
                print("...")
        except ProgrammingError:
            pass  # sql wasn't a SELECT statement


@click.command(name='init')
@click.argument('repository', type=sg.to_repository)
def init_c(repository):
    sg.init(repository)
    print("Initialized empty repository %s" % str(repository))


@click.command(name='pull')
@click.argument('repository', type=sg.to_repository)
@click.argument('remote', default='origin')
@click.option('-d', '--download-all', help='Download all objects immediately instead on checkout.')
def pull_c(repository, remote, download_all):
    sg.pull(repository, remote, download_all)


@click.command(name='clone')
@click.argument('remote_repository', type=sg.to_repository)
@click.argument('local_repository', required=False, type=sg.to_repository)
@click.option('-d', '--download-all', help='Download all objects immediately instead on checkout.',
              default=False, is_flag=True)
def clone_c(remote_repository, local_repository, download_all):
    sg.clone(remote_repository, local_repository=local_repository, download_all=download_all)


@click.command(name='push')
@click.argument('repository', type=sg.to_repository)
@click.argument('remote', default='origin')  # origin (must be specified in the config or remembered by the repo)
@click.argument('remote_repository', required=False, type=sg.to_repository)
@click.option('-h', '--upload-handler', help='Where to upload objects (FILE or DB for the remote itself)', default='DB')
@click.option('-o', '--upload-handler-options', help="""For FILE, e.g. '{"path": /mnt/sgobjects}'""", default="{}")
def push_c(repository, remote, remote_repository, upload_handler, upload_handler_options):
    if not remote_repository:
        # Get actual connection string and remote repository
        remote_info = sg.get_remote_for(repository, remote)
        if not remote_info:
            raise sg.SplitGraphException("No remote found for %s!" % str(repository))
        remote, remote_repository = remote_info
    else:
        remote = sg.serialize_connection_string(*sg.get_remote_connection_params(remote))
    sg.push(repository, remote, remote_repository, handler=upload_handler,
            handler_options=json.loads(upload_handler_options))


@click.command(name='tag')
@click.argument('repository', type=sg.to_repository)
@click.option('-i', '--image')
@click.argument('tag', required=False)
@click.option('-f', '--force', required=False, is_flag=True)
def tag_c(repository, image, tag, force):
    if tag is None:
        # List all tags
        tag_dict = defaultdict(list)
        for img, img_tag in sg.get_all_hashes_tags(repository):
            tag_dict[img].append(img_tag)
        if image is None:
            for img, tags in tag_dict.items():
                # Sometimes HEAD is none (if we've just cloned the repo)
                if img:
                    print("%s: %s" % (img[:12], ', '.join(tags)))
        else:
            print(', '.join(tag_dict[sg.get_canonical_image_id(repository, image)]))
        return

    if tag == 'HEAD':
        raise sg.SplitGraphException("HEAD is a reserved tag!")

    if image is None:
        image = sg.get_current_head(repository)
    else:
        image = sg.get_canonical_image_id(repository, image)
    sg.set_tag(repository, image, tag, force)
    print("Tagged %s:%s with %s." % (str(repository), image, tag))


@click.command(name='import')
@click.argument('repository', type=sg.to_repository)
@click.argument('table_or_query')
@click.argument('target_repository', type=sg.to_repository)
@click.argument('target_table', required=False)
@click.argument('image', required=False)
@click.option('-q', '--is-query', is_flag=True, default=False)
@click.option('-f', '--foreign-tables', is_flag=True, default=False)
def import_c(repository, table_or_query, target_repository, target_table, image, is_query, foreign_tables):
    if is_query and not target_table:
        print("TARGET_TABLE is required when is_query is True!")
        sys.exit(1)

    if not foreign_tables:
        if not image:
            image = sg.get_current_head(repository)
        else:
            image = sg.get_canonical_image_id(repository, image)
    else:
        image = None
    sg.import_tables(repository, [table_or_query], target_repository, [target_table] if target_table else [],
                     image_hash=image, foreign_tables=foreign_tables, table_queries=[] if not is_query else [True])

    print("%s:%s has been imported from %s:%s%s" % (str(target_repository), target_table, str(repository),
                                                    table_or_query, (' (%s)' % image[:12] if image else '')))


@click.command(name='cleanup')
def cleanup_c():
    deleted = sg.cleanup_objects()
    print("Deleted %d physical object(s)" % len(deleted))


@click.command(name='provenance')
@click.argument('repository', type=sg.to_repository)
@click.argument('snapshot_or_tag')
@click.option('-f', '--full', required=False, is_flag=True, help='Recreate the sgfile used to create this image')
@click.option('-e', '--error-on-end', required=False, default=True, is_flag=True,
              help='If False, bases the recreated sgfile on the last image where the provenance chain breaks')
def provenance_c(repository, snapshot_or_tag, full, error_on_end):
    snapshot = sg.tag_or_hash_to_actual_hash(repository, snapshot_or_tag)

    if full:
        sgfile_commands = sg.image_hash_to_sgfile(repository, snapshot, error_on_end)
        print("# sgfile commands used to recreate %s:%s" % (str(repository), snapshot))
        print('\n'.join(sgfile_commands))
    else:
        result = sg.provenance(repository, snapshot)
        print("%s:%s depends on:" % (str(repository), snapshot))
        print('\n'.join("%s:%s" % rs for rs in result))


@click.command(name='rerun')
@click.argument('repository', type=sg.to_repository)
@click.argument('snapshot_or_tag')
@click.option('-u', '--update', is_flag=True, help='Rederive the image against the latest version of all dependencies.')
@click.option('-i', '--repo-image', multiple=True, type=(sg.to_repository, str))
def rerun_c(repository, snapshot_or_tag, update, repo_image):
    snapshot = sg.tag_or_hash_to_actual_hash(repository, snapshot_or_tag)

    # Replace the sources used to construct the image with either the latest ones or the images specified by the user.
    # This doesn't require us at this point to have pulled all the dependencies: the sgfile executor will do it
    # after we feed in the reconstructed and patched sgfile.
    deps = {k: v for k, v in sg.provenance(repository, snapshot)}
    new_images = {repo: image for repo, image in repo_image} if not update \
        else {repo: 'latest' for repo, _ in deps.items()}
    deps.update(new_images)

    print("Rerunning %s:%s against:" % (str(repository), snapshot))
    print('\n'.join("%s:%s" % rs for rs in new_images.items()))

    sg.rerun_image_with_replacement(repository, snapshot, new_images)


@click.command(name='publish')
@click.argument('repository', type=sg.to_repository)
@click.argument('tag')
@click.option('-r', '--readme', type=click.File('r'))
@click.option('--skip-provenance', is_flag=True, help='Don''t include provenance in the published information.')
@click.option('--skip-previews', is_flag=True, help='Don''t include table previews in the published information.')
def publish_c(repository, tag, readme, skip_provenance, skip_previews):
    if readme:
        readme = readme.read()
    else:
        readme = ""
    sg.publish(repository, tag, readme=readme, include_provenance=not skip_provenance,
               include_table_previews=not skip_previews)


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
