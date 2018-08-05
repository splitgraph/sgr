import json
import re
from collections import Counter, defaultdict
from pprint import pprint

import click
import psycopg2
from psycopg2 import ProgrammingError

from splitgraph.commands import mount, unmount, checkout, commit, get_current_head, \
    get_log, get_parent_children, diff, init, pull, push, clone
from splitgraph.constants import POSTGRES_CONNECTION, SplitGraphException
from splitgraph.drawing import render_tree
from splitgraph.meta_handler import get_snap_parent, get_canonical_snap_id, get_all_tables, \
    get_current_mountpoints_hashes, get_all_tags_hashes, set_tag
from splitgraph.sgfile import parse_commands, execute_commands


def _conn():
    return psycopg2.connect(POSTGRES_CONNECTION)


@click.group()
def cli():
    # Toplevel click command group to allow us to invoke e.g. "sg checkout" / "sg commit" etc.
    pass


@click.command(name='status')
@click.argument('mountpoint', required=False)
def status_c(mountpoint):
    conn = _conn()
    if mountpoint is None:
        mountpoints = get_current_mountpoints_hashes(conn)
        print ("Currently mounted databases: ")
        for mp_name, mp_hash in mountpoints:
            # Maybe should also show the remote DB address/server
            print "%s: \t %s" % (mp_name, mp_hash)
        print "\nUse sg status MOUNTPOINT to get information about a given mountpoint."
    else:
        current_snap = get_current_head(conn, mountpoint, raise_on_none=False)
        if not current_snap:
            print "%s: nothing checked out." % mountpoint
            return
        parent, children = get_parent_children(conn, mountpoint, current_snap)
        print "%s: on snapshot %s." % (mountpoint, current_snap)
        if parent is not None:
            print "Parent: %s" % parent
        if len(children) > 1:
            print "Children: " + "\n".join(children)
        elif len(children) == 1:
            print "Child: %s" % children[0]


@click.command(name='log')
@click.argument('mountpoint')
@click.option('-t', '--tree', is_flag=True)
def log_c(mountpoint, tree):
    conn = _conn()
    if tree:
        render_tree(conn, mountpoint)
    else:
        head = get_current_head(conn, mountpoint)
        log = get_log(conn, mountpoint, head)
        for entry in log:
            if entry == head:
                print "%s <--- HEAD" % entry
            else:
                print entry


@click.command(name='diff')
@click.option('-v', '--verbose', default=False, is_flag=True)
@click.option('-t', '--table-name')
@click.argument('mountpoint')
@click.argument('snap_1', required=False)
@click.argument('snap_2', required=False)
def diff_c(verbose, table_name, mountpoint, snap_1, snap_2):
    conn = _conn()

    if snap_1 is None and snap_2 is None:
        # Comparing current working copy against the last commit
        snap_1 = get_current_head(conn, mountpoint)
    elif snap_2 is None:
        snap_1 = get_canonical_snap_id(conn, mountpoint, snap_1)
        # One parameter: diff from that and its parent.
        snap_2 = get_snap_parent(conn, mountpoint, snap_1)
        if snap_2 is None:
            print "%s has no parent to compare to!" % snap_1
        snap_1, snap_2 = snap_2, snap_1 # snap_1 has to come first
    else:
        snap_1 = get_canonical_snap_id(conn, mountpoint, snap_1)
        snap_2 = get_canonical_snap_id(conn, mountpoint, snap_2)

    if table_name:
        diffs = {table_name: diff(conn, mountpoint, table_name, snap_1, snap_2)}
    else:
        all_tables = sorted(get_all_tables(conn, mountpoint))

        diffs = {table_name: diff(conn, mountpoint, table_name, snap_1, snap_2)
                 for table_name in all_tables}

    if snap_2 is None:
        print ("Between %s and the current working copy: " % snap_1[:12])
    else:
        print ("Between %s and %s: " % (snap_1[:12], snap_2[:12]))

    for table_name, diff_result in diffs.iteritems():
        to_print = "%s: " % table_name

        if isinstance(diff_result, list):
            change_count = dict(Counter(d[0] for d in diff_result).most_common())
            added = change_count.get(0, 0)
            removed = change_count.get(1, 0)
            updated = change_count.get(2, 0)

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
                for kind, change in diff_result:
                    print ['+', '-', 'U'][kind] + " " + change
        else:
            # Whole table was either added or removed
            print to_print + ("table added" if diff_result else "table removed")


@click.command(name='mount')
@click.argument('mountpoint')
@click.option('--connection', '-c', help='Connection string in the form username:password@server:port')
@click.option('--handler', '-h', help='Mount handler, one of mongo_fdw or postgres_fdw.')
@click.option('--handler-options', '-o', help='JSON-encoded list of handler options. For postgres_fdw, use '
                                        '{"dbname": <dbname>, "remote_schema": <remote schema>, '
                                        '"tables": <tables to mount (optional)>}. For mongo_fdw, use '
                                        '{"table_name": {"db": <dbname>, "coll": <collection>, "schema": {"col1": "type1"...}}}', default='{}')
def mount_c(mountpoint, connection, handler, handler_options):
    # Parse the connection string in some horrible way
    match = re.match('(\S+):(\S+)@(.+):(\d+)', connection)
    conn = _conn()
    mount(conn, server=match.group(3), port=int(match.group(4)), username=match.group(1), password=match.group(2),
          mountpoint=mountpoint, mount_handler=handler, extra_options=json.loads(handler_options))

    conn.commit()


@click.command(name='unmount')
@click.argument('mountpoint')
def unmount_c(mountpoint):
    conn = _conn()
    unmount(conn, mountpoint)
    conn.commit()


@click.command(name='checkout')
@click.argument('mountpoint')
@click.argument('snapshot')
def checkout_c(mountpoint, snapshot):
    conn = _conn()
    checkout(conn, mountpoint, snapshot)
    conn.commit()


@click.command(name='commit')
@click.argument('mountpoint')
@click.option('-h', '--commit-hash')
@click.option('-s', '--include-snap', default=False, is_flag=True)
def commit_c(mountpoint, commit_hash, include_snap):
    conn = _conn()
    if commit_hash and (len(commit_hash) != 64 or any([x not in 'abcdef0123456789' for x in set(commit_hash)])):
        print "Commit hash must be of the form [a-f0-9] x 64!"
        return

    commit(conn, mountpoint, commit_hash, include_snap=include_snap)
    conn.commit()


@click.command(name='file')
@click.argument('sgfile', type=click.File('r'))
def file_c(sgfile):
    lines = sgfile.readlines()
    conn = _conn()
    commands = parse_commands(lines)
    execute_commands(conn, commands)
    conn.commit()


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
                print "..."
        except ProgrammingError:
            pass  # sql wasn't a SELECT statement
    conn.commit()


@click.command(name='init')
@click.argument('mountpoint')
def init_c(mountpoint):
    conn = _conn()
    init(conn, mountpoint)
    print "Initialized empty mountpoint %s" % mountpoint
    conn.commit()


@click.command(name='pull')
@click.argument('mountpoint')
@click.argument('remote', default='origin')
@click.option('-d', '--download-all', help='Download all objects immediately instead on checkout.')
def pull_c(mountpoint, remote, download_all):
    conn = _conn()
    pull(conn, mountpoint, remote, download_all)
    conn.commit()


@click.command(name='clone')
@click.argument('remote') # username:password@server:port/dbname
@click.argument('remote_mountpoint')
@click.argument('local_mountpoint')
@click.option('-d', '--download-all', help='Download all objects immediately instead on checkout.')
def clone_c(remote, remote_mountpoint, local_mountpoint, download_all):
    conn = _conn()
    clone(conn, remote, remote_mountpoint, local_mountpoint, download_all)
    conn.commit()


@click.command(name='push')
@click.argument('remote') # username:password@server:port/dbname
@click.argument('remote_mountpoint')
@click.argument('local_mountpoint')
def push_c(remote, remote_mountpoint, local_mountpoint):
    conn = _conn()
    push(conn, remote, remote_mountpoint, local_mountpoint)
    conn.commit()


@click.command(name='tag')
@click.argument('mountpoint')
@click.argument('image', required=False)
@click.argument('tag', required=False)
@click.option('-f', '--force', required=False, is_flag=True)
def tag_c(mountpoint, image, tag, force):
    conn = _conn()
    if tag is None:
        # List all tags
        all_tags = get_all_tags_hashes(conn, mountpoint)
        tag_dict = defaultdict(list)
        for img, tag in all_tags:
            tag_dict[img].append(tag)
        if image is None:
            for img, tags in tag_dict.iteritems():
                print "%s: %s" % (img[:12], ', '.join(tags))
        else:
            print ', '.join(tag_dict[image])
        return

    if tag == 'HEAD':
        raise SplitGraphException("HEAD is a reserved tag!")

    image = get_canonical_snap_id(conn, mountpoint, image)
    set_tag(conn, mountpoint, image, tag, force)
    print "Tagged %s:%s with %s." % (mountpoint, image, tag)
    conn.commit()


cli.add_command(status_c)
cli.add_command(log_c)
cli.add_command(mount_c)
cli.add_command(unmount_c)
cli.add_command(checkout_c)
cli.add_command(diff_c)
cli.add_command(commit_c)
cli.add_command(file_c)
cli.add_command(sql_c)
cli.add_command(init_c)
cli.add_command(clone_c)
cli.add_command(pull_c)
cli.add_command(push_c)
cli.add_command(tag_c)