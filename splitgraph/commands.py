import re
from contextlib import contextmanager
from random import getrandbits

import psycopg2

from splitgraph.constants import SPLITGRAPH_META_SCHEMA, SplitGraphException, _log, get_random_object_id
from splitgraph.meta_handler import get_tables_at, get_all_foreign_tables, get_current_head, add_new_snap_id, \
    set_head, register_mountpoint, unregister_mountpoint, get_snap_parent, get_canonical_snap_id, \
    get_table, get_all_tables, register_table_object, get_all_snap_parents, register_objects, \
    get_existing_objects, ensure_metadata_schema, get_table_with_format, deregister_table_object, get_remote_for, \
    add_remote, get_downloaded_objects
# Commands to import a foreign schema locally, with version control.
# Tables that are created in the local schema:
# <table>_origin      -- an FDW table that is directly connected to the origin table
# <table>             -- the current staging table/HEAD -- gets overwritten every time we check out a commit.
# <object_id>         -- "git objects" that are either table snapshots or diffs. Diffs use the JSON format and
#                     -- are populated by dumping the WAL.
from splitgraph.pg_replication import apply_record_to_staging, stop_replication, start_replication, \
    record_pending_changes, commit_pending_changes, discard_pending_changes, dump_pending_changes


def pg_table_exists(conn, mountpoint, table_name):
    # WTF: postgres quietly truncates all table names to 63 characters
    # at creation and in select statements
    with conn.cursor() as cur:
        cur.execute("""SELECT table_name from information_schema.tables
                       WHERE table_schema = %s AND table_name = %s""", (mountpoint, table_name[:63]))
        return cur.fetchone() is not None


def _get_column_names(conn, mountpoint, table_name):
    with conn.cursor() as cur:
        cur.execute("""SELECT column_name FROM information_schema.columns
                       WHERE table_schema = %s
                       AND table_name = %s""", (mountpoint, table_name))
        return [c[0] for c in cur.fetchall()]


def materialize_table(conn, mountpoint, schema_snap, table, destination):
    with conn.cursor() as cur:
        fq_dest = cur.mogrify('%s.%s' % (mountpoint, destination))

        cur.execute("""DROP TABLE IF EXISTS %s""" % fq_dest)
        # Get the closest snapshot from the table's parents
        # and then apply all deltas consecutively from it.
        to_apply = []
        parent_snap = schema_snap
        snap_found = False
        while parent_snap is not None:

            # Do we have a snapshot of this image?
            object_id = get_table_with_format(conn, mountpoint, table, parent_snap, 'SNAP')
            if object_id is not None:
                snap_found = True
                break
            else:
                # Otherwise, have to reconstruct it manually.
                to_apply.append(get_table_with_format(conn, mountpoint, table, parent_snap, 'DIFF'))
            parent_snap = get_snap_parent(conn, mountpoint, parent_snap)
        if not snap_found:
            # We didn't find an actual snapshot for this table -- either it doesn't exist in this
            # version or something went wrong. Skip the table.
            return

        # Make sure all the objects have been downloaded from remote if it exists
        remote_info = get_remote_for(conn, mountpoint)
        if remote_info:
            remote_conn, remote_mountpoint = remote_info
            download_objects(conn, mountpoint, remote_conn, remote_mountpoint,
                             objects_to_fetch=to_apply + [object_id])

        # Copy the given snap id over to "staging"
        cur.execute("""CREATE TABLE %s AS SELECT * FROM %s""" %
                    (fq_dest, cur.mogrify('%s.%s' % (mountpoint, object_id))))
        # This is to work around logical replication not reflecting deletions from non-PKd tables. However, this makes
        # it emit all column values in the row, not just the updated ones.
        # FIXME: fiddling with it based on us inspecting the table structure.
        cur.execute("""ALTER TABLE %s REPLICA IDENTITY FULL""" % fq_dest)

        # Apply the deltas sequentially to the checked out table
        for pack_object in reversed(to_apply):
            _log("Applying %s..." % pack_object)
            apply_record_to_staging(conn, mountpoint, pack_object, destination)


def checkout(conn, mountpoint, schema_snap, tables=[]):
    ensure_metadata_schema(conn)
    record_pending_changes(conn)

    stop_replication(conn)
    discard_pending_changes(conn, mountpoint)

    # Detect the actual schema snap we want to check out
    schema_snap = get_canonical_snap_id(conn, mountpoint, schema_snap)
    tables = tables or get_tables_at(conn, mountpoint, schema_snap)
    with conn.cursor() as cur:
        # Drop all current tables in staging
        for table in get_all_tables(conn, mountpoint):
            cur.execute("""DROP TABLE IF EXISTS %s""" % cur.mogrify('%s.%s' % (mountpoint, table)))

    for table in tables:
        materialize_table(conn, mountpoint, schema_snap, table, table)

    # Repoint the current HEAD for this mountpoint to the new snap ID
    set_head(conn, mountpoint, schema_snap)

    # Start recording changes to the mountpoint.
    conn.commit()
    start_replication(conn)

    _log("Checked out %s:%s." % (mountpoint, schema_snap[:12]))


def commit(conn, mountpoint, schema_snap=None, include_snap=False):
    ensure_metadata_schema(conn)
    # required here so that the logical replication sees changes made before the commit in this tx
    conn.commit()
    record_pending_changes(conn)
    _log("Committing...")

    HEAD = get_current_head(conn, mountpoint)

    if schema_snap is None:
        schema_snap = "%0.2x" % getrandbits(256)

    # Add the new snap ID to the tree
    add_new_snap_id(conn, mountpoint, HEAD, schema_snap)

    commit_pending_changes(conn, mountpoint, HEAD, schema_snap, include_snap=include_snap)

    stop_replication(conn)
    set_head(conn, mountpoint, schema_snap)
    conn.commit()  # need to commit before starting replication
    start_replication(conn)
    _log("Committed as %s" % schema_snap[:12])
    return schema_snap


def mount_postgres(conn, server, port, username, password, mountpoint, extra_options):
    with conn.cursor() as cur:
        dbname = extra_options['dbname']

        _log("postgres_fdw: importing foreign schema...")

        cur.execute("""CREATE SERVER %s
                        FOREIGN DATA WRAPPER postgres_fdw
                        OPTIONS (host %%s, port %%s, dbname %%s)""" %
                    cur.mogrify(mountpoint + '_server'), (server, str(port), dbname))
        cur.execute("""CREATE USER MAPPING FOR clientuser
                        SERVER %s
                        OPTIONS (user %%s, password %%s)""" % cur.mogrify(mountpoint + '_server'), (username, password))

        tables = extra_options.get('tables', [])
        remote_schema = extra_options['remote_schema']

        cur.execute("""CREATE SCHEMA %s""" % cur.mogrify(mountpoint))

        # Construct a query: import schema limit to (%s, %s, ...) from server mountpoint_server into mountpoint
        query = """IMPORT FOREIGN SCHEMA %s """ % cur.mogrify(remote_schema)
        if tables:
            query += "LIMIT TO (" + ",".join("%s" for _ in tables) + ") "
        query += "FROM SERVER %s INTO %s" % (cur.mogrify(mountpoint + '_server'), cur.mogrify(mountpoint))
        cur.execute(query, tables)


def mount_mongo(conn, server, port, username, password, mountpoint, extra_options):
    with conn.cursor() as cur:
        _log("mongo_fdw: mounting foreign tables...")

        fq_server = cur.mogrify(mountpoint + '_server')

        cur.execute("""CREATE SERVER %s
                        FOREIGN DATA WRAPPER mongo_fdw
                        OPTIONS (address %%s, port %%s)""" % fq_server, (server, str(port)))
        cur.execute("""CREATE USER MAPPING FOR clientuser
                        SERVER %s
                        OPTIONS (username %%s, password %%s)""" % fq_server, (username, password))

        cur.execute("""CREATE SCHEMA %s""" % cur.mogrify(mountpoint))

        # Mongo extra options: a map of
        # {table_name: {db: remote_db_name, coll: remote_collection_name, schema: {col1: type1, col2: type2...}}}
        for table_name, table_options in extra_options.iteritems():
            _log("Mounting table %s" % table_name)
            db = table_options['db']
            coll = table_options['coll']

            query = "CREATE FOREIGN TABLE %s (_id NAME " % cur.mogrify('%s.%s' % (mountpoint, table_name))
            if table_options['schema']:
                query += ',' + ','.join(
                    "%s %s" % (cur.mogrify(cname), cur.mogrify(ctype)) for cname, ctype in
                    table_options['schema'].iteritems())
            query += ") SERVER %s OPTIONS (database %%s, collection %%s)" % fq_server

            cur.execute(query, (db, coll))


def mount(conn, server, port, username, password, mountpoint, mount_handler, extra_options):
    ensure_metadata_schema(conn)
    if mount_handler == 'postgres_fdw':
        mh_func = mount_postgres
    elif mount_handler == 'mongo_fdw':
        mh_func = mount_mongo
    else:
        raise SplitGraphException("Mount handler %s not supported!" % mount_handler)

    _log("Connecting to remote server...")
    mh_func(conn, server, port, username, password, mountpoint, extra_options)

    with conn.cursor() as cur:
        # For now we are just assigning a random ID to this schema snap
        schema_snap = "%0.2x" % getrandbits(256)

        _log("Pulling image %s from remote schema..." % schema_snap[:12])

        # Update tables to list all tables we've actually pulled.
        tables = get_all_foreign_tables(conn, mountpoint)

        # Rename all foreign tables into tablename_origin and pretend to "pull" the current remote schema HEAD.
        table_object_ids = []
        for table in tables:
            # Create a dummy object ID for each table.
            object_id = get_random_object_id()
            table_object_ids.append(object_id)
            cur.execute("""ALTER TABLE %s RENAME TO %s""" % (
                cur.mogrify('%s.%s' % (mountpoint, table)),
                cur.mogrify('%s_origin' % table)))
            cur.execute("""CREATE TABLE %s AS SELECT * FROM %s""" % (
                cur.mogrify('%s.%s' % (mountpoint, object_id)),
                cur.mogrify('%s.%s_origin' % (mountpoint, table))))

    # Finally, register the mountpoint in our metadata store.
    register_mountpoint(conn, mountpoint, schema_snap, tables, table_object_ids)

    # Also check out the HEAD of the schema
    checkout(conn, mountpoint, schema_snap)

    conn.commit()
    return schema_snap


def unmount(conn, mountpoint):
    ensure_metadata_schema(conn)
    stop_replication(conn)
    with conn.cursor() as cur:
        cur.execute("""DROP SCHEMA IF EXISTS %s CASCADE""" % cur.mogrify(mountpoint))
        # Drop server too if it exists (could have been a non-foreign mountpoint)
        cur.execute("""DROP SERVER IF EXISTS %s CASCADE""" % cur.mogrify(mountpoint + '_server', ))

    # Currently we just discard all history info about the mounted schema
    unregister_mountpoint(conn, mountpoint)
    conn.commit()


def get_parent_children(conn, mountpoint, snap_id):
    parent = get_snap_parent(conn, mountpoint, snap_id)

    with conn.cursor() as cur:
        cur.execute("""SELECT snap_id FROM %s.snap_tree WHERE mountpoint = %%s AND parent_id = %%s"""
                    % SPLITGRAPH_META_SCHEMA, (mountpoint, snap_id))
        children = [c[0] for c in cur.fetchall()]
    return parent, children


def get_log(conn, mountpoint, start_snap):
    # Repeatedly gets the parent of a given snapshot until it reaches the bottom.
    result = []
    while start_snap is not None:
        result.append(start_snap)
        start_snap = get_snap_parent(conn, mountpoint, start_snap)
    return result


@contextmanager
def materialized_table(conn, mountpoint, table_name, snap):
    # hacks hacks hacks
    if snap is not None:
        with conn.cursor() as cur:
            # See if the table snapshot already exists, otherwise reconstruct it
            object_id = get_table_with_format(conn, mountpoint, table_name, snap, 'SNAP')
            if object_id is None:
                # Materialize the SNAP into a new object
                new_id = get_random_object_id()
                materialize_table(conn, mountpoint, snap, table_name, new_id)
                register_table_object(conn, mountpoint, table_name, snap, new_id, 'SNAP')
                yield new_id
                # Maybe some cache management/expiry strategies here
                cur.execute("""DROP TABLE IF EXISTS %s""" % cur.mogrify('%s.%s' % (mountpoint, object_id)))
                deregister_table_object(conn, mountpoint, object_id)
            else:
                yield object_id
    else:
        # No snapshot -- just return the current staging table.
        yield table_name


def _table_exists_at(conn, mountpoint, table_name, image):
    return pg_table_exists(conn, mountpoint, table_name) if image is None \
        else bool(get_table(conn, mountpoint, table_name, image))


def _find_path(conn, mountpoint, snap_1, snap_2):
    path = []
    while snap_2 is not None:
        path.append(snap_2)
        snap_2 = get_snap_parent(conn, mountpoint, snap_2)
        if snap_2 == snap_1:
            return path


def diff(conn, mountpoint, table_name, snap_1, snap_2):
    # Returns a list of changes done to a table if it exists in both images.
    # Otherwise, returns True if the table was added and False if it was removed.
    with conn.cursor() as cur:
        # If the table doesn't exist in the first or the second image, short-circuit and
        # return the bool.
        if not _table_exists_at(conn, mountpoint, table_name, snap_1):
            return True
        if not _table_exists_at(conn, mountpoint, table_name, snap_2):
            return False

        # Special case: if diffing HEAD and staging, then just return the current pending changes.
        HEAD = get_current_head(conn, mountpoint)
        # TODO do we need to conflate them e.g. if A changed to B and then B changed to C, do we emit A -> C?
        if snap_1 == HEAD and snap_2 is None:
            return dump_pending_changes(conn, mountpoint, table_name)

        # If the table is the same in the two images, short circuit as well.
        if set(get_table(conn, mountpoint, table_name, snap_1)) == set(get_table(conn, mountpoint, table_name, snap_2)):
            return []

        # Otherwise, check if snap_1 is a parent of snap_2, then we can merge all the diffs.
        path = _find_path(conn, mountpoint, snap_1, (snap_2 if snap_2 is not None else HEAD))
        if path is not None:
            result = []
            for image in reversed(path):
                diff_id = get_table_with_format(conn, mountpoint, table_name, image, 'DIFF')
                cur.execute("""SELECT kind, change FROM %s""" % cur.mogrify('%s.%s' % (mountpoint, diff_id)))
                result.extend(cur.fetchall())

            # If snap_2 is staging, also include all changes that have happened since the last commit.
            if snap_2 is None:
                result.extend(dump_pending_changes(conn, mountpoint, table_name))
            return result

        else:
            # Finally, resort to manual diffing (images on different branches or reverse comparison order).
            with materialized_table(conn, mountpoint, table_name, snap_1) as table_1:
                with materialized_table(conn, mountpoint, table_name, snap_2) as table_2:
                    # Check both tables out at the same time since then table_2 calculation can be based
                    # on table_1's snapshot.
                    cur.execute("""SELECT * FROM %s""" % cur.mogrify('%s.%s' % (mountpoint, table_1)))
                    left = cur.fetchall()
                    cur.execute("""SELECT * FROM %s""" % cur.mogrify('%s.%s' % (mountpoint, table_2)))
                    right = cur.fetchall()

            # Mimic the diff format returned by the WAL
            return [(1, r) for r in left if r not in right] + [(0, r) for r in right if r not in left]


def init(conn, mountpoint):
    ensure_metadata_schema(conn)
    # Initializes an empty repo with an initial commit (hash 0000...)
    with conn.cursor() as cur:
        cur.execute("""CREATE SCHEMA %s""" % cur.mogrify(mountpoint))
    snap_id = '0' * 64
    register_mountpoint(conn, mountpoint, snap_id, tables=[], table_object_ids=[])


def _make_conn(server, port, username, password, dbname):
    return psycopg2.connect(host=server, port=port, user=username, password=password, dbname=dbname)


def _get_required_snaps_objects(conn, remote_conn, local_mountpoint, remote_mountpoint):
    local_snap_parents = {snap_id: parent_id for snap_id, parent_id in get_all_snap_parents(conn, local_mountpoint)}
    remote_snap_parents = {snap_id: parent_id for snap_id, parent_id in
                           get_all_snap_parents(remote_conn, remote_mountpoint)}

    # We assume here that none of the remote snapshot IDs have changed (are immutable) since otherwise the remote
    # would have created a new snapshot.
    snaps_to_fetch = [s for s in remote_snap_parents if s not in local_snap_parents]
    object_meta = []
    for snap_id in snaps_to_fetch:
        # This is not batched but there shouldn't be that many entries here anyway.
        add_new_snap_id(conn, local_mountpoint, remote_snap_parents[snap_id], snap_id)
        # Get the meta for all objects we'll need to fetch.
        with remote_conn.cursor() as cur:
            cur.execute("""SELECT snap_id, table_name, object_id, format from %s.tables 
                           WHERE mountpoint = %%s AND snap_id = %%s"""
                        % SPLITGRAPH_META_SCHEMA, (remote_mountpoint, snap_id))
            object_meta.extend(cur.fetchall())
    return snaps_to_fetch, object_meta


def pull(conn, mountpoint, remote, download_all=False):
    remote_info = get_remote_for(conn, mountpoint, remote)
    if not remote_info:
        raise SplitGraphException("No remote %s found for mountpoint %s!" % (remote, mountpoint))

    remote_conn_string, remote_mountpoint = remote_info
    clone(conn, remote_conn_string, remote_mountpoint, mountpoint, download_all)


def clone(conn, remote_conn_string, remote_mountpoint, local_mountpoint, download_all=False):
    ensure_metadata_schema(conn)
    # Pulls a schema from the remote, including all of its history.

    with conn.cursor() as cur:
        cur.execute("""CREATE SCHEMA IF NOT EXISTS %s""" % cur.mogrify(local_mountpoint))

    _log("Connecting to the remote driver...")
    match = re.match('(\S+):(\S+)@(.+):(\d+)/(\S+)', remote_conn_string)
    remote_conn = _make_conn(server=match.group(3), port=int(match.group(4)), username=match.group(1),
                             password=match.group(2), dbname=match.group(5))

    # Get the remote log and the list of objects we need to fetch.
    _log("Gathering remote metadata...")

    # This also registers the new versions locally.
    snaps_to_fetch, object_meta = _get_required_snaps_objects(conn, remote_conn, local_mountpoint, remote_mountpoint)

    if not snaps_to_fetch:
        _log("Nothing to do.")
        return

    # Don't actually download any real objects until the user tries to check out a revision.
    if download_all:
        # Check which new objects we need to fetch/preregister.
        # We might already have some objects prefetched
        # (e.g. if a new version of the table is the same as the old version)
        _log("Fetching remote objects...")
        download_objects(conn, local_mountpoint, remote_conn_string, remote_mountpoint,
                         objects_to_fetch=list(set(o[2] for o in object_meta)))

    # Map the tables to the actual objects no matter whether or not we're downloading them.
    register_objects(conn, local_mountpoint, object_meta)

    # Don't check anything out, keep the repo bare.
    set_head(conn, local_mountpoint, None)

    if get_remote_for(conn, local_mountpoint) is None:
        add_remote(conn, local_mountpoint, remote_conn_string, remote_mountpoint)


def download_objects(conn, local_mountpoint, remote_conn_string, remote_mountpoint, objects_to_fetch):
    # Fetches the required objects from the remote and stores them locally. Does nothing for objects that already exist.
    existing_objects = get_downloaded_objects(conn, local_mountpoint)
    objects_to_fetch = set(o for o in objects_to_fetch if o not in existing_objects)
    if not objects_to_fetch:
        return

    # Instead of connecting and pushing queries to it from the Python client, we just mount the remote mountpoint
    # into a temporary space (without any checking out) and SELECT the required data into our local tables.
    match = re.match('(\S+):(\S+)@(.+):(\d+)/(\S+)', remote_conn_string)
    remote_data_mountpoint = 'tmp_remote_data'
    unmount(conn, remote_data_mountpoint)  # Maybe worth making sure we're not stepping on anyone else
    mount_postgres(conn, server=match.group(3), port=int(match.group(4)),
                   username=match.group(1), password=match.group(2), mountpoint=remote_data_mountpoint,
                   extra_options={'dbname': match.group(5), 'remote_schema': remote_mountpoint})

    for i, obj in enumerate(objects_to_fetch):
        _log("(%d/%d) %s..." % (i + 1, len(objects_to_fetch), obj))
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE %s AS SELECT * FROM %s""" % (
                cur.mogrify('%s.%s' % (local_mountpoint, obj)),
                cur.mogrify('%s.%s' % (remote_data_mountpoint, obj))))
    unmount(conn, remote_data_mountpoint)


def push(conn, remote_conn, remote_mountpoint, local_mountpoint):
    ensure_metadata_schema(conn)
    # Inverse of pull: uploads missing pack/snap tables to the remote and updates its index.
    # Could actually be done by flipping the arguments in pull but that assumes the remote SG driver can connect
    # to us directly, which might not be the case. Although tunnels?

    # Still, a lot of code here similar to pull.
    _log("Connecting to the remote driver...")
    match = re.match('(\S+):(\S+)@(.+):(\d+)/(\S+)', remote_conn)
    remote_conn = _make_conn(server=match.group(3), port=int(match.group(4)), username=match.group(1),
                             password=match.group(2), dbname=match.group(5))

    _log("Gathering remote metadata...")
    snaps_to_push, object_meta = _get_required_snaps_objects(remote_conn, conn, remote_mountpoint, local_mountpoint)

    if not snaps_to_push:
        _log("Nothing to do.")
        return

    existing_objects = get_existing_objects(remote_conn, remote_mountpoint)
    objects_to_push = list(set(o[2] for o in object_meta if o[2] not in existing_objects))

    _log("Uploading objects...")
    # Difference from pull here: since we can't get remote to mount us, we instead use normal SQL statements
    # to create new tables remotely, then mount them and write into them from our side.
    # Is there seriously no better way to do this?
    for object_id in objects_to_push:
        with conn.cursor() as cur:
            cur.execute("""SELECT column_name, data_type, is_nullable
                           FROM information_schema.columns
                           WHERE table_name = %s AND table_schema = %s""", (object_id, local_mountpoint))
            cols = cur.fetchall()
        with remote_conn.cursor() as cur:
            query = """CREATE TABLE %s (""" % cur.mogrify("%s.%s" % (remote_mountpoint, object_id)) \
                    + ",".join("%s %s %s" % (cur.mogrify(cname), ctype, "NOT NULL" if not cnull else "")
                               for cname, ctype, cnull in cols) + ")"
            cur.execute(query)
    # Have to commit the remote connection here since otherwise we won't see the new tables in the
    # mounted remote.
    remote_conn.commit()

    remote_data_mountpoint = 'tmp_remote_data'
    unmount(conn, remote_data_mountpoint)
    mount_postgres(conn, server=match.group(3), port=int(match.group(4)),
                   username=match.group(1), password=match.group(2), mountpoint=remote_data_mountpoint,
                   extra_options={'dbname': match.group(5), 'remote_schema': remote_mountpoint})

    for i, obj in enumerate(objects_to_push):
        _log("(%d/%d) %s..." % (i + 1, len(objects_to_push), obj))
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO %s SELECT * FROM %s""" % (
                cur.mogrify('%s.%s' % (remote_data_mountpoint, obj)),
                cur.mogrify('%s.%s' % (local_mountpoint, obj))))
    unmount(conn, remote_data_mountpoint)

    register_objects(remote_conn, remote_mountpoint, object_meta)
    # Kind of have to commit here in any case?
    remote_conn.commit()
