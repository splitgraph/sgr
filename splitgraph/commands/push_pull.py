import re

from splitgraph.commands.misc import make_conn, unmount, mount_postgres, dump_table_creation
from splitgraph.constants import SPLITGRAPH_META_SCHEMA, SplitGraphException, _log
from splitgraph.meta_handler import get_all_snap_parents, add_new_snap_id, get_remote_for, ensure_metadata_schema, \
    register_objects, set_head, add_remote, get_downloaded_objects, get_existing_objects


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
    remote_conn = make_conn(server=match.group(3), port=int(match.group(4)), username=match.group(1),
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


def upload_objects(conn, local_mountpoint, remote_conn_string, remote_mountpoint, objects_to_push):
    match = re.match('(\S+):(\S+)@(.+):(\d+)/(\S+)', remote_conn_string)
    remote_conn = make_conn(server=match.group(3), port=int(match.group(4)), username=match.group(1),
                            password=match.group(2), dbname=match.group(5))
    existing_objects = get_existing_objects(remote_conn, remote_mountpoint)
    objects_to_push = list(set(o for o in objects_to_push if o not in existing_objects))
    _log("Uploading objects...")

    # Difference from pull here: since we can't get remote to mount us, we instead use normal SQL statements
    # to create new tables remotely, then mount them and write into them from our side.
    # Is there seriously no better way to do this?
    with remote_conn.cursor() as cur:
        cur.execute(dump_table_creation(conn, schema=local_mountpoint,
                                        tables=objects_to_push, created_schema=remote_mountpoint))
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


def push(conn, remote_conn_string, remote_mountpoint, local_mountpoint):
    ensure_metadata_schema(conn)
    # Inverse of pull: uploads missing pack/snap tables to the remote and updates its index.
    # Could actually be done by flipping the arguments in pull but that assumes the remote SG driver can connect
    # to us directly, which might not be the case. Although tunnels?

    # Still, a lot of code here similar to pull.
    _log("Connecting to the remote driver...")
    match = re.match('(\S+):(\S+)@(.+):(\d+)/(\S+)', remote_conn_string)
    remote_conn = make_conn(server=match.group(3), port=int(match.group(4)), username=match.group(1),
                            password=match.group(2), dbname=match.group(5))

    _log("Gathering remote metadata...")
    # This also registers new commits remotely. Should make explicit and move down later on.
    snaps_to_push, object_meta = _get_required_snaps_objects(remote_conn, conn, remote_mountpoint, local_mountpoint)

    if not snaps_to_push:
        _log("Nothing to do.")
        return

    # Different upload handlers go here.
    upload_objects(conn, local_mountpoint, remote_conn_string, remote_mountpoint, list(set(o[2] for o in object_meta)))
    register_objects(remote_conn, remote_mountpoint, object_meta)
    # Kind of have to commit here in any case?
    remote_conn.commit()