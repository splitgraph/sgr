from contextlib import contextmanager
from psycopg2.sql import Identifier, SQL

from splitgraph.commands.object_loading import download_objects
from splitgraph.constants import _log, get_random_object_id, SplitGraphException
from splitgraph.meta_handler import get_table_with_format, get_snap_parent, get_remote_for, ensure_metadata_schema, \
    get_canonical_snap_id, get_tables_at, get_all_tables, set_head, register_table_object, deregister_table_object, \
    get_external_object_locations, get_tagged_id
from splitgraph.pg_replication import apply_record_to_staging, record_pending_changes, stop_replication, \
    discard_pending_changes, start_replication


def materialize_table(conn, mountpoint, schema_snap, table, destination):
    with conn.cursor() as cur:
        cur.execute(SQL("DROP TABLE IF EXISTS {}.{}").format(Identifier(mountpoint), Identifier(destination)))
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
            object_locations = get_external_object_locations(conn, mountpoint, to_apply + [object_id])
            download_objects(conn, mountpoint, remote_conn, remote_mountpoint,
                             objects_to_fetch=to_apply + [object_id], object_locations=object_locations)

        # Copy the given snap id over to "staging"
        cur.execute(SQL("CREATE TABLE {}.{} AS SELECT * FROM {}.{}").format(
                    Identifier(mountpoint), Identifier(destination),
                    Identifier(mountpoint), Identifier(object_id)))
        # This is to work around logical replication not reflecting deletions from non-PKd tables. However, this makes
        # it emit all column values in the row, not just the updated ones.
        # FIXME: fiddling with it based on us inspecting the table structure.
        cur.execute(SQL("ALTER TABLE {}.{} REPLICA IDENTITY FULL").format(Identifier(mountpoint), Identifier(destination)))

        # Apply the deltas sequentially to the checked out table
        for pack_object in reversed(to_apply):
            _log("Applying %s..." % pack_object)
            apply_record_to_staging(conn, mountpoint, pack_object, destination)


def checkout(conn, mountpoint, schema_snap=None, tag=None, tables=[]):
    ensure_metadata_schema(conn)
    record_pending_changes(conn)
    stop_replication(conn)
    discard_pending_changes(conn, mountpoint)

    # Detect the actual schema snap we want to check out
    # TODO this is strange: if the stop_replication/discard_pending_changes lines are moved down
    # below this block (after getting the actual ID to check out), then the very last sgfile test
    # (test_sgfile_rebase) fails with an entry being duplicated in a table. Just that test. Wut?
    if schema_snap:
        # get_canonical_snap_id called twice if the commandline entry point already called it. How to fix?
        schema_snap = get_canonical_snap_id(conn, mountpoint, schema_snap)
    elif tag:
        schema_snap = get_tagged_id(conn, mountpoint, tag)
    else:
        raise SplitGraphException("One of schema_snap or tag must be specified!")

    tables = tables or get_tables_at(conn, mountpoint, schema_snap)
    with conn.cursor() as cur:
        # Drop all current tables in staging
        for table in get_all_tables(conn, mountpoint):
            cur.execute(SQL("DROP TABLE IF EXISTS {}.{}").format(Identifier(mountpoint), Identifier(table)))

    for table in tables:
        materialize_table(conn, mountpoint, schema_snap, table, table)

    # Repoint the current HEAD for this mountpoint to the new snap ID
    set_head(conn, mountpoint, schema_snap)

    # Start recording changes to the mountpoint.
    conn.commit()
    start_replication(conn)

    _log("Checked out %s:%s." % (mountpoint, schema_snap[:12]))


@contextmanager
def materialized_table(conn, mountpoint, table_name, snap):
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