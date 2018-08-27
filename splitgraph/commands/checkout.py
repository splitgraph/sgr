from contextlib import contextmanager

from psycopg2.sql import Identifier, SQL

from splitgraph.commands.object_loading import download_objects
from splitgraph.constants import log, get_random_object_id, SplitGraphException, SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler import get_table_with_format, get_remote_for, get_canonical_snap_id, get_tables_at, \
    get_all_tables, set_head, register_table, deregister_table_object, \
    get_external_object_locations, get_tagged_id
from splitgraph.pg_replication import apply_record_to_staging, discard_pending_changes,\
    get_closest_parent_snap_object, suspend_replication
from splitgraph.pg_utils import copy_table, get_primary_keys


def materialize_table(conn, mountpoint, image_hash, table, destination, destination_mountpoint=None):
    """
    Materializes a SplitGraph table in the target schema as a normal Postgres table, potentially downloading all
    required objects and using them to reconstruct the table.
    :param conn: psycopg connection object
    :param mountpoint: Target mountpoint to materialize the table in.
    :param image_hash: Hash of the commit to get the table from.
    :param table: Name of the table.
    :param destination: Name of the destination table.
    :param destination_mountpoint: Name of the destination schema.
    """
    destination_mountpoint = destination_mountpoint or mountpoint
    with conn.cursor() as cur:
        cur.execute(
            SQL("DROP TABLE IF EXISTS {}.{}").format(Identifier(destination_mountpoint), Identifier(destination)))
        # Get the closest snapshot from the table's parents
        # and then apply all deltas consecutively from it.
        object_id, to_apply = get_closest_parent_snap_object(conn, mountpoint, table, image_hash)

        # Make sure all the objects have been downloaded from remote if it exists
        remote_info = get_remote_for(conn, mountpoint)
        if remote_info:
            remote_conn, _ = remote_info
            object_locations = get_external_object_locations(conn, to_apply + [object_id])
            download_objects(conn, remote_conn, objects_to_fetch=to_apply + [object_id],
                             object_locations=object_locations)

        # Copy the given snap id over to "staging"
        copy_table(conn, SPLITGRAPH_META_SCHEMA, object_id, destination_mountpoint, destination,
                   with_pk_constraints=True)
        # This is to work around logical replication not reflecting deletions from non-PKd tables. However, this makes
        # it emit all column values in the row, not just the updated ones.
        if not get_primary_keys(conn, destination_mountpoint, destination):
            log("WARN: table %s has no primary key. "
                "This means that changes will have to be recorded as whole-row." % destination)
            cur.execute(SQL("ALTER TABLE {}.{} REPLICA IDENTITY FULL").format(Identifier(destination_mountpoint),
                                                                              Identifier(destination)))

        # Apply the deltas sequentially to the checked out table
        for pack_object in reversed(to_apply):
            log("Applying %s..." % pack_object)
            apply_record_to_staging(conn, pack_object, destination_mountpoint, destination)


@suspend_replication
def checkout(conn, mountpoint, image_hash=None, tag=None, tables=None):
    """
    Discards all pending changes in the current mountpoint and checks out an image, changing the current HEAD pointer.
    :param conn: psycopg connection object
    :param mountpoint: Mountpoint to check out.
    :param image_hash: Hash of the image to check out.
    :param tag: Tag of the image to check out. One of `image_hash` or `tag` must be specified.
    :param tables: List of tables to materialize in the mountpoint.
    """
    if tables is None:
        tables = []
    discard_pending_changes(conn, mountpoint)
    # Detect the actual schema snap we want to check out
    if image_hash:
        # get_canonical_snap_id called twice if the commandline entry point already called it. How to fix?
        image_hash = get_canonical_snap_id(conn, mountpoint, image_hash)
    elif tag:
        image_hash = get_tagged_id(conn, mountpoint, tag)
    else:
        raise SplitGraphException("One of schema_snap or tag must be specified!")

    tables = tables or get_tables_at(conn, mountpoint, image_hash)
    with conn.cursor() as cur:
        # Drop all current tables in staging
        for table in get_all_tables(conn, mountpoint):
            cur.execute(SQL("DROP TABLE IF EXISTS {}.{}").format(Identifier(mountpoint), Identifier(table)))

    for table in tables:
        materialize_table(conn, mountpoint, image_hash, table, table)

    # Repoint the current HEAD for this mountpoint to the new snap ID
    set_head(conn, mountpoint, image_hash)

    conn.commit()
    log("Checked out %s:%s." % (mountpoint, image_hash[:12]))


@contextmanager
def materialized_table(conn, mountpoint, table_name, snap):
    """A context manager that returns a pointer to a read-only materialized table. If the table is already stored
    as a SNAP, this doesn't use any extra space. Otherwise, the table is materialized and deleted on exit from the
    context manager."""
    if snap is not None:
        with conn.cursor() as cur:
            # See if the table snapshot already exists, otherwise reconstruct it
            object_id = get_table_with_format(conn, mountpoint, table_name, snap, 'SNAP')
            if object_id is None:
                # Materialize the SNAP into a new object
                new_id = get_random_object_id()
                materialize_table(conn, mountpoint, snap, table_name, new_id)
                register_table(conn, mountpoint, table_name, snap, new_id)
                yield new_id
                # Maybe some cache management/expiry strategies here
                cur.execute(
                    SQL("DROP TABLE IF EXISTS {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)))
                deregister_table_object(conn, object_id)
            else:
                yield object_id
    else:
        # No snapshot -- just return the current staging table.
        yield table_name
