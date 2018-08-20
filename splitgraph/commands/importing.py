from random import getrandbits

from psycopg2.sql import Identifier, SQL

from splitgraph.commands.checkout import materialize_table
from splitgraph.commands.misc import unmount
from splitgraph.commands.push_pull import clone
from splitgraph.constants import SPLITGRAPH_META_SCHEMA, get_random_object_id, _log
from splitgraph.meta_handler import get_current_head, add_new_snap_id, register_table, set_head, get_table, \
    get_tables_at, get_all_tables, register_object, get_all_foreign_tables
from splitgraph.pg_replication import suspend_replication
from splitgraph.pg_utils import copy_table, _get_primary_keys


@suspend_replication
def import_tables(conn, mountpoint, tables, target_mountpoint, target_tables, image_hash=None, foreign_tables=False,
                  do_checkout=True, target_hash=None):
    # Creates a new commit in target_mountpoint with one or more tables linked to already-existing tables.
    # After this operation, the HEAD of the target mountpoint moves to the new commit and the new tables
    # are materialized.
    # If tables = [], all tables are imported.
    # If target_tables = [], the target tables will have the same names as the original tables.
    # foreign_tables=True is for tables that aren't actually SG snapshots but have been mounted via an FDW or
    # just exist in the driver in some other way. Creates a new object/commit as well.
    HEAD = get_current_head(conn, target_mountpoint, raise_on_none=False)
    target_hash = target_hash or "%0.2x" % getrandbits(256)

    if not foreign_tables:
        image_hash = image_hash or get_current_head(conn, mountpoint)

    if not tables:
        tables = get_tables_at(conn, mountpoint, image_hash) if not foreign_tables \
            else get_all_foreign_tables(conn, mountpoint)
    if not target_tables:
        target_tables = tables
    if len(tables) != len(target_tables):
        raise ValueError("tables and target_tables have mismatching lengths!")

    existing_tables = get_all_tables(conn, target_mountpoint)
    clashing = [t for t in target_tables if t in existing_tables]
    if clashing:
        raise ValueError("Table(s) %r already exist(s) at %s!" % (clashing, target_mountpoint))

    # Add the new snap ID to the tree
    add_new_snap_id(conn, target_mountpoint, HEAD, target_hash, comment="Importing %s from %s" % (tables, mountpoint))

    # Materialize the actual tables in the target mountpoint and register them.
    for table, target_table in zip(tables, target_tables):
        if foreign_tables:
            # For foreign tables, we define a new object/table instead.
            object_id = get_random_object_id()
            copy_table(conn, mountpoint, table, SPLITGRAPH_META_SCHEMA, object_id)
            # Might not be necessary if we don't actually want to materialize the snapshot (wastes space).
            register_object(conn, object_id, 'SNAP', parent_object=None)
            register_table(conn, target_mountpoint, target_table, target_hash, object_id)
            if do_checkout:
                copy_table(conn, SPLITGRAPH_META_SCHEMA, object_id, target_mountpoint, target_table)
                if not _get_primary_keys(conn, target_mountpoint, target_table):
                    _log(
                        "WARN: table %s has no primary key. This means that changes will have to be recorded as "
                        "whole-row." % target_table)
                    with conn.cursor() as cur:
                        cur.execute(
                            SQL("ALTER TABLE {}.{} REPLICA IDENTITY FULL").format(Identifier(target_mountpoint),
                                                                                  Identifier(target_table)))
        else:
            for object_id, _ in get_table(conn, mountpoint, table, image_hash):
                register_table(conn, target_mountpoint, target_table, target_hash, object_id)
            if do_checkout:
                materialize_table(conn, mountpoint, image_hash, table, target_table,
                                  destination_mountpoint=target_mountpoint)

    # Register the existing tables at the new commit as well.
    if HEAD is not None:
        with conn.cursor() as cur:
            cur.execute(SQL("""INSERT INTO {0}.tables (mountpoint, snap_id, table_name, object_id)
                (SELECT %s, %s, table_name, object_id FROM {0}.tables
                WHERE mountpoint = %s AND snap_id = %s)""").format(Identifier(SPLITGRAPH_META_SCHEMA)),
                        (target_mountpoint, target_hash, target_mountpoint, HEAD))

    set_head(conn, target_mountpoint, target_hash)
    conn.commit()
    return target_hash


def import_table_from_unmounted(conn, remote_conn_string, remote_mountpoint, remote_tables, remote_image_hash,
                                target_mountpoint, target_tables, target_hash=None):
    # Shorthand for importing one or more tables from a yet-uncloned remote. Here, the remote image hash
    # is required, as otherwise we aren't necessarily able to determine what the remote head is.

    # In the future, we could do some vaguely intelligent interrogation of the remote to directly copy the required
    # metadata (object locations and relationships) into the local mountpoint. However, since the metadata is fairly
    # lightweight (we never download unneeded objects), we just clone it into a temporary mountpoint,
    # do the import into the target and destroy the temporary mp.
    tmp_mountpoint = remote_mountpoint + '_clone_tmp'

    clone(conn, remote_conn_string, remote_mountpoint, tmp_mountpoint, download_all=False)
    import_tables(conn, tmp_mountpoint, remote_tables, target_mountpoint, target_tables, image_hash=remote_image_hash,
                  target_hash=target_hash)

    unmount(conn, tmp_mountpoint)
    conn.commit()
