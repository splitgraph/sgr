import logging
from random import getrandbits

from psycopg2.sql import Identifier, SQL

from splitgraph.commands.checkout import materialize_table, checkout
from splitgraph.commands.misc import unmount
from splitgraph.commands.push_pull import clone
from splitgraph.constants import SPLITGRAPH_META_SCHEMA, get_random_object_id
from splitgraph.meta_handler import get_current_head, add_new_snap_id, register_table, set_head, get_table, \
    get_tables_at, get_all_tables, register_object, get_all_foreign_tables
from splitgraph.pg_replication import manage_audit
from splitgraph.pg_utils import copy_table, get_primary_keys, execute_sql_in


@manage_audit
def import_tables(conn, mountpoint, tables, target_mountpoint, target_tables, image_hash=None, foreign_tables=False,
                  do_checkout=True, target_hash=None, table_queries=[]):
    """
    Creates a new commit in target_mountpoint with one or more tables linked to already-existing tables.
    After this operation, the HEAD of the target mountpoint moves to the new commit and the new tables are materialized.
    :param conn: psycopg connection object
    :param mountpoint: Mountpoint to get the source table(s) from.
    :param tables: List of tables to import. If empty, imports all tables.
    :param target_mountpoint: Mountpoint to import tables into.
    :param target_tables: If not empty, must be the list of the same length as `tables` specifying names to store them
        under in the target mountpoint.
    :param image_hash: Commit hash on the source mountpoint to import tables from.
        Uses the current source HEAD by default.
    :param foreign_tables: If True, copies all source tables to create a series of new SNAP objects instead of treating
        them as SplitGraph-versioned tables.
    :param do_checkout: If False, doesn't materialize the tables in the target mountpoint.
    :param target_hash: Hash of the new image that tables is recorded under. If None, gets chosen at random.
    :param table_queries: If not [], it's treated as a Boolean mask showing which entries in the `tables` list are
        instead SELECT SQL queries that form the target table. The queries have to be non-schema qualified and work only
        against tables in the source mountpoint. Each target table created is the result of the respective SQL query.
        This is committed as a new snapshot.
    :return: Hash that the new image was stored under.
    """
    # Sanitize/validate the parameters and call the internal function.
    target_hash = target_hash or "%0.2x" % getrandbits(256)

    if not foreign_tables:
        image_hash = image_hash or get_current_head(conn, mountpoint)

    if not tables:
        tables = get_tables_at(conn, mountpoint, image_hash) if not foreign_tables \
            else get_all_foreign_tables(conn, mountpoint)
    if not target_tables:
        if table_queries:
            raise ValueError("target_tables has to be defined if table_queries is True!")
        target_tables = tables
    if not table_queries:
        table_queries = [False] * len(tables)
    if len(tables) != len(target_tables) or len(tables) != len(table_queries):
        raise ValueError("tables, target_tables and table_queries have mismatching lengths!")

    existing_tables = get_all_tables(conn, target_mountpoint)
    clashing = [t for t in target_tables if t in existing_tables]
    if clashing:
        raise ValueError("Table(s) %r already exist(s) at %s!" % (clashing, target_mountpoint))

    return _import_tables(conn, mountpoint, image_hash, tables, target_mountpoint, target_hash, target_tables,
                          do_checkout, table_queries, foreign_tables)


def _import_tables(conn, mountpoint, image_hash, tables, target_mountpoint, target_hash, target_tables, do_checkout,
                   table_queries, foreign_tables):
    head = get_current_head(conn, target_mountpoint, raise_on_none=False)
    # Add the new snap ID to the tree
    add_new_snap_id(conn, target_mountpoint, head, target_hash, comment="Importing %s from %s" % (tables, mountpoint))

    if any(table_queries) and not foreign_tables:
        # If we want to run some queries against the source mountpoint to create the new tables,
        # we have to materialize it fully.
        checkout(conn, mountpoint, image_hash)
    # Materialize the actual tables in the target mountpoint and register them.
    for table, target_table, is_query in zip(tables, target_tables, table_queries):
        if foreign_tables or is_query:
            # For foreign tables/SELECT queries, we define a new object/table instead.
            object_id = get_random_object_id()
            if is_query:
                # is_query precedes foreign_tables: if we're importing using a query, we don't care if it's a
                # foreign table or not since we're storing it as a full snapshot.
                execute_sql_in(conn, mountpoint,
                               SQL("CREATE TABLE {}.{} AS ").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                    Identifier(object_id)) + SQL(table))
            elif foreign_tables:
                copy_table(conn, mountpoint, table, SPLITGRAPH_META_SCHEMA, object_id)

            _register_and_checkout_new_table(conn, do_checkout, object_id, target_hash, target_mountpoint, target_table)
        else:
            for object_id, _ in get_table(conn, mountpoint, table, image_hash):
                register_table(conn, target_mountpoint, target_table, target_hash, object_id)
            if do_checkout:
                materialize_table(conn, mountpoint, image_hash, table, target_table,
                                  destination_mountpoint=target_mountpoint)
    # Register the existing tables at the new commit as well.
    if head is not None:
        with conn.cursor() as cur:
            cur.execute(SQL("""INSERT INTO {0}.tables (mountpoint, snap_id, table_name, object_id)
                (SELECT %s, %s, table_name, object_id FROM {0}.tables
                WHERE mountpoint = %s AND snap_id = %s)""").format(Identifier(SPLITGRAPH_META_SCHEMA)),
                        (target_mountpoint, target_hash, target_mountpoint, head))
    set_head(conn, target_mountpoint, target_hash)
    return target_hash


def _register_and_checkout_new_table(conn, do_checkout, object_id, target_hash, target_mountpoint, target_table):
    # Might not be necessary if we don't actually want to materialize the snapshot (wastes space).
    register_object(conn, object_id, 'SNAP', parent_object=None)
    register_table(conn, target_mountpoint, target_table, target_hash, object_id)
    if do_checkout:
        copy_table(conn, SPLITGRAPH_META_SCHEMA, object_id, target_mountpoint, target_table)
        if not get_primary_keys(conn, target_mountpoint, target_table):
            logging.warning(
                "Table %s has no primary key. This means that changes will have to be recorded as "
                "whole-row.", target_table)
            with conn.cursor() as cur:
                cur.execute(
                    SQL("ALTER TABLE {}.{} REPLICA IDENTITY FULL").format(Identifier(target_mountpoint),
                                                                          Identifier(target_table)))


def import_table_from_unmounted(conn, remote_conn_string, remote_mountpoint, remote_tables, remote_image_hash,
                                target_mountpoint, target_tables, target_hash=None):
    # Shorthand for importing one or more tables from a yet-uncloned remote. Here, the remote image hash
    # is required, as otherwise we aren't necessarily able to determine what the remote head is.

    # In the future, we could do some vaguely intelligent interrogation of the remote to directly copy the required
    # metadata (object locations and relationships) into the local mountpoint. However, since the metadata is fairly
    # lightweight (we never download unneeded objects), we just clone it into a temporary mountpoint,
    # do the import into the target and destroy the temporary mp.
    tmp_mountpoint = remote_mountpoint + '_clone_tmp'

    clone(conn, remote_mountpoint, remote_conn_string=remote_conn_string,
          local_mountpoint=tmp_mountpoint, download_all=False)
    import_tables(conn, tmp_mountpoint, remote_tables, target_mountpoint, target_tables, image_hash=remote_image_hash,
                  target_hash=target_hash)

    unmount(conn, tmp_mountpoint)
    conn.commit()
