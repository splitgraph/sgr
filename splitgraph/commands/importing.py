from random import getrandbits

from psycopg2.sql import Identifier, SQL

from splitgraph.commands._objects.utils import get_random_object_id
from splitgraph.commands.checkout import materialize_table, checkout
from splitgraph.commands.info import get_tables_at, get_table
from splitgraph.commands.misc import unmount
from splitgraph.commands.push_pull import clone
from splitgraph.commands.tagging import get_current_head
from splitgraph.connection import get_connection
from splitgraph.constants import SPLITGRAPH_META_SCHEMA, Repository
from splitgraph.meta_handler.images import add_new_image
from splitgraph.meta_handler.objects import register_table, register_object
from splitgraph.pg_utils import copy_table, execute_sql_in, get_all_tables, get_all_foreign_tables
from ._common import set_head
from ._pg_audit import manage_audit


@manage_audit
def import_tables(repository, tables, target_repository, target_tables, image_hash=None, foreign_tables=False,
                  do_checkout=True, target_hash=None, table_queries=None):
    """
    Creates a new commit in target_mountpoint with one or more tables linked to already-existing tables.
    After this operation, the HEAD of the target mountpoint moves to the new commit and the new tables are materialized.

    :param repository: Mountpoint to get the source table(s) from.
    :param tables: List of tables to import. If empty, imports all tables.
    :param target_repository: Mountpoint to import tables into.
    :param target_tables: If not empty, must be the list of the same length as `tables` specifying names to store them
        under in the target mountpoint.
    :param image_hash: Commit hash on the source mountpoint to import tables from.
        Uses the current source HEAD by default.
    :param foreign_tables: If True, copies all source tables to create a series of new SNAP objects instead of treating
    them as SplitGraph-versioned tables. This is useful for adding brand new tables
        (for example, from an FDW-mounted table).
    :param do_checkout: If False, doesn't materialize the tables in the target mountpoint.
    :param target_hash: Hash of the new image that tables is recorded under. If None, gets chosen at random.
    :param table_queries: If not [], it's treated as a Boolean mask showing which entries in the `tables` list are
        instead SELECT SQL queries that form the target table. The queries have to be non-schema qualified and work only
        against tables in the source mountpoint. Each target table created is the result of the respective SQL query.
        This is committed as a new snapshot.
    :return: Hash that the new image was stored under.
    """
    # Sanitize/validate the parameters and call the internal function.
    if table_queries is None:
        table_queries = []
    target_hash = target_hash or "%0.2x" % getrandbits(256)
    conn = get_connection()

    if not foreign_tables:
        image_hash = image_hash or get_current_head(repository)

    if not tables:
        tables = get_tables_at(repository, image_hash) if not foreign_tables \
            else get_all_foreign_tables(conn, repository.to_schema())
    if not target_tables:
        if table_queries:
            raise ValueError("target_tables has to be defined if table_queries is True!")
        target_tables = tables
    if not table_queries:
        table_queries = [False] * len(tables)
    if len(tables) != len(target_tables) or len(tables) != len(table_queries):
        raise ValueError("tables, target_tables and table_queries have mismatching lengths!")

    existing_tables = get_all_tables(conn, target_repository.to_schema())
    clashing = [t for t in target_tables if t in existing_tables]
    if clashing:
        raise ValueError("Table(s) %r already exist(s) at %s!" % (clashing, target_repository))

    return _import_tables(repository, image_hash, tables, target_repository, target_hash, target_tables, do_checkout,
                          table_queries, foreign_tables)


def _import_tables(repository, image_hash, tables, target_repository, target_hash, target_tables, do_checkout,
                   table_queries, foreign_tables):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(target_repository.to_schema())))

    head = get_current_head(target_repository, raise_on_none=False)
    # Add the new snap ID to the tree
    add_new_image(target_repository, head, target_hash, comment="Importing %s from %s" % (tables, repository))

    if any(table_queries) and not foreign_tables:
        # If we want to run some queries against the source repository to create the new tables,
        # we have to materialize it fully.
        checkout(repository, image_hash)
    # Materialize the actual tables in the target repository and register them.
    for table, target_table, is_query in zip(tables, target_tables, table_queries):
        if foreign_tables or is_query:
            # For foreign tables/SELECT queries, we define a new object/table instead.
            object_id = get_random_object_id()
            if is_query:
                # is_query precedes foreign_tables: if we're importing using a query, we don't care if it's a
                # foreign table or not since we're storing it as a full snapshot.
                execute_sql_in(conn, repository.to_schema(),
                               SQL("CREATE TABLE {}.{} AS ").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                    Identifier(object_id)) + SQL(table))
            elif foreign_tables:
                copy_table(conn, repository.to_schema(), table, SPLITGRAPH_META_SCHEMA, object_id)

            _register_and_checkout_new_table(do_checkout, object_id, target_hash, target_repository, target_table)
        else:
            for object_id, _ in get_table(repository, table, image_hash):
                register_table(target_repository, target_table, target_hash, object_id)
            if do_checkout:
                materialize_table(repository, image_hash, table, target_table,
                                  destination_schema=target_repository.to_schema())
    # Register the existing tables at the new commit as well.
    if head is not None:
        with conn.cursor() as cur:
            cur.execute(SQL("""INSERT INTO {0}.tables (namespace, repository, image_hash, table_name, object_id)
                (SELECT %s, %s, %s, table_name, object_id FROM {0}.tables
                WHERE namespace = %s AND repository = %s AND image_hash = %s)""")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                        (target_repository.namespace, target_repository.repository,
                         target_hash, target_repository.namespace, target_repository.repository, head))
    set_head(target_repository, target_hash)
    return target_hash


def _register_and_checkout_new_table(do_checkout, object_id, target_hash, target_repository, target_table):
    # Might not be necessary if we don't actually want to materialize the snapshot (wastes space).
    register_object(object_id, 'SNAP', namespace=target_repository.namespace, parent_object=None)
    register_table(target_repository, target_table, target_hash, object_id)
    if do_checkout:
        copy_table(get_connection(), SPLITGRAPH_META_SCHEMA, object_id, target_repository.to_schema(), target_table)


def import_table_from_remote(remote_conn_string, remote_repository, remote_tables, remote_image_hash, target_repository,
                             target_tables, target_hash=None):
    # Shorthand for importing one or more tables from a yet-uncloned remote. Here, the remote image hash
    # is required, as otherwise we aren't necessarily able to determine what the remote head is.

    # In the future, we could do some vaguely intelligent interrogation of the remote to directly copy the required
    # metadata (object locations and relationships) into the local mountpoint. However, since the metadata is fairly
    # lightweight (we never download unneeded objects), we just clone it into a temporary mountpoint,
    # do the import into the target and destroy the temporary mp.
    tmp_mountpoint = Repository(namespace=remote_repository.namespace, repository=remote_repository.repository +
                                                                                  '_clone_tmp')

    clone(remote_repository, remote_conn_string=remote_conn_string, local_repository=tmp_mountpoint, download_all=False)
    import_tables(tmp_mountpoint, remote_tables, target_repository, target_tables, image_hash=remote_image_hash,
                  target_hash=target_hash)

    unmount(tmp_mountpoint)
    get_connection().commit()
