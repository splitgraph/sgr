from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler.common import select


def get_all_tables(conn, mountpoint):
    # Gets all user tables in the current mountpoint, tracked or untracked
    with conn.cursor() as cur:
        cur.execute(
            """SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s and table_type = 'BASE TABLE'""", (mountpoint,))
        all_table_names = {c[0] for c in cur.fetchall()}

        # Exclude all snapshots/packed tables
        cur.execute(select('tables', 'object_id', 'mountpoint = %s'), (mountpoint,))
        sys_tables = [c[0] for c in cur.fetchall()]
        return list(all_table_names.difference(sys_tables))


def get_tables_at(conn, mountpoint, image):
    # Returns all table names mounted at mountpoint.
    with conn.cursor() as cur:
        cur.execute(select('tables', 'table_name', 'mountpoint = %s AND image_hash = %s'), (mountpoint, image))
        return [t[0] for t in cur.fetchall()]


def get_table_with_format(conn, mountpoint, table_name, image, object_format):
    # Returns the object ID of a table at a given time, with a given format.
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT {0}.tables.object_id FROM {0}.tables JOIN {0}.objects
                            ON {0}.objects.object_id = {0}.tables.object_id
                            WHERE mountpoint = %s AND image_hash = %s AND table_name = %s AND format = %s""").format(
            Identifier(SPLITGRAPH_META_SCHEMA)), (mountpoint, image, table_name, object_format))
        result = cur.fetchone()
        return None if result is None else result[0]


def get_table(conn, mountpoint, table_name, image):
    # Returns a list of available[(object_id, object_format)] from the table meta
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT {0}.tables.object_id, format FROM {0}.tables JOIN {0}.objects
                            ON {0}.objects.object_id = {0}.tables.object_id
                            WHERE mountpoint = %s AND image_hash = %s AND table_name = %s""").format(
            Identifier(SPLITGRAPH_META_SCHEMA)), (mountpoint, image, table_name))
        return cur.fetchall()
