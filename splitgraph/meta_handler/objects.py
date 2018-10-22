from psycopg2.extras import execute_batch
from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler.common import select, insert


def get_object_parents(conn, object_id):
    with conn.cursor() as cur:
        cur.execute(select("objects", "parent_id", "object_id = %s"), (object_id,))
        return [c[0] for c in cur.fetchall()]


def get_object_format(conn, object_id):
    with conn.cursor() as cur:
        cur.execute(select("objects", "format", "object_id = %s"), (object_id,))
        result = cur.fetchone()
        return None if result is None else result[0]


def register_object(conn, object_id, object_format, parent_object=None):
    if not parent_object and object_format != 'SNAP':
        raise ValueError("Non-SNAP objects can't have no parent!")
    with conn.cursor() as cur:
        cur.execute(insert("objects", ("object_id", "format", "parent_id")),
                    (object_id, object_format, parent_object))


def deregister_table_object(conn, object_id):
    with conn.cursor() as cur:
        query = SQL("DELETE FROM {}.tables WHERE object_id = %s").format(Identifier(SPLITGRAPH_META_SCHEMA))
        cur.execute(query, (object_id,))


def register_objects(conn, object_meta):
    with conn.cursor() as cur:
        execute_batch(cur, insert("objects", ("object_id", "format", "parent_id")), object_meta, page_size=100)


def register_tables(conn, repository, table_meta, namespace=''):
    table_meta = [(namespace, repository) + o for o in table_meta]
    with conn.cursor() as cur:
        execute_batch(cur, insert("tables", ("namespace", "repository", "image_hash", "table_name", "object_id")),
                      table_meta, page_size=100)


def register_object_locations(conn, object_locations):
    with conn.cursor() as cur:
        # Don't insert redundant objects here either.
        cur.execute(select("object_locations", "object_id"))
        existing_locations = [c[0] for c in cur.fetchall()]
        object_locations = [o for o in object_locations if o[0] not in existing_locations]

        execute_batch(cur, insert("object_locations", ("object_id", "location", "protocol")),
                      object_locations, page_size=100)


def get_existing_objects(conn):
    with conn.cursor() as cur:
        cur.execute(select("objects", "object_id"))
        return set(c[0] for c in cur.fetchall())


def get_downloaded_objects(conn):
    # Minor normalization sadness here: this can return duplicate object IDs since
    # we might repeat them if different versions of the same table point to the same object ID.
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT information_schema.tables.table_name FROM information_schema.tables JOIN {}.tables
                        ON information_schema.tables.table_name = {}.tables.object_id
                        WHERE information_schema.tables.table_schema = %s""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                                     Identifier(
                                                                                         SPLITGRAPH_META_SCHEMA)),
                    (SPLITGRAPH_META_SCHEMA,))
        return set(c[0] for c in cur.fetchall())


def get_external_object_locations(conn, objects):
    with conn.cursor() as cur:
        query = select("object_locations", "object_id, location, protocol",
                       "object_id IN (" + ','.join('%s' for _ in objects) + ")")
        cur.execute(query, objects)
        object_locations = cur.fetchall()
    return object_locations


def get_object_meta(conn, objects):
    with conn.cursor() as cur:
        cur.execute(select("objects", "object_id, format, parent_id",
                           "object_id IN (" + ','.join('%s' for _ in objects) + ")"), objects)
        return cur.fetchall()


def register_table(conn, repository, table, image, object_id, namespace=''):
    with conn.cursor() as cur:
        cur.execute(insert("tables", ("namespace", "repository", "image_hash", "table_name", "object_id")),
                    (namespace, repository, image, table, object_id))
