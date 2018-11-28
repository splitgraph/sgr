from psycopg2.extras import execute_batch
from psycopg2.sql import SQL, Identifier

from splitgraph._data.common import select, insert
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.connection import get_connection


def get_full_object_tree():
    with get_connection().cursor() as cur:
        cur.execute(select("objects", "object_id,parent_id,format"))
        return cur.fetchall()


def get_object_parents(object_id):
    with get_connection().cursor() as cur:
        cur.execute(select("objects", "parent_id", "object_id = %s"), (object_id,))
        return [c[0] for c in cur.fetchall()]


def get_object_format(object_id):
    with get_connection().cursor() as cur:
        cur.execute(select("objects", "format", "object_id = %s"), (object_id,))
        result = cur.fetchone()
        return None if result is None else result[0]


def register_object(object_id, object_format, namespace, parent_object=None):
    if not parent_object and object_format != 'SNAP':
        raise ValueError("Non-SNAP objects can't have no parent!")
    with get_connection().cursor() as cur:
        cur.execute(insert("objects", ("object_id", "format", "parent_id", "namespace")),
                    (object_id, object_format, parent_object, namespace))


def deregister_table_object(object_id):
    with get_connection().cursor() as cur:
        query = SQL("DELETE FROM {}.tables WHERE object_id = %s").format(Identifier(SPLITGRAPH_META_SCHEMA))
        cur.execute(query, (object_id,))


def register_objects(object_meta):
    with get_connection().cursor() as cur:
        execute_batch(cur, insert("objects", ("object_id", "format", "parent_id", "namespace")),
                      object_meta, page_size=100)


def register_tables(repository, table_meta):
    table_meta = [(repository.namespace, repository.repository) + o for o in table_meta]
    with get_connection().cursor() as cur:
        execute_batch(cur, insert("tables", ("namespace", "repository", "image_hash", "table_name", "object_id")),
                      table_meta, page_size=100)


def register_object_locations(object_locations):
    with get_connection().cursor() as cur:
        # Don't insert redundant objects here either.
        cur.execute(select("object_locations", "object_id"))
        existing_locations = [c[0] for c in cur.fetchall()]
        object_locations = [o for o in object_locations if o[0] not in existing_locations]

        execute_batch(cur, insert("object_locations", ("object_id", "location", "protocol")),
                      object_locations, page_size=100)


def get_existing_objects():
    with get_connection().cursor() as cur:
        cur.execute(select("objects", "object_id"))
        return set(c[0] for c in cur.fetchall())


def get_downloaded_objects():
    # Minor normalization sadness here: this can return duplicate object IDs since
    # we might repeat them if different versions of the same table point to the same object ID.
    with get_connection().cursor() as cur:
        cur.execute(SQL("""SELECT information_schema.tables.table_name FROM information_schema.tables JOIN {}.tables
                        ON information_schema.tables.table_name = {}.tables.object_id
                        WHERE information_schema.tables.table_schema = %s""").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                                     Identifier(
                                                                                         SPLITGRAPH_META_SCHEMA)),
                    (SPLITGRAPH_META_SCHEMA,))
        return set(c[0] for c in cur.fetchall())


def get_external_object_locations(objects):
    with get_connection().cursor() as cur:
        query = select("object_locations", "object_id, location, protocol",
                       "object_id IN (" + ','.join('%s' for _ in objects) + ")")
        cur.execute(query, objects)
        object_locations = cur.fetchall()
    return object_locations


def get_object_meta(objects):
    with get_connection().cursor() as cur:
        cur.execute(select("objects", "object_id, format, parent_id, namespace",
                           "object_id IN (" + ','.join('%s' for _ in objects) + ")"), objects)
        return cur.fetchall()


def register_table(repository, table, image, object_id):
    with get_connection().cursor() as cur:
        cur.execute(insert("tables", ("namespace", "repository", "image_hash", "table_name", "object_id")),
                    (repository.namespace, repository.repository, image, table, object_id))


def get_object_for_table(repository, table_name, image, object_format):
    # Returns the object ID of a table at a given time, with a given format.
    with get_connection().cursor() as cur:
        cur.execute(SQL("""SELECT {0}.tables.object_id FROM {0}.tables JOIN {0}.objects
                            ON {0}.objects.object_id = {0}.tables.object_id
                            WHERE {0}.tables.namespace = %s AND repository = %s AND image_hash = %s
                            AND table_name = %s AND format = %s""").format(
            Identifier(SPLITGRAPH_META_SCHEMA)), (repository.namespace, repository.repository,
                                                  image, table_name, object_format))
        result = cur.fetchone()
        return None if result is None else result[0]
