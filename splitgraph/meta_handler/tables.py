from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler.common import select


def get_tables_at(conn, repository, image):
    with conn.cursor() as cur:
        cur.execute(select('tables', 'table_name', 'namespace = %s AND repository = %s AND image_hash = %s'),
                    (repository.namespace, repository.repository, image))
        return [t[0] for t in cur.fetchall()]


def get_object_for_table(conn, repository, table_name, image, object_format):
    # Returns the object ID of a table at a given time, with a given format.
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT {0}.tables.object_id FROM {0}.tables JOIN {0}.objects
                            ON {0}.objects.object_id = {0}.tables.object_id
                            WHERE {0}.tables.namespace = %s AND repository = %s AND image_hash = %s
                            AND table_name = %s AND format = %s""").format(
            Identifier(SPLITGRAPH_META_SCHEMA)), (repository.namespace, repository.repository,
                                                  image, table_name, object_format))
        result = cur.fetchone()
        return None if result is None else result[0]


def get_table(conn, repository, table_name, image):
    # Returns a list of available[(object_id, object_format)] from the table meta
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT {0}.tables.object_id, format FROM {0}.tables JOIN {0}.objects
                            ON {0}.objects.object_id = {0}.tables.object_id
                            WHERE {0}.tables.namespace = %s AND repository = %s AND image_hash = %s
                            AND table_name = %s""").format(
            Identifier(SPLITGRAPH_META_SCHEMA)), (repository.namespace, repository.repository, image, table_name))
        return cur.fetchall()
