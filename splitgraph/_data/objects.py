"""
Internal data access functions for accessing the Splitgraph object tree
"""

from psycopg2.extras import execute_batch
from psycopg2.sql import SQL, Identifier

from splitgraph._data.common import select, insert
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.connection import get_connection


def get_full_object_tree():
    """Returns a list of (object_id, parent_id, SNAP/DIFF) with the full object tree in the """
    with get_connection().cursor() as cur:
        cur.execute(select("objects", "object_id,parent_id,format"))
        return cur.fetchall()


def register_object(object_id, object_format, namespace, parent_object=None):
    """
    Registers a Splitgraph object in the object tree
    :param object_id: Object ID
    :param object_format: Format (SNAP or DIFF)
    :param namespace: Namespace that owns the object. In registry mode, only namespace owners can alter or delete
        objects.
    :param parent_object: Parent that the object depends on, if it's a DIFF object.
    :return:
    """
    if not parent_object and object_format != 'SNAP':
        raise ValueError("Non-SNAP objects can't have no parent!")
    with get_connection().cursor() as cur:
        cur.execute(insert("objects", ("object_id", "format", "parent_id", "namespace")),
                    (object_id, object_format, parent_object, namespace))


def deregister_table_object(object_id):
    """
    Deletes an object from the tree.
    :param object_id: Object ID to delete.
    :return:
    """
    with get_connection().cursor() as cur:
        query = SQL("DELETE FROM {}.tables WHERE object_id = %s").format(Identifier(SPLITGRAPH_META_SCHEMA))
        cur.execute(query, (object_id,))


def register_objects(object_meta):
    """
    Registers multiple Splitgraph objects in the tree. See `register_object` for more information.
    :param object_meta: List of (object_id, format, parent_id, namesapce).
    """
    with get_connection().cursor() as cur:
        execute_batch(cur, insert("objects", ("object_id", "format", "parent_id", "namespace")),
                      object_meta, page_size=100)


def register_tables(repository, table_meta):
    """
    Links tables in an image to physical objects that they are stored as.
    Objects must already be registered in the object tree.

    :param repository: Repository that the tables belong to.
    :param table_meta: A list of (image_hash, table_name, object_id).
    """
    table_meta = [(repository.namespace, repository.repository) + o for o in table_meta]
    with get_connection().cursor() as cur:
        execute_batch(cur, insert("tables", ("namespace", "repository", "image_hash", "table_name", "object_id")),
                      table_meta, page_size=100)


def register_object_locations(object_locations):
    """
    Registers external locations (e.g. HTTP or S3) for Splitgraph objects.
    Objects must already be registered in the object tree.

    :param object_locations: List of (object_id, location, protocol).
    :return:
    """
    with get_connection().cursor() as cur:
        # Don't insert redundant objects here either.
        cur.execute(select("object_locations", "object_id"))
        existing_locations = [c[0] for c in cur.fetchall()]
        object_locations = [o for o in object_locations if o[0] not in existing_locations]

        execute_batch(cur, insert("object_locations", ("object_id", "location", "protocol")),
                      object_locations, page_size=100)


def get_existing_objects():
    """
    Gets all objects currently in the Splitgraph tree.
    :return: Set of object IDs.
    """
    with get_connection().cursor() as cur:
        cur.execute(select("objects", "object_id"))
        return set(c[0] for c in cur.fetchall())


def get_downloaded_objects():
    """
    Gets a list of objects currently in the Splitgraph cache (i.e. not only existing externally.)
    :return: Set of object IDs.
    """
    # Minor normalization sadness here: this can return duplicate object IDs since
    # we might repeat them if different versions of the same table point to the same object ID.
    with get_connection().cursor() as cur:
        cur.execute(SQL("""SELECT information_schema.tables.table_name FROM information_schema.tables JOIN {}.tables
                        ON information_schema.tables.table_name = {}.tables.object_id
                        WHERE information_schema.tables.table_schema = %s""")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA),
                            Identifier(
                                SPLITGRAPH_META_SCHEMA)),
                    (SPLITGRAPH_META_SCHEMA,))
        return set(c[0] for c in cur.fetchall())


def get_external_object_locations(objects):
    """
    Gets external locations for objects.
    :param objects: List of objects stored externally.
    :return: List of (object_id, location, protocol).
    """
    with get_connection().cursor() as cur:
        query = select("object_locations", "object_id, location, protocol",
                       "object_id IN (" + ','.join('%s' for _ in objects) + ")")
        cur.execute(query, objects)
        object_locations = cur.fetchall()
    return object_locations


def get_object_meta(objects):
    """
    Get metadata for multiple Splitgraph objects from the tree
    :param objects: List of objects to get metadata for.
    :return: List of (object_id, format, parent_id, namespace).
    """
    with get_connection().cursor() as cur:
        cur.execute(select("objects", "object_id, format, parent_id, namespace",
                           "object_id IN (" + ','.join('%s' for _ in objects) + ")"), objects)
        return cur.fetchall()


def register_table(repository, table, image, object_id):
    """
    Registers the object that represents a Splitgraph table inside of an image.
    :param repository: Repository
    :param table: Table name
    :param image: Image hash
    :param object_id: Object ID to register the table to.
    :return:
    """
    with get_connection().cursor() as cur:
        cur.execute(insert("tables", ("namespace", "repository", "image_hash", "table_name", "object_id")),
                    (repository.namespace, repository.repository, image, table, object_id))


def get_object_for_table(repository, table_name, image, object_format):
    """
    Returns the object ID of a table in a given image, with a given format. Used in cases
    where it's easier to materialize a table by downloading the whole snapshot but the table can be
    stored as both a DIFF and a SNAP.

    :param repository: Repository the table belongs to
    :param table_name: Name of the table
    :param image: Image the table belongs to
    :param object_format: Format of the object (DIFF or SNAP).
    :return: None if there's no such object, otherwise the object ID.
    """
    with get_connection().cursor() as cur:
        cur.execute(SQL("""SELECT {0}.tables.object_id FROM {0}.tables JOIN {0}.objects
                            ON {0}.objects.object_id = {0}.tables.object_id
                            WHERE {0}.tables.namespace = %s AND repository = %s AND image_hash = %s
                            AND table_name = %s AND format = %s""")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA)), (repository.namespace, repository.repository,
                                                                  image, table_name, object_format))
        result = cur.fetchone()
        return None if result is None else result[0]
