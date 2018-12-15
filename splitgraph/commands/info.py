"""
Public API functions to get information about Splitgraph images and repositories.
"""

from psycopg2.extras import NamedTupleCursor
from psycopg2.sql import SQL, Identifier

from splitgraph._data.common import select
from splitgraph._data.images import IMAGE_COLS, get_image_object_path
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.connection import get_connection
from splitgraph.engine import get_engine
from splitgraph.exceptions import SplitGraphException


def get_image(repository, image):
    """
    Gets information about a single image.

    :param repository: Repository the image belongs to
    :param image: Image hash
    :return: A named tuple of (image_hash, parent_id, created, comment, provenance_type, provenance_data)
    """
    with get_connection().cursor(cursor_factory=NamedTupleCursor) as cur:
        cur.execute(select("images", IMAGE_COLS,
                           "repository = %s AND image_hash = %s AND namespace = %s"), (repository.repository,
                                                                                       image, repository.namespace))
        return cur.fetchone()


def get_canonical_image_id(repository, short_image):
    """
    Converts a truncated image hash into a full hash. Raises if there's an ambiguity.

    :param repository: Repository the image belongs to
    :param short_image: Shortened image hash
    """
    with get_connection().cursor() as cur:
        cur.execute(select("images", "image_hash", "namespace = %s AND repository = %s AND image_hash LIKE %s"),
                    (repository.namespace, repository.repository, short_image.lower() + '%'))
        candidates = [c[0] for c in cur.fetchall()]

    if not candidates:
        raise SplitGraphException("No snapshots beginning with %s found for mountpoint %s!" % (short_image,
                                                                                               repository.to_schema()))

    if len(candidates) > 1:
        result = "Multiple suitable candidates found: \n * " + "\n * ".join(candidates)
        raise SplitGraphException(result)

    return candidates[0]


def get_parent_children(repository, image_hash):
    """Gets the parent and a list of children of a given image."""
    parent = get_image(repository, image_hash).parent_id

    with get_connection().cursor() as cur:
        cur.execute(SQL("""SELECT image_hash FROM {}.images
            WHERE namespace = %s AND repository = %s AND parent_id = %s""")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                    (repository.namespace, repository.repository, image_hash))
        children = [c[0] for c in cur.fetchall()]
    return parent, children


def get_tables_at(repository, image):
    """
    Gets the names of all tables inside of an image.

    :param repository: Repository the image belongs to
    :param image: Image hash
    """
    with get_connection().cursor() as cur:
        cur.execute(select('tables', 'table_name', 'namespace = %s AND repository = %s AND image_hash = %s'),
                    (repository.namespace, repository.repository, image))
        return [t[0] for t in cur.fetchall()]


def get_table(repository, table_name, image):
    """
    Returns a list of objects linked to a given table. Can contain a DIFF object (beginning a chain of DIFFs that
    describe a table), a SNAP object (a full table copy), or both.

    :param repository: Repository the image belongs to
    :param table_name: Name of the table
    :param image: Image hash
    :return: List of (object_id, format)
    """
    with get_connection().cursor() as cur:
        cur.execute(SQL("""SELECT {0}.tables.object_id, format FROM {0}.tables JOIN {0}.objects
                            ON {0}.objects.object_id = {0}.tables.object_id
                            WHERE {0}.tables.namespace = %s AND repository = %s AND image_hash = %s
                            AND table_name = %s""")
                    .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                    (repository.namespace, repository.repository, image, table_name))
        return cur.fetchall()


def table_schema_changed(repository, table_name, image_1, image_2=None):
    """
    Checks if the table schema has changed between two images in a repository

    :param repository: Repository object
    :param table_name: Table name
    :param image_1: Hash of the first image
    :param image_2: Hash of the second image. If not specified, uses the current staging area.
    """
    snap_1 = get_image_object_path(repository, table_name, image_1)[0]
    conn = get_connection()
    # image_2 = None here means the current staging area.
    if image_2 is not None:
        snap_2 = get_image_object_path(repository, table_name, image_2)[0]
        return get_engine().get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_1) != \
               get_engine().get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_2)
    return get_engine().get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_1) != \
           get_engine().get_full_table_schema(repository.to_schema(), table_name)


def get_schema_at(repository, table_name, image_hash):
    """
    Gets the schema of a given table in a given Splitgraph image.

    :param repository: Repository object
    :param table_name: Table name
    :param image_hash: Hash of the image
    :return: The table schema. See the documentation for `get_full_table_schema` for the spec.
    """
    snap_1 = get_image_object_path(repository, table_name, image_hash)[0]
    return get_engine().get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_1)
