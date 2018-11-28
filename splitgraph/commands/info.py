from psycopg2.extras import NamedTupleCursor
from psycopg2.sql import SQL, Identifier

from splitgraph.connection import get_connection
from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.exceptions import SplitGraphException
from splitgraph.meta_handler.common import select
from splitgraph.meta_handler.images import IMAGE_COLS


def get_image(repository, image):
    with get_connection().cursor(cursor_factory=NamedTupleCursor) as cur:
        cur.execute(select("images", IMAGE_COLS,
                           "repository = %s AND image_hash = %s AND namespace = %s"), (repository.repository,
                                                                                       image, repository.namespace))
        return cur.fetchone()


def get_canonical_image_id(repository, short_image):
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
            WHERE namespace = %s AND repository = %s AND parent_id = %s""").format(
            Identifier(SPLITGRAPH_META_SCHEMA)),
            (repository.namespace, repository.repository, image_hash))
        children = [c[0] for c in cur.fetchall()]
    return parent, children


def get_tables_at(repository, image):
    with get_connection().cursor() as cur:
        cur.execute(select('tables', 'table_name', 'namespace = %s AND repository = %s AND image_hash = %s'),
                    (repository.namespace, repository.repository, image))
        return [t[0] for t in cur.fetchall()]


def get_table(repository, table_name, image):
    # Returns a list of available[(object_id, object_format)] from the table meta
    with get_connection().cursor() as cur:
        cur.execute(SQL("""SELECT {0}.tables.object_id, format FROM {0}.tables JOIN {0}.objects
                            ON {0}.objects.object_id = {0}.tables.object_id
                            WHERE {0}.tables.namespace = %s AND repository = %s AND image_hash = %s
                            AND table_name = %s""").format(
            Identifier(SPLITGRAPH_META_SCHEMA)), (repository.namespace, repository.repository, image, table_name))
        return cur.fetchall()
