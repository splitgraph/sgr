from datetime import datetime

from psycopg2.extras import Json, NamedTupleCursor
from psycopg2.sql import SQL

from splitgraph.constants import SplitGraphException
from splitgraph.meta_handler.common import select, insert
from splitgraph.meta_handler.objects import get_object_format, get_object_parents
from splitgraph.meta_handler.tables import get_object_for_table

IMAGE_COLS = "image_hash, parent_id, created, comment, provenance_type, provenance_data"


def get_image(conn, repository, image):
    with conn.cursor(cursor_factory=NamedTupleCursor) as cur:
        cur.execute(select("images", IMAGE_COLS,
                           "repository = %s AND image_hash = %s AND namespace = %s"), (repository.repository,
                                                                                       image, repository.namespace))
        return cur.fetchone()


def get_all_images_parents(conn, repository):
    with conn.cursor() as cur:
        cur.execute(select("images", IMAGE_COLS, "repository = %s AND namespace = %s") +
                    SQL(" ORDER BY created"), (repository.repository, repository.namespace))
        return cur.fetchall()


def get_canonical_image_id(conn, repository, short_image):
    with conn.cursor() as cur:
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


def get_closest_parent_image_object(conn, repository, table, image):
    path = []
    object_id = get_object_for_table(conn, repository, table, image, object_format='SNAP')
    if object_id is not None:
        return object_id, path

    object_id = get_object_for_table(conn, repository, table, image, object_format='DIFF')
    while object_id is not None:
        path.append(object_id)
        parents = get_object_parents(conn, object_id)
        for object_id in parents:
            if get_object_format(conn, object_id) == 'SNAP':
                return object_id, path
            break  # Found 1 diff, will be added to the path at the next iteration.

    # We didn't find an actual snapshot for this table -- something's wrong with the object tree.
    raise SplitGraphException("Couldn't find a SNAP object for %s (malformed object tree)" % table)


def add_new_image(conn, repository, parent_id, image, created=None, comment=None, provenance_type=None,
                  provenance_data=None):
    with conn.cursor() as cur:
        cur.execute(insert("images", ("image_hash", "namespace", "repository", "parent_id", "created", "comment",
                                         "provenance_type", "provenance_data")),
                    (image, repository.namespace, repository.repository, parent_id, created or datetime.now(), comment,
                     provenance_type, Json(provenance_data)))
