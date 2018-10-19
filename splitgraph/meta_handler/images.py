from datetime import datetime

from psycopg2.extras import Json
from psycopg2.sql import SQL

from splitgraph.constants import SplitGraphException
from splitgraph.meta_handler.common import select, insert
from splitgraph.meta_handler.objects import get_object_format, get_object_parents
from splitgraph.meta_handler.tables import get_table_with_format


def get_image_parent(conn, mountpoint, image):
    with conn.cursor() as cur:
        cur.execute(select("images", "parent_id", "mountpoint = %s AND image_hash = %s"), (mountpoint, image))
        return cur.fetchone()[0]


def get_image_parent_provenance(conn, mountpoint, image):
    with conn.cursor() as cur:
        cur.execute(select("images", "parent_id, provenance_type, provenance_data",
                           "mountpoint = %s AND image_hash = %s"), (mountpoint, image))
        return cur.fetchone()


def get_all_image_info(conn, mountpoint, image):
    with conn.cursor() as cur:
        cur.execute(select("images", "parent_id, created, comment", "mountpoint = %s AND image_hash = %s"),
                    (mountpoint, image))
        return cur.fetchone()


def get_all_images_parents(conn, mountpoint):
    with conn.cursor() as cur:
        cur.execute(select("images", "image_hash, parent_id, created, comment, provenance_type, provenance_data",
                           "mountpoint = %s") +
                    SQL(" ORDER BY created"),
                    (mountpoint,))
        return cur.fetchall()


def get_canonical_image_id(conn, mountpoint, short_image):
    with conn.cursor() as cur:
        cur.execute(select("images", "image_hash", "mountpoint = %s AND image_hash LIKE %s"),
                    (mountpoint, short_image.lower() + '%'))
        candidates = [c[0] for c in cur.fetchall()]

    if not candidates:
        raise SplitGraphException("No snapshots beginning with %s found for mountpoint %s!" % (short_image, mountpoint))

    if len(candidates) > 1:
        result = "Multiple suitable candidates found: \n * " + "\n * ".join(candidates)
        raise SplitGraphException(result)

    return candidates[0]


def get_closest_parent_image_object(conn, mountpoint, table, image):
    path = []
    object_id = get_table_with_format(conn, mountpoint, table, image, object_format='SNAP')
    if object_id is not None:
        return object_id, path

    object_id = get_table_with_format(conn, mountpoint, table, image, object_format='DIFF')
    while object_id is not None:
        path.append(object_id)
        parents = get_object_parents(conn, object_id)
        for object_id in parents:
            if get_object_format(conn, object_id) == 'SNAP':
                return object_id, path
            break  # Found 1 diff, will be added to the path at the next iteration.

    # We didn't find an actual snapshot for this table -- either it doesn't exist in this
    # version or something went wrong. Should we raise here?


def add_new_image(conn, mountpoint, parent_id, image, created=None, comment=None, provenance_type=None,
                  provenance_data=None):
    with conn.cursor() as cur:
        cur.execute(insert("images", ("image_hash", "mountpoint", "parent_id", "created", "comment",
                                         "provenance_type", "provenance_data")),
                    (image, mountpoint, parent_id, created or datetime.now(), comment,
                     provenance_type, Json(provenance_data)))
