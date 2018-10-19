from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SplitGraphException, SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler.common import ensure_metadata_schema, select, insert
from splitgraph.meta_handler.images import get_canonical_image_id
from splitgraph.meta_handler.misc import mountpoint_exists


def get_current_head(conn, mountpoint, raise_on_none=True):
    return get_tagged_id(conn, mountpoint, 'HEAD', raise_on_none)


def get_tagged_id(conn, mountpoint, tag, raise_on_none=True):
    ensure_metadata_schema(conn)
    if not mountpoint_exists(conn, mountpoint) and raise_on_none:
        raise SplitGraphException("%s is not mounted." % mountpoint)

    if tag == 'latest':
        # Special case, return the latest commit from the mountpoint.
        with conn.cursor() as cur:
            cur.execute(select("images", "image_hash", "mountpoint = %s")
                        + SQL(" ORDER BY created DESC LIMIT 1"), (mountpoint,))
            result = cur.fetchone()
            if result is None:
                raise SplitGraphException("No commits found in %s!")
            return result[0]

    with conn.cursor() as cur:
        cur.execute(select("snap_tags", "image_hash", "mountpoint = %s AND tag = %s"), (mountpoint, tag))
        result = cur.fetchone()
        if result is None or result == (None,):
            if raise_on_none:
                if tag == 'HEAD':
                    raise SplitGraphException("No current checked out revision found for %s. Check one out with \"sgr "
                                              "checkout MOUNTPOINT image_hash\"." % mountpoint)
                else:
                    raise SplitGraphException("Tag %s not found in mountpoint %s" % (tag, mountpoint))
            else:
                return None
        return result[0]


def get_all_hashes_tags(conn, mountpoint):
    with conn.cursor() as cur:
        cur.execute(select("snap_tags", "image_hash, tag", "mountpoint = %s"), (mountpoint,))
        return cur.fetchall()


def set_tags(conn, mountpoint, tags, force=False):
    for tag, image_id in tags.items():
        if tag != 'HEAD':
            set_tag(conn, mountpoint, image_id, tag, force)


def set_head(conn, mountpoint, image):
    set_tag(conn, mountpoint, image, 'HEAD', force=True)


def set_tag(conn, mountpoint, image, tag, force=False):
    with conn.cursor() as cur:
        cur.execute(select("snap_tags", "1", "mountpoint = %s AND tag = %s"), (mountpoint, tag))
        if cur.fetchone() is None:
            cur.execute(insert("snap_tags", ("image_hash", "mountpoint", "tag")),
                        (image, mountpoint, tag))
        else:
            if force:
                cur.execute(SQL("UPDATE {}.snap_tags SET image_hash = %s WHERE mountpoint = %s AND tag = %s").format(
                    Identifier(SPLITGRAPH_META_SCHEMA)),
                    (image, mountpoint, tag))
            else:
                raise SplitGraphException("Tag %s already exists in mountpoint %s!" % (tag, mountpoint))


def tag_or_hash_to_actual_hash(conn, mountpoint, tag_or_hash):
    """Converts a tag or shortened hash to a full image hash that exists in the mountpoint."""
    try:
        return get_canonical_image_id(conn, mountpoint, tag_or_hash)
    except SplitGraphException:
        try:
            return get_tagged_id(conn, mountpoint, tag_or_hash)
        except SplitGraphException:
            raise SplitGraphException("%s does not refer to either an image commit hash or a tag!" % tag_or_hash)
