from datetime import datetime

from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler.common import select, insert, ensure_metadata_schema
from splitgraph.meta_handler.images import get_closest_parent_image_object
from splitgraph.meta_handler.objects import register_object, register_table
from splitgraph.pg_utils import get_full_table_schema


def get_all_foreign_tables(conn, mountpoint):
    """Inspects the information_schema to see which foreign tables we have in a given mountpoint.
    Used by `mount` to populate the metadata since if we did IMPORT FOREIGN SCHEMA we've no idea what tables we actually
    fetched from the remote postgres."""
    with conn.cursor() as cur:
        cur.execute(
            select("tables", "table_name", "table_schema = %s and table_type = 'FOREIGN TABLE'", "information_schema"),
            (mountpoint,))
        return [c[0] for c in cur.fetchall()]


def mountpoint_exists(conn, mountpoint):
    with conn.cursor() as cur:
        cur.execute(SQL("SELECT 1 FROM {}.images WHERE mountpoint = %s").format(Identifier(SPLITGRAPH_META_SCHEMA)),
                    (mountpoint,))
        return cur.fetchone() is not None


def register_mountpoint(conn, mountpoint, initial_image, tables, table_object_ids):
    with conn.cursor() as cur:
        cur.execute(insert("images", ("image_hash", "mountpoint", "parent_id", "created")),
                    (initial_image, mountpoint, None, datetime.now()))
        # Strictly speaking this is redundant since the checkout (of the "HEAD" commit) updates the tag table.
        cur.execute(insert("snap_tags", ("mountpoint", "image_hash", "tag")),
                    (mountpoint, initial_image, "HEAD"))
        for t, ti in zip(tables, table_object_ids):
            # Register the tables and the object IDs they were stored under.
            # They're obviously stored as snaps since there's nothing to diff to...
            register_object(conn, ti, 'SNAP', None)
            register_table(conn, mountpoint, t, initial_image, ti)


def unregister_mountpoint(conn, mountpoint):
    with conn.cursor() as cur:
        for meta_table in ["tables", "snap_tags", "images", "remotes"]:
            cur.execute(SQL("DELETE FROM {}.{} WHERE mountpoint = %s").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                              Identifier(meta_table)),
                        (mountpoint,))


def get_current_mountpoints_hashes(conn):
    ensure_metadata_schema(conn)
    with conn.cursor() as cur:
        cur.execute(select("snap_tags", "mountpoint, image_hash", "tag = 'HEAD'"))
        return cur.fetchall()


def get_remote_for(conn, mountpoint, remote_name='origin'):
    with conn.cursor() as cur:
        cur.execute(select("remotes", "remote_conn_string, remote_mountpoint", "mountpoint = %s AND remote_name = %s"),
                    (mountpoint, remote_name))
        return cur.fetchone()


def add_remote(conn, mountpoint, remote_conn, remote_mountpoint, remote_name='origin'):
    with conn.cursor() as cur:
        cur.execute(insert("remotes", ("mountpoint", "remote_name", "remote_conn_string", "remote_mountpoint")),
                    (mountpoint, remote_name, remote_conn, remote_mountpoint))


def table_schema_changed(conn, mountpoint, table_name, image_1, image_2=None):
    snap_1 = get_closest_parent_image_object(conn, mountpoint, table_name, image_1)[0]
    # image_2 = None here means the current staging area.
    if image_2 is not None:
        snap_2 = get_closest_parent_image_object(conn, mountpoint, table_name, image_2)[0]
        return get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_1) != \
               get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_2)
    return get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_1) != \
           get_full_table_schema(conn, mountpoint, table_name)


def get_schema_at(conn, mountpoint, table_name, image_hash):
    snap_1 = get_closest_parent_image_object(conn, mountpoint, table_name, image_hash)[0]
    return get_full_table_schema(conn, SPLITGRAPH_META_SCHEMA, snap_1)
