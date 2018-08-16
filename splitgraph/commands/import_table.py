from random import getrandbits

from psycopg2.sql import Identifier, SQL

from splitgraph.commands.checkout import materialize_table
from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler import get_current_head, add_new_snap_id, register_table, set_head, get_table


def import_table(conn, mountpoint, table, target_mountpoint, target_table, image_hash=None):
    # Creates a new commit in target_mountpoint with an extra table linked to an already-existing table.
    # After this operation, the HEAD of the target mountpoint moves to the new commit and the new table
    # is materialized.
    HEAD = get_current_head(conn, target_mountpoint)
    image_hash = image_hash or get_current_head(conn, mountpoint)

    target_hash = "%0.2x" % getrandbits(256)

    # Add the new snap ID to the tree
    add_new_snap_id(conn, target_mountpoint, HEAD, target_hash, comment="Importing %s from %s" % (table, mountpoint))

    # Materialize the actual table in the target mountpoint
    materialize_table(conn, mountpoint, image_hash, table, target_table, destination_mountpoint=target_mountpoint)

    # Register the new table as well as all existing tables.
    for object_id, _ in get_table(conn, mountpoint, table, image_hash):
        register_table(conn, target_mountpoint, target_table, target_hash, object_id)

    with conn.cursor() as cur:
        cur.execute(SQL("""INSERT INTO {0}.tables (mountpoint, snap_id, table_name, object_id)
            (SELECT %s, %s, table_name, object_id FROM {0}.tables
            WHERE mountpoint = %s AND snap_id = %s)""").format(Identifier(SPLITGRAPH_META_SCHEMA)),
                    (target_mountpoint, target_hash, target_mountpoint, HEAD))

    set_head(conn, target_mountpoint, target_hash)
