import logging
from random import getrandbits

from psycopg2.sql import SQL, Identifier

from splitgraph.meta_handler.common import ensure_metadata_schema
from splitgraph.meta_handler.images import add_new_image
from splitgraph.meta_handler.misc import table_schema_changed
from splitgraph.meta_handler.objects import register_table
from splitgraph.meta_handler.tables import get_all_tables, get_table
from splitgraph.meta_handler.tags import get_current_head, set_head
from splitgraph.objects.creation import record_table_as_diff, record_table_as_snap
from splitgraph.pg_audit import manage_audit_triggers, discard_pending_changes


def commit(conn, mountpoint, image_hash=None, include_snap=False, comment=None):
    """
    Commits all pending changes to a given mountpoint, creating a new image.
    :param conn: psycopg connection object.
    :param mountpoint: Mountpoint to commit.
    :param image_hash: Hash of the commit. Chosen by random if unspecified.
    :param include_snap: If True, also creates a SNAP object with a full copy of the table. This will speed up
        checkouts, but consumes extra space.
    :param comment: Optional comment to add to the commit.
    :return: The image hash the current state of the mountpoint was committed under.
    """
    ensure_metadata_schema(conn)
    conn.commit()
    manage_audit_triggers(conn)

    logging.info("Committing %s...", mountpoint)

    head = get_current_head(conn, mountpoint)

    if image_hash is None:
        image_hash = "%0.2x" % getrandbits(256)

    # Add the new snap ID to the tree
    add_new_image(conn, mountpoint, head, image_hash, comment=comment)

    commit_pending_changes(conn, mountpoint, head, image_hash, include_snap=include_snap)

    set_head(conn, mountpoint, image_hash)
    conn.commit()
    manage_audit_triggers(conn)
    return image_hash


def commit_pending_changes(conn, mountpoint, current_head, image_hash, include_snap=False):
    """
    Reads the recorded pending changes to all tables in a given mountpoint, conflates them and possibly stores them
    as new object(s) as follows:
        * If a table has been created or there has been a schema change, it's only stored as a SNAP (full snapshot).
        * If a table hasn't changed since the last revision, no new objects are created and it's linked to the previous
          objects belonging to the last revision.
        * Otherwise, the table is stored as a conflated (1 change per PK) DIFF object and an optional SNAP.
    :param conn: psycopg connection object.
    :param mountpoint: Mountpoint to commit.
    :param current_head: Current HEAD pointer to base the commit on.
    :param image_hash: Hash of the image to commit changes under.
    :param include_snap: If True, also stores the table as a SNAP.
    """
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT DISTINCT(table_name) FROM {}.{}
                       WHERE schema_name = %s""").format(Identifier("audit"),
                                                         Identifier("logged_actions")), (mountpoint,))
        changed_tables = [c[0] for c in cur.fetchall()]
    for table in get_all_tables(conn, mountpoint):
        table_info = get_table(conn, mountpoint, table, current_head)
        # Table already exists at the current HEAD
        if table_info:
            # If there has been a schema change, we currently just snapshot the whole table.
            # This is obviously wasteful (say if just one column has been added/dropped or we added a PK,
            # but it's a starting point to support schema changes.
            if table_schema_changed(conn, mountpoint, table, image_1=current_head, image_2=None):
                record_table_as_snap(conn, mountpoint, image_hash, table, table_info)
                continue

            if table in changed_tables:
                record_table_as_diff(conn, mountpoint, image_hash, table, table_info)
            else:
                # If the table wasn't changed, point the commit to the old table objects (including
                # any of snaps or diffs).
                # This feels slightly weird: are we denormalized here?
                for prev_object_id, _ in table_info:
                    register_table(conn, mountpoint, table, image_hash, prev_object_id)

        # If table created (or we want to store a snap anyway), copy the whole table over as well.
        if not table_info or include_snap:
            record_table_as_snap(conn, mountpoint, image_hash, table, table_info)

    # Make sure that all pending changes have been discarded by this point (e.g. if we created just a snapshot for
    # some tables and didn't consume the WAL).
    # NB if we allow partial commits, this will have to be changed (only discard for committed tables).
    discard_pending_changes(conn, mountpoint)
