import logging
from random import getrandbits

from splitgraph.meta_handler import ensure_metadata_schema, get_current_head, add_new_snap_id, set_head
from splitgraph.pg_audit import manage_audit_triggers
from splitgraph.pg_replication import commit_pending_changes


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
    add_new_snap_id(conn, mountpoint, head, image_hash, comment=comment)

    commit_pending_changes(conn, mountpoint, head, image_hash, include_snap=include_snap)

    set_head(conn, mountpoint, image_hash)
    conn.commit()
    manage_audit_triggers(conn)
    return image_hash
