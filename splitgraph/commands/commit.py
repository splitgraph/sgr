from random import getrandbits

from splitgraph.constants import _log
from splitgraph.meta_handler import ensure_metadata_schema, get_current_head, add_new_snap_id, set_head
from splitgraph.pg_replication import record_pending_changes, commit_pending_changes, stop_replication, \
    start_replication


def commit(conn, mountpoint, schema_snap=None, include_snap=False):
    ensure_metadata_schema(conn)
    # required here so that the logical replication sees changes made before the commit in this tx
    conn.commit()
    record_pending_changes(conn)
    _log("Committing...")

    HEAD = get_current_head(conn, mountpoint)

    if schema_snap is None:
        schema_snap = "%0.2x" % getrandbits(256)

    # Add the new snap ID to the tree
    add_new_snap_id(conn, mountpoint, HEAD, schema_snap)

    commit_pending_changes(conn, mountpoint, HEAD, schema_snap, include_snap=include_snap)

    stop_replication(conn)
    set_head(conn, mountpoint, schema_snap)
    conn.commit()  # need to commit before starting replication
    start_replication(conn)
    _log("Committed as %s" % schema_snap[:12])
    return schema_snap