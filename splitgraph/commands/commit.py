import logging
from random import getrandbits

from psycopg2.sql import SQL, Identifier

from splitgraph._data.common import ensure_metadata_schema
from splitgraph._data.images import add_new_image
from splitgraph._data.misc import table_schema_changed
from splitgraph._data.objects import register_table
from splitgraph.commands._objects.creation import record_table_as_diff, record_table_as_snap
from splitgraph.commands.info import get_table
from splitgraph.commands.tagging import get_current_head
from splitgraph.connection import get_connection
from splitgraph.pg_utils import get_all_tables
from ._common import set_head
from ._pg_audit import manage_audit_triggers, discard_pending_changes


def commit(repository, image_hash=None, include_snap=False, comment=None):
    """
    Commits all pending changes to a given repository, creating a new image.

    :param repository: Repository to commit.
    :param image_hash: Hash of the commit. Chosen by random if unspecified.
    :param include_snap: If True, also creates a SNAP object with a full copy of the table. This will speed up
        checkouts, but consumes extra space.
    :param comment: Optional comment to add to the commit.
    :return: The image hash the current state of the mountpoint was committed under.
    """
    target_schema = repository.to_schema()

    conn = get_connection()
    ensure_metadata_schema()
    conn.commit()
    manage_audit_triggers()

    logging.info("Committing %s...", target_schema)

    head = get_current_head(repository)

    if image_hash is None:
        image_hash = "%0.2x" % getrandbits(256)

    # Add the new snap ID to the tree
    add_new_image(repository, head, image_hash, comment=comment)

    commit_pending_changes(repository, head, image_hash, include_snap=include_snap)

    set_head(repository, image_hash)
    conn.commit()
    manage_audit_triggers()
    return image_hash


def commit_pending_changes(repository, current_head, image_hash, include_snap=False):
    """
    Reads the recorded pending changes to all tables in a given mountpoint, conflates them and possibly stores them
    as new object(s) as follows:

        * If a table has been created or there has been a schema change, it's only stored as a SNAP (full snapshot).
        * If a table hasn't changed since the last revision, no new objects are created and it's linked to the previous
          objects belonging to the last revision.
        * Otherwise, the table is stored as a conflated (1 change per PK) DIFF object and an optional SNAP.

    :param repository: Repository to commit.
    :param current_head: Current HEAD pointer to base the commit on.
    :param image_hash: Hash of the image to commit changes under.
    :param include_snap: If True, also stores the table as a SNAP.
    """

    conn = get_connection()
    target_schema = repository.to_schema()
    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT DISTINCT(table_name) FROM {}.{}
                       WHERE schema_name = %s""").format(Identifier("audit"),
                                                         Identifier("logged_actions")), (target_schema,))
        changed_tables = [c[0] for c in cur.fetchall()]
    for table in get_all_tables(conn, target_schema):
        table_info = get_table(repository, table, current_head)
        # Table already exists at the current HEAD
        if table_info:
            # If there has been a schema change, we currently just snapshot the whole table.
            # This is obviously wasteful (say if just one column has been added/dropped or we added a PK,
            # but it's a starting point to support schema changes.
            if table_schema_changed(repository, table, image_1=current_head, image_2=None):
                record_table_as_snap(repository, image_hash, table, table_info)
                continue

            if table in changed_tables:
                record_table_as_diff(repository, image_hash, table, table_info)
            else:
                # If the table wasn't changed, point the commit to the old table objects (including
                # any of snaps or diffs).
                # This feels slightly weird: are we denormalized here?
                for prev_object_id, _ in table_info:
                    register_table(repository, table, image_hash, prev_object_id)

        # If table created (or we want to store a snap anyway), copy the whole table over as well.
        if not table_info or include_snap:
            record_table_as_snap(repository, image_hash, table, table_info)

    # Make sure that all pending changes have been discarded by this point (e.g. if we created just a snapshot for
    # some tables and didn't consume the WAL).
    # NB if we allow partial commits, this will have to be changed (only discard for committed tables).
    discard_pending_changes(target_schema)
