"""
Functions to manage the Postgres audit stored procedures on the driver to detect changes to tables.
"""

from psycopg2.extras import execute_batch
from psycopg2.sql import SQL, Identifier

from splitgraph.commands._objects.utils import KIND, get_replica_identity, convert_audit_change
from splitgraph.commands.info import get_table
from splitgraph.commands.repository import get_current_repositories
from splitgraph.connection import get_connection
from splitgraph.pg_utils import get_all_tables
from .._data.common import ensure_metadata_schema

ROW_TRIGGER_NAME = "audit_trigger_row"
STM_TRIGGER_NAME = "audit_trigger_stm"


def manage_audit_triggers():
    """Does bookkeeping on audit triggers / audit table:

        * Detect tables that are being audited that don't need to be any more
          (e.g. they've been unmounted)
        * Drop audit triggers for those and delete all audit info for them
        * Set up audit triggers for new tables
    """
    conn = get_connection()
    repos_tables = [(r.to_schema(), t) for r, head in get_current_repositories()
                    for t in get_all_tables(conn, r.to_schema()) if get_table(r, t, head)]

    with conn.cursor() as cur:
        cur.execute("SELECT event_object_schema, event_object_table "
                    "FROM information_schema.triggers WHERE trigger_name IN (%s, %s)",
                    (ROW_TRIGGER_NAME, STM_TRIGGER_NAME))
        existing_triggers = cur.fetchall()

    triggers_to_remove = [t for t in existing_triggers if t not in repos_tables]
    triggers_to_add = [t for t in repos_tables if t not in existing_triggers]

    with conn.cursor() as cur:
        if triggers_to_remove:
            # Delete the triggers for untracked tables
            for trigger in (ROW_TRIGGER_NAME, STM_TRIGGER_NAME):
                cur.execute(SQL(";").join(SQL("DROP TRIGGER IF EXISTS {} ON {}.{}").format(
                    Identifier(trigger), Identifier(s), Identifier(t))
                                          for s, t in triggers_to_remove))
            # Delete the actual logged actions for untracked tables
            execute_batch(cur, "DELETE FROM audit.logged_actions WHERE schema_name = %s AND table_name = %s",
                          triggers_to_remove)
        if triggers_to_add:
            # Create triggers for untracked tables
            # Call to the procedure: target_table, audit_rows (default True), audit_query_text (default False)
            execute_batch(cur, "SELECT audit.audit_table(%s)", [(s + '.' + t,) for s, t in triggers_to_add])
    conn.commit()


def discard_pending_changes(schema):
    """
    Discards all recorded pending (uncommitted) changes to a Postgres schema from the audit table.
    Doesn't discard the actual changes.
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(SQL("DELETE FROM {}.{} WHERE schema_name = %s").format(Identifier("audit"),
                                                                           Identifier("logged_actions")),
                    (schema,))
    conn.commit()


def has_pending_audit_changes(repository):
    """
    Checks if a checked-out Splitgraph repository has any changes to its tracked tables. Doesn't detect
    table creations, deletions or schema changes.
    :param repository: Repository object
    :return:
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(SQL("SELECT 1 FROM {}.{} WHERE schema_name = %s").format(Identifier("audit"),
                                                                             Identifier("logged_actions")),
                    (repository.to_schema(),))
        return cur.fetchone() is not None


def manage_audit(func):
    """A decorator to be put around various Splitgraph commands that performs general admin and auditing management
    (makes sure the metadata schema exists and delete/add required audit triggers)
    """

    def wrapped(*args, **kwargs):
        try:
            ensure_metadata_schema()
            manage_audit_triggers()
            func(*args, **kwargs)
        finally:
            get_connection().commit()
            manage_audit_triggers()

    return wrapped


def dump_pending_changes(schema, table, aggregate=False):
    """
    Dumps pending changes from the audit table
    :param schema: Schema to dump changes for
    :param table: Table to dump changes for
    :param aggregate: If True, returns a tuple (added_rows, removed_rows, updated_rows).
        If False, returns a list of changes (primary_key, change_type, change_data).
    """
    with get_connection().cursor() as cur:
        if aggregate:
            cur.execute(SQL(
                "SELECT action, count(action) FROM {}.{} "
                "WHERE schema_name = %s AND table_name = %s GROUP BY action").format(Identifier("audit"),
                                                                                     Identifier("logged_actions")),
                        (schema, table))
            return [(KIND[k], c) for k, c in cur.fetchall()]

        cur.execute(SQL(
            "SELECT action, row_data, changed_fields FROM {}.{} "
            "WHERE schema_name = %s AND table_name = %s").format(Identifier("audit"),
                                                                 Identifier("logged_actions")),
                    (schema, table))
        repl_id = get_replica_identity(schema, table)
        ri_cols, _ = zip(*repl_id)
        result = []
        for action, row_data, changed_fields in cur:
            result.extend(convert_audit_change(action, row_data, changed_fields, ri_cols))
        return result
