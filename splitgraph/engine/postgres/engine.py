import logging
from pkgutil import get_data

import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.sql import SQL, Identifier

from splitgraph import get_connection, CONFIG, PG_HOST, PG_PORT, PG_DB
from splitgraph.engine import Engine, ResultShape

_AUDIT_SCHEMA = 'audit'
_AUDIT_TRIGGER = 'resources/audit_trigger.sql'
_PACKAGE = 'splitgraph'
ROW_TRIGGER_NAME = "audit_trigger_row"
STM_TRIGGER_NAME = "audit_trigger_stm"


class PostgresEngine(Engine):
    """An implementation of the Postgres engine for Splitgraph"""

    def run_sql(self, statement, arguments=None, return_shape=ResultShape.MANY_MANY):
        with get_connection().cursor() as cur:
            cur.execute(statement, arguments)

            if return_shape == ResultShape.ONE_ONE:
                result = cur.fetchone()
                return result[0] if result else None
            if return_shape == ResultShape.ONE_MANY:
                return cur.fetchone()
            if return_shape == ResultShape.MANY_ONE:
                return [c[0] for c in cur.fetchall()]
            if return_shape == ResultShape.MANY_MANY:
                return cur.fetchall()

    def get_primary_keys(self, schema, table):
        """Inspects the Postgres information_schema to get the primary keys for a given table."""
        return self.run_sql(SQL("""SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                               FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid
                                                                      AND a.attnum = ANY(i.indkey)
                               WHERE i.indrelid = '{}.{}'::regclass AND i.indisprimary""")
                            .format(Identifier(schema), Identifier(table)), return_shape=ResultShape.MANY_MANY)

    def run_sql_batch(self, statement, arguments):
        with get_connection().cursor() as cur:
            execute_batch(cur, statement, arguments, page_size=1000)

    def initialize(self):
        """Create the Splitgraph Postgres database and install the audit trigger"""
        # Use the connection to the "postgres" database to create the actual PG_DB
        with psycopg2.connect(dbname=CONFIG['SG_DRIVER_POSTGRES_DB_NAME'],
                              user=CONFIG['SG_DRIVER_ADMIN_USER'],
                              password=CONFIG['SG_DRIVER_ADMIN_PWD'],
                              host=PG_HOST,
                              port=PG_PORT) as admin_conn:
            # CREATE DATABASE can't run inside of tx
            admin_conn.autocommit = True
            with admin_conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (PG_DB,))
                if cur.fetchone() is None:
                    logging.info("Creating database %s", PG_DB)
                    cur.execute(SQL("CREATE DATABASE {}").format(Identifier(PG_DB)))
                else:
                    logging.info("Database %s already exists, skipping", PG_DB)

        # Install the audit trigger if it doesn't exist
        if not self.schema_exists(_AUDIT_SCHEMA):
            logging.info("Installing the audit trigger...")
            audit_trigger = get_data(_PACKAGE, _AUDIT_TRIGGER)
            self.run_sql(audit_trigger.decode('utf-8'), return_shape=ResultShape.NONE)
        else:
            logging.info("Skipping the audit trigger as it's already installed")

    def get_tracked_tables(self):
        """Return a list of tables that the audit trigger is working on."""
        return self.run_sql("SELECT event_object_schema, event_object_table "
                            "FROM information_schema.triggers WHERE trigger_name IN (%s, %s)",
                            (ROW_TRIGGER_NAME, STM_TRIGGER_NAME))

    def track_tables(self, tables):
        """Install the audit trigger on the required tables"""
        # FIXME: escaping for schema + . + table
        self.run_sql_batch("SELECT audit.audit_table(%s)", [(s + '.' + t,) for s, t in tables])

    def untrack_tables(self, tables):
        """Remove triggers from tables and delete their pending changes"""
        for trigger in (ROW_TRIGGER_NAME, STM_TRIGGER_NAME):
            self.run_sql(SQL(";").join(SQL("DROP TRIGGER IF EXISTS {} ON {}.{}").format(
                Identifier(trigger), Identifier(s), Identifier(t))
                                       for s, t in tables),
                         return_shape=ResultShape.NONE)
        # Delete the actual logged actions for untracked tables
        self.run_sql_batch("DELETE FROM audit.logged_actions WHERE schema_name = %s AND table_name = %s", tables)

    def has_pending_changes(self, schema):
        """
        Return True if the tracked schema has pending changes and False if it doesn't.
        """
        return self.run_sql(SQL("SELECT 1 FROM {}.{} WHERE schema_name = %s").format(Identifier("audit"),
                                                                                     Identifier("logged_actions")),
                            (schema,), return_shape=ResultShape.ONE_ONE) is not None

    def discard_pending_changes(self, schema, table=None):
        """
        Discard recorded pending changes for a tracked schema / table
        """
        query = SQL("DELETE FROM {}.{} WHERE schema_name = %s").format(Identifier("audit"),
                                                                       Identifier("logged_actions"))

        if table:
            self.run_sql(query + SQL(" AND table_name = %s"), (schema, table), return_shape=ResultShape.NONE)
        else:
            self.run_sql(query, (schema,), return_shape=ResultShape.NONE)

    def get_pending_changes(self, schema, table, aggregate=False):
        """
        Return pending changes for a given tracked table
        :param schema: Schema the table belongs to
        :param table: Table to return changes for
        :param aggregate: Whether to aggregate changes or return them completely
        :return: If aggregate is True: tuple with numbers of `(added_rows, removed_rows, updated_rows)`.
            If aggregate is False: List of (primary_key, change_type, change_data)
        """
        if aggregate:
            return [(_KIND[k], c) for k, c in
                    self.run_sql(SQL(
                        "SELECT action, count(action) FROM {}.{} "
                        "WHERE schema_name = %s AND table_name = %s GROUP BY action").format(Identifier("audit"),
                                                                                             Identifier(
                                                                                                 "logged_actions")),
                                 (schema, table))]

        ri_cols, _ = zip(*self.get_change_key(schema, table))
        result = []
        for action, row_data, changed_fields in self.run_sql(SQL(
                "SELECT action, row_data, changed_fields FROM {}.{} "
                "WHERE schema_name = %s AND table_name = %s").format(Identifier("audit"),
                                                                     Identifier("logged_actions")),
                                                             (schema, table)):
            result.extend(_convert_audit_change(action, row_data, changed_fields, ri_cols))
        return result

    def get_changed_tables(self, schema):
        """Get list of tables that have changed content"""
        return self.run_sql(SQL("""SELECT DISTINCT(table_name) FROM {}.{}
                               WHERE schema_name = %s""").format(Identifier("audit"),
                                                                 Identifier("logged_actions")), (schema,),
                            return_shape=ResultShape.MANY_ONE)


def _split_ri_cols(action, row_data, changed_fields, ri_cols):
    """
    :return: `(ri_vals, non_ri_cols, non_ri_vals)`: a tuple of 3 lists:
        * `ri_vals`: values identifying the replica identity (RI) of a given tuple (matching column names in `ri_cols`)
        * `non_ri_cols`: column names not in the RI that have been changed/updated
        * `non_ri_vals`: column values not in the RI that have been changed/updated (matching colnames in `non_ri_cols`)
    """
    non_ri_cols = []
    non_ri_vals = []
    ri_vals = [None] * len(ri_cols)

    if action == 'I':
        for cc, cv in row_data.items():
            if cc in ri_cols:
                ri_vals[ri_cols.index(cc)] = cv
            else:
                non_ri_cols.append(cc)
                non_ri_vals.append(cv)
    elif action == 'D':
        for cc, cv in row_data.items():
            if cc in ri_cols:
                ri_vals[ri_cols.index(cc)] = cv
    elif action == 'U':
        for cc, cv in row_data.items():
            if cc in ri_cols:
                ri_vals[ri_cols.index(cc)] = cv
        for cc, cv in changed_fields.items():
            # Hmm: these might intersect with the RI values (e.g. when the whole tuple is the replica identity and
            # we're updating some of it)
            non_ri_cols.append(cc)
            non_ri_vals.append(cv)

    return ri_vals, non_ri_cols, non_ri_vals


def _recalculate_disjoint_ri_cols(ri_cols, ri_vals, non_ri_cols, non_ri_vals, row_data):
    # If part of the PK has been updated (is in the non_ri_cols/vals), we have to instead
    # apply the update to the PK (ri_cols/vals) and recalculate the new full new tuple
    # (by applying the update to row_data).
    new_nric = []
    new_nriv = []
    row_data = row_data.copy()

    for nrc, nrv in zip(non_ri_cols, non_ri_vals):
        try:
            ri_vals[ri_cols.index(nrc)] = nrv
        except ValueError:
            row_data[nrc] = nrv

    for col, val in row_data.items():
        if col not in ri_cols:
            new_nric.append(col)
            new_nriv.append(val)

    return ri_vals, new_nric, new_nriv


def _convert_audit_change(action, row_data, changed_fields, ri_cols):
    """
    Converts the audit log entry into Splitgraph's internal format.
    :returns: [(pk, kind, extra data)] (more than 1 change might be emitted from a single audit entry).
    """
    ri_vals, non_ri_cols, non_ri_vals = _split_ri_cols(action, row_data, changed_fields, ri_cols)
    pk_changed = any(c in ri_cols for c in non_ri_cols)
    if pk_changed:
        assert action == 'U'
        # If it's an update that changed the PK (e.g. the table has no replica identity so we treat the whole
        # tuple as a primary key), then we turn it into a delete old tuple + insert new one.
        result = [(tuple(ri_vals), 1, None)]

        # Recalculate the new PK to be inserted + the new (full) tuple, otherwise if the whole
        # tuple hasn't been updated, we'll lose parts of the old row (see test_diff_conflation_on_commit[test_case2]).
        ri_vals, non_ri_cols, non_ri_vals = _recalculate_disjoint_ri_cols(ri_cols, ri_vals,
                                                                          non_ri_cols, non_ri_vals, row_data)
        result.append((tuple(ri_vals), 0, {'c': non_ri_cols, 'v': non_ri_vals}))
        return result
    return [(tuple(ri_vals), _KIND[action],
             {'c': non_ri_cols, 'v': non_ri_vals} if action in ('I', 'U') else None)]


_KIND = {'I': 0, 'D': 1, 'U': 2}
