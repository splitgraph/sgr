"""Default Splitgraph engine: uses PostgreSQL to store metadata and actual objects and an audit stored procedure
to track changes, as well as the Postgres FDW interface to upload/download objects to/from other Postgres engines."""

import itertools
import json
import logging
from pkgutil import get_data

import psycopg2
from psycopg2 import DatabaseError
from psycopg2.extras import execute_batch, Json
from psycopg2.sql import SQL, Identifier
from splitgraph.config import SPLITGRAPH_META_SCHEMA, CONFIG
from splitgraph.core._common import select
from splitgraph.engine import ResultShape, ObjectEngine, ChangeEngine, SQLEngine, switch_engine
from splitgraph.exceptions import SplitGraphException
from splitgraph.hooks.mount_handlers import mount_postgres

_AUDIT_SCHEMA = 'audit'
_AUDIT_TRIGGER = 'resources/audit_trigger.sql'
_PACKAGE = 'splitgraph'
ROW_TRIGGER_NAME = "audit_trigger_row"
STM_TRIGGER_NAME = "audit_trigger_stm"
REMOTE_TMP_SCHEMA = "tmp_remote_data"
SG_UD_FLAG = 'sg_ud_flag'


class PsycopgEngine(SQLEngine):
    """Postgres SQL engine backed by a Psycopg connection."""

    def __init__(self, conn_params, name):
        """
        :param conn_params: Tuple of (server, port, username, password, dbname)
        """
        self.conn_params = conn_params
        self.name = name
        self._conn = None

    def __repr__(self):
        return "PostgresEngine %s (%s@%s:%s/%s)" % (self.name, self.conn_params[2], self.conn_params[0],
                                                    str(self.conn_params[1]), self.conn_params[4])

    def commit(self):
        if self._conn and not self._conn.closed:
            self._conn.commit()

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()

    def rollback(self):
        if self._conn and not self._conn.closed:
            self._conn.rollback()

    def lock_table(self, schema, table):
        self.run_sql(SQL("LOCK TABLE {}.{} IN ACCESS EXCLUSIVE MODE").format(Identifier(schema), Identifier(table)))

    @property
    def connection(self):
        """Engine-internal Psycopg connection. Will (re)open if closed/doesn't exist."""
        if self._conn is None or self._conn.closed:
            self._conn = make_conn(*self.conn_params)
        return self._conn

    def run_sql(self, statement, arguments=None, return_shape=ResultShape.MANY_MANY):
        with self.connection.cursor() as cur:
            try:
                cur.execute(statement, _convert_vals(arguments) if arguments else None)
            except DatabaseError:
                self.rollback()
                raise

            if cur.description is None:
                return None

            if return_shape == ResultShape.ONE_ONE:
                result = cur.fetchone()
                return result[0] if result else None
            if return_shape == ResultShape.ONE_MANY:
                return cur.fetchone()
            if return_shape == ResultShape.MANY_ONE:
                return [c[0] for c in cur.fetchall()]
            if return_shape == ResultShape.MANY_MANY:
                return cur.fetchall()

        # ResultShape.NONE or None
        return None

    def get_primary_keys(self, schema, table):
        """Inspects the Postgres information_schema to get the primary keys for a given table."""
        return self.run_sql(SQL("""SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                               FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid
                                                                      AND a.attnum = ANY(i.indkey)
                               WHERE i.indrelid = '{}.{}'::regclass AND i.indisprimary""")
                            .format(Identifier(schema), Identifier(table)), return_shape=ResultShape.MANY_MANY)

    def run_sql_batch(self, statement, arguments, schema=None):
        with self.connection.cursor() as cur:
            try:
                if schema:
                    cur.execute("SET search_path to %s;", (schema,))
                execute_batch(cur, statement, [_convert_vals(a) for a in arguments], page_size=1000)
                if schema:
                    cur.execute("SET search_path to public")
            except DatabaseError:
                self.rollback()
                raise

    def get_table_size(self, schema, table):
        return self.run_sql(SQL("SELECT pg_relation_size('{}.{}')").format(Identifier(schema), Identifier(table)),
                            return_shape=ResultShape.ONE_ONE)

    def _admin_conn(self):
        return psycopg2.connect(dbname=CONFIG['SG_ENGINE_POSTGRES_DB_NAME'],
                                user=CONFIG['SG_ENGINE_ADMIN_USER'],
                                password=CONFIG['SG_ENGINE_ADMIN_PWD'],
                                host=self.conn_params[0],
                                port=self.conn_params[1])

    def initialize(self):
        """Create the Splitgraph Postgres database and install the audit trigger"""
        # Use the connection to the "postgres" database to create the actual PG_DB
        with self._admin_conn() as admin_conn:
            # CREATE DATABASE can't run inside of tx
            pg_db = self.conn_params[4]

            admin_conn.autocommit = True
            with admin_conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (pg_db,))
                if cur.fetchone() is None:
                    logging.info("Creating database %s", pg_db)
                    cur.execute(SQL("CREATE DATABASE {}").format(Identifier(pg_db)))
                else:
                    logging.info("Database %s already exists, skipping", pg_db)

        # Install the audit trigger if it doesn't exist
        if not self.schema_exists(_AUDIT_SCHEMA):
            logging.info("Installing the audit trigger...")
            audit_trigger = get_data(_PACKAGE, _AUDIT_TRIGGER)
            self.run_sql(audit_trigger.decode('utf-8'), return_shape=ResultShape.NONE)
        else:
            logging.info("Skipping the audit trigger as it's already installed")

    def delete_database(self, database):
        """
        Helper function to drop a database using the admin connection

        :param database: Database name to drop
        """
        with self._admin_conn() as admin_conn:
            admin_conn.autocommit = True
            with admin_conn.cursor() as cur:
                cur.execute(SQL("DROP DATABASE IF EXISTS {}").format(Identifier(database)))


class AuditTriggerChangeEngine(PsycopgEngine, ChangeEngine):
    """Change tracking based on an audit trigger stored procedure"""

    def get_tracked_tables(self):
        """Return a list of tables that the audit trigger is working on."""
        return self.run_sql("SELECT DISTINCT event_object_schema, event_object_table "
                            "FROM information_schema.triggers WHERE trigger_name IN (%s, %s)",
                            (ROW_TRIGGER_NAME, STM_TRIGGER_NAME))

    def track_tables(self, tables):
        """Install the audit trigger on the required tables"""
        self.run_sql(SQL(";").join(SQL("SELECT audit.audit_table('{}.{}')").format(Identifier(s), Identifier(t))
                                   for s, t in tables))

    def untrack_tables(self, tables):
        """Remove triggers from tables and delete their pending changes"""
        for trigger in (ROW_TRIGGER_NAME, STM_TRIGGER_NAME):
            self.run_sql(SQL(";").join(SQL("DROP TRIGGER IF EXISTS {} ON {}.{}").format(
                Identifier(trigger), Identifier(s), Identifier(t))
                                       for s, t in tables))
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


class PostgresEngine(AuditTriggerChangeEngine, ObjectEngine):
    """An implementation of the Postgres engine for Splitgraph"""

    def store_fragment(self, inserted, deleted, schema, table, source_schema, source_table):
        # Add the upserted-deleted flag
        # Deletes are tricky: we store the full value here because we're assuming they
        # mostly don't happen and if they do, we will soon replace this fragment with a
        # new one that doesn't have those rows at all.

        schema_spec = self.get_full_table_schema(source_schema, source_table)
        # Assuming the schema_spec has the whole tuple as PK if the table has no PK.
        if all(not c[3] for c in schema_spec):
            schema_spec = [(c[0], c[1], c[2], True) for c in schema_spec]
        ri_cols = [c[1] for c in schema_spec if c[3]]
        ri_types = [c[2] for c in schema_spec if c[3]]
        non_ri_cols = [c[1] for c in schema_spec if not c[3]]
        all_cols = ri_cols + non_ri_cols
        # If no rows has been deleted, we don't need to add a special deletion flag to the fragment
        self.create_table(schema, table,
                          schema_spec=([(0, SG_UD_FLAG, 'boolean', False)] if deleted else []) + schema_spec)

        # Store upserts
        # INSERT INTO target_table (sg_ud_flag, col1, col2...)
        #   (SELECT true, t.col1, t.col2, ...
        #    FROM VALUES ((pk1_1, pk1_2), (pk2_1, pk2_2)...)) v JOIN source_table t
        #    ON v.pk1 = t.pk1::pk1_type AND v.pk2::pk2_type = t.pk2...
        #    -- the cast is required since the audit trigger gives us strings for values of updated columns
        #    -- and we're intending to join those with the PKs in the original table.
        if inserted:
            if non_ri_cols:
                query = SQL("INSERT INTO {}.{} (").format(Identifier(schema), Identifier(table)) + \
                        SQL(",").join(Identifier(c) for c in ([SG_UD_FLAG] if deleted else [])
                                      + all_cols) + SQL(")") + \
                        (SQL("(SELECT %s, ") if deleted else SQL("(SELECT ")) \
                        + SQL(",").join(SQL("t.") + Identifier(c) for c in all_cols) + \
                        SQL(" FROM (VALUES " +
                            ','.join(
                                itertools.repeat("(" + ','.join(itertools.repeat('%s', len(inserted[0]))) + ")",
                                                 len(inserted)))
                            + ")") + \
                        SQL(" AS v (") + SQL(",").join(Identifier(c) for c in ri_cols) + SQL(")") + \
                        SQL("JOIN {}.{} t").format(Identifier(source_schema), Identifier(source_table)) + \
                        SQL(" ON ") + SQL(" AND ").join(SQL("t.{0} = v.{0}::%s" % r)
                                                        .format(Identifier(c)) for c, r in zip(ri_cols, ri_types)) \
                        + SQL(")")
                # Flatten the args
                args = ([True] if deleted else []) + [p for pk in inserted for p in pk]
            else:
                # If the whole tuple is the PK, there's no point joining on the actual source table
                query = SQL("INSERT INTO {}.{} (").format(Identifier(schema), Identifier(table)) + \
                        SQL(",").join(Identifier(c) for c in ([SG_UD_FLAG] if deleted else []) + ri_cols) + SQL(")") + \
                        SQL("VALUES " +
                            ','.join(
                                itertools.repeat("(" + ','.join(itertools.repeat('%s', len(inserted[0])
                                                                                 + (1 if deleted else 0))) + ")",
                                                 len(inserted))))
                args = [p for pk in inserted for p in ([True] if deleted else []) + list(pk)]
            self.run_sql(query, args)

        # Store the deletes
        # we don't actually have the old values here so we put NULLs but we probably waste space anyway
        if deleted:
            query = SQL("INSERT INTO {}.{} (").format(Identifier(schema), Identifier(table)) + \
                    SQL(",").join(Identifier(c) for c in [SG_UD_FLAG] + ri_cols) + SQL(")") + \
                    SQL("VALUES " +
                        ','.join(itertools.repeat("(" + ','.join(itertools.repeat('%s', len(deleted[0]) + 1)) + ")",
                                                  len(deleted))))
            args = [p for pk in deleted for p in [False] + list(pk)]
            self.run_sql(query, args)

    def _prepare_ri_data(self, schema, table):
        ri_cols, _ = zip(*self.get_change_key(schema, table))
        non_ri_cols_types = [c for c in self.get_column_names_types(schema, table)
                             if c[0] not in ri_cols and c[0] != SG_UD_FLAG]
        non_ri_cols, non_ri_types = zip(*non_ri_cols_types) if non_ri_cols_types else ((), ())
        return ri_cols, non_ri_cols, non_ri_types

    def _generate_fragment_application(self, source_schema, source_table,
                                       target_schema, target_table, ri_data, extra_quals=None):
        ri_cols, non_ri_cols, _ = ri_data
        all_cols = ri_cols + non_ri_cols

        # First, delete all PKs from staging that are mentioned in the new fragment. This conveniently
        # covers both deletes and updates.
        query = SQL("DELETE FROM {0}.{2} USING {1}.{3}").format(
            Identifier(target_schema), Identifier(source_schema), Identifier(target_table),
            Identifier(source_table)) + SQL(" WHERE ") + _generate_where_clause(target_schema, target_table, ri_cols,
                                                                                source_table, source_schema)

        # At this point, we can insert all rows directly since we won't have any conflicts.
        # We can also apply extra qualifiers to only insert rows that match a certain query,
        # which will result in fewer rows actually being written to the staging table.
        has_ud_flag = any(c[1] == SG_UD_FLAG for c in self.get_full_table_schema(source_schema, source_table))
        # INSERT INTO target_table (col1, col2...)
        #   (SELECT col1, col2, ...
        #    FROM fragment_table WHERE sg_ud_flag = true (AND optional quals))
        query += SQL(";INSERT INTO {}.{} (").format(Identifier(target_schema), Identifier(target_table)) + \
                 SQL(",").join(Identifier(c) for c in all_cols) + SQL(")") + \
                 SQL("(SELECT ") + SQL(",").join(Identifier(c) for c in all_cols) + \
                 SQL(" FROM {}.{}").format(Identifier(source_schema), Identifier(source_table))
        extra_quals = [extra_quals] if extra_quals else []
        if has_ud_flag:
            extra_quals.append(SQL("{} = true").format(Identifier(SG_UD_FLAG)))
        if extra_quals:
            query += SQL(" WHERE ") + SQL(" AND ").join(extra_quals)
        query += SQL(")")
        return query

    def apply_fragments(self, objects, target_schema, target_table, extra_quals=None, extra_qual_args=None):
        ri_data = self._prepare_ri_data(target_schema, target_table)
        query = SQL(";").join(self._generate_fragment_application(ss, st, target_schema, target_table,
                                                                  ri_data, extra_quals)
                              for ss, st in objects)
        self.run_sql(query, (extra_qual_args * len(objects)) if extra_qual_args else None)

    def dump_table_sql(self, schema, table_name, stream, columns='*', where='', where_args=None):
        with self.connection.cursor() as cur:
            cur.execute(select(table_name, columns, where, schema), where_args)
            if cur.rowcount == 0:
                return

            stream.write(SQL("INSERT INTO {}.{} VALUES \n")
                         .format(Identifier(schema), Identifier(table_name)).as_string(self.connection))
            stream.write(',\n'.join(
                cur.mogrify("(" + ','.join(itertools.repeat('%s', len(row))) + ")", _convert_vals(row)).decode('utf-8')
                for row in cur))
        stream.write(";\n")

    # Utilities to dump objects (SNAP/DIFF) into an external format.
    # We use a slightly ad hoc format: the schema (JSON) + a null byte + Postgres's copy_to
    # binary format (only contains data). There's probably some scope to make this more optimized, maybe
    # we should look into columnar on-disk formats (Parquet/Avro) but we currently just want to get the objects
    # out of/into postgres as fast as possible.
    def dump_object(self, schema, table, stream):
        schema_spec = json.dumps(self.get_full_table_schema(schema, table))
        stream.write(schema_spec.encode('utf-8') + b'\0')
        with self.connection.cursor() as cur:
            cur.copy_expert(SQL("COPY {}.{} TO STDOUT WITH (FORMAT 'binary')")
                            .format(Identifier(schema), Identifier(table)), stream)

    def load_object(self, schema, table, stream):
        chars = b''
        # Read until the delimiter separating a JSON schema from the Postgres copy_to dump.
        # Surely this is buffered?
        while True:
            c = stream.read(1)
            if c == b'\0':
                break
            chars += c

        schema_spec = json.loads(chars.decode('utf-8'))
        self.create_table(schema, table, schema_spec)

        with self.connection.cursor() as cur:
            cur.copy_expert(SQL("COPY {}.{} FROM STDIN WITH (FORMAT 'binary')")
                            .format(Identifier(schema), Identifier(table)), stream)

    def upload_objects(self, objects, remote_engine):
        if not isinstance(remote_engine, PostgresEngine):
            raise SplitGraphException("Remote engine isn't a Postgres engine, object uploading "
                                      "is unsupported for now!")

        # Since we can't get remote to mount us, we instead use normal SQL statements
        # to create new tables remotely, then mount them and write into them from our side.
        # Is there seriously no better way to do this?
        # This also includes applying our table's FK constraints.
        create = self.dump_table_creation(schema=SPLITGRAPH_META_SCHEMA, tables=objects,
                                          created_schema=SPLITGRAPH_META_SCHEMA)
        remote_engine.run_sql(create, return_shape=ResultShape.NONE)
        # Have to commit the remote connection here since otherwise we won't see the new tables in the
        # mounted remote.
        remote_engine.commit()
        self.delete_schema(REMOTE_TMP_SCHEMA)

        # In case the local engine isn't the same as the target (e.g. we're running from the FDW)
        with switch_engine(self):
            mount_postgres(mountpoint=REMOTE_TMP_SCHEMA, server=remote_engine.conn_params[0],
                           port=remote_engine.conn_params[1], username=remote_engine.conn_params[2],
                           password=remote_engine.conn_params[3], dbname=remote_engine.conn_params[4],
                           remote_schema=SPLITGRAPH_META_SCHEMA)
        for i, obj in enumerate(objects):
            print("(%d/%d) %s..." % (i + 1, len(objects), obj))
            self.copy_table(SPLITGRAPH_META_SCHEMA, obj, REMOTE_TMP_SCHEMA, obj, with_pk_constraints=False)
        self.connection.commit()
        self.delete_schema(REMOTE_TMP_SCHEMA)

    def download_objects(self, objects, remote_engine):
        # Instead of connecting and pushing queries to it from the Python client, we just mount the remote mountpoint
        # into a temporary space (without any checking out) and SELECT the required data into our local tables.

        server, port, user, pwd, dbname = remote_engine.conn_params
        self.delete_schema(REMOTE_TMP_SCHEMA)
        logging.info("Mounting remote schema %s@%s:%s/%s/%s to %s...", user, server, port, dbname,
                     SPLITGRAPH_META_SCHEMA, REMOTE_TMP_SCHEMA)

        # In case the local engine isn't the same as the target (e.g. we're running from the FDW)
        with switch_engine(self):
            mount_postgres(mountpoint=REMOTE_TMP_SCHEMA, server=server, port=port, username=user, password=pwd,
                           dbname=dbname,
                           remote_schema=SPLITGRAPH_META_SCHEMA)

        try:
            for i, obj in enumerate(objects):
                logging.info("(%d/%d) Downloading %s...", i + 1, len(objects), obj)
                # Foreign tables don't have PK constraints so we'll have to apply them manually.
                self.copy_table(REMOTE_TMP_SCHEMA, obj, SPLITGRAPH_META_SCHEMA, obj, with_pk_constraints=False)
                # Get the PKs from the remote and apply them
                source_pks = remote_engine.get_primary_keys(SPLITGRAPH_META_SCHEMA, obj)
                if source_pks:
                    self.run_sql(SQL("ALTER TABLE {}.{} ADD PRIMARY KEY (").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier(obj))
                                 + SQL(',').join(SQL("{}").format(Identifier(c)) for c, _ in source_pks) + SQL(")"),
                                 return_shape=ResultShape.NONE)
                self.connection.commit()
        finally:
            self.delete_schema(REMOTE_TMP_SCHEMA)


def _split_ri_cols(action, row_data, changed_fields, ri_cols):
    """
    :return: `(ri_data, non_ri_data)`: a tuple of 2 dictionaries:
        * `ri_data`: maps column names in `ri_cols` to values identifying the replica identity (RI) of a given tuple
        * `non_ri_data`: map of column names and values not in the RI that have been changed/updated
    """
    non_ri_data = {}
    ri_data = {}

    if action == 'I':
        for cc, cv in row_data.items():
            if cc in ri_cols:
                ri_data[cc] = cv
            else:
                non_ri_data[cc] = cv
    elif action == 'D':
        for cc, cv in row_data.items():
            if cc in ri_cols:
                ri_data[cc] = cv
    elif action == 'U':
        for cc, cv in row_data.items():
            if cc in ri_cols:
                ri_data[cc] = cv
        if changed_fields:
            for cc, cv in changed_fields.items():
                non_ri_data[cc] = cv

    return ri_data, non_ri_data


def _recalculate_disjoint_ri_cols(ri_cols, ri_data, non_ri_data, row_data):
    # If part of the PK has been updated (is in the non_ri_cols/vals), we have to instead
    # apply the update to the PK (ri_cols/vals) and recalculate the new full new tuple
    # (by applying the update to row_data).
    new_non_ri_data = {}
    row_data = row_data.copy()

    for nrc, nrv in non_ri_data.items():
        if nrc in ri_cols:
            ri_data[nrc] = nrv
        else:
            row_data[nrc] = nrv

    for col, val in row_data.items():
        if col not in ri_cols:
            new_non_ri_data[col] = val

    return ri_data, new_non_ri_data


def _convert_audit_change(action, row_data, changed_fields, ri_cols):
    """
    Converts the audit log entry into Splitgraph's internal format.

    :returns: [(pk, (True for upserted, False for deleted), (old row value if updated/deleted))].
        More than 1 change might be emitted from a single audit entry.
    """
    ri_data, non_ri_data = _split_ri_cols(action, row_data, changed_fields, ri_cols)
    pk_changed = any(c in ri_cols for c in non_ri_data)
    if pk_changed:
        assert action == 'U'
        # If it's an update that changed the PK (e.g. the table has no replica identity so we treat the whole
        # tuple as a primary key), then we turn it into (old PK, DELETE, old row data); (new PK, INSERT, {})

        # Recalculate the new PK to be inserted + the new (full) tuple, otherwise if the whole
        # tuple hasn't been updated, we'll lose parts of the old row (see test_diff_conflation_on_commit[test_case2]).

        result = [(tuple(ri_data[c] for c in ri_cols), False, row_data)]

        ri_data, non_ri_data = _recalculate_disjoint_ri_cols(ri_cols, ri_data, non_ri_data, row_data)
        result.append((tuple(ri_data[c] for c in ri_cols), True, {}))
        return result
    if action == 'U' and not non_ri_data:
        # Nothing was actually updated -- don't emit an action
        return []
    return [(tuple(ri_data[c] for c in ri_cols), action in ('I', 'U'), row_data if action in ('U', 'D') else {})]


_KIND = {'I': 0, 'D': 1, 'U': 2}


def _convert_vals(vals):
    """Psycopg returns jsonb objects as dicts/lists but doesn't actually accept them directly
    as a query param (or in the case of lists coerces them into an array.
    Hence, we have to wrap them in the Json datatype when doing a dump + load."""
    return [Json(v) if isinstance(v, dict)
                       or (isinstance(v, list) and v and isinstance(v[0], (list, tuple)))
            else v for v in vals]


def _generate_where_clause(schema, table, cols, table_2=None, schema_2=None):
    if not table_2:
        return SQL(" AND ").join(SQL("{}.{}.{} = %s").format(
            Identifier(schema), Identifier(table), Identifier(c)) for c in cols)
    return SQL(" AND ").join(SQL("{}.{}.{} = {}.{}.{}").format(
        Identifier(schema), Identifier(table), Identifier(c),
        Identifier(schema_2), Identifier(table_2), Identifier(c)) for c in cols)


def make_conn(server, port, username, password, dbname):
    """
    Initializes a connection a Splitgraph Postgres engine.

    :return: Psycopg connection object
    """
    return psycopg2.connect(host=server, port=port, user=username, password=password, dbname=dbname)
