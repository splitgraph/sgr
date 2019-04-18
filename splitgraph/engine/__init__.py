"""
Defines the interface for a Splitgraph engine (a backing database), including running basic SQL commands,
tracking tables for changes and uploading/downloading tables to other remote engines.

By default, Splitgraph is backed by Postgres: see :mod:`splitgraph.engine.postgres` for an example of how to
implement a different engine.
"""
from abc import ABC
from contextlib import contextmanager
from enum import Enum

from psycopg2.sql import SQL, Identifier

from splitgraph.config import CONFIG

# List of config flags that are extracted from the global configuration and passed to a given engine
_ENGINE_SPECIFIC_CONFIG = ['SG_ENGINE_HOST', 'SG_ENGINE_PORT', 'SG_ENGINE_USER', 'SG_ENGINE_PWD',
                           'SG_ENGINE_DB_NAME', 'SG_ENGINE_POSTGRES_DB_NAME', 'SG_ENGINE_ADMIN_USER',
                           'SG_ENGINE_ADMIN_PWD', 'SG_ENGINE_FDW_HOST', 'SG_ENGINE_FDW_PORT']


class ResultShape(Enum):
    """Shape that the result of a query will be coerced to"""
    NONE = 0  # No result expected
    ONE_ONE = 1  # e.g. "row1_val1"
    ONE_MANY = 2  # e.g. ("row1_val1", "row1_val_2")
    MANY_ONE = 3  # e.g. ["row1_val1", "row2_val_1", ...]
    MANY_MANY = 4  # e.g. [("row1_val1", "row1_val_2"), ("row2_val1", "row2_val_2"), ...]


class SQLEngine(ABC):
    """Abstraction for a Splitgraph SQL backend. Requires any overriding classes to implement `run_sql` as well as
    a few other functions. Together with the `information_schema` (part of the SQL standard), this class uses those
    functions to implement some basic database management methods like listing, deleting, creating, dumping
    and loading tables."""

    def run_sql(self, statement, arguments=None, return_shape=ResultShape.MANY_MANY):
        """Run an arbitrary SQL statement with some arguments, return an iterator of results.
        If the statement doesn't return any results, return None."""
        raise NotImplementedError()

    def commit(self):
        """Commit the engine's backing connection"""

    def close(self):
        """Commit and close the engine's backing connection"""

    def rollback(self):
        """Rollback the engine's backing connection"""

    def run_sql_batch(self, statement, arguments, schema=None):
        """Run a parameterized SQL statement against multiple sets of arguments. Other engines
        can override if they support a more efficient batching mechanism."""
        for args in arguments:
            if schema:
                self.run_sql_in(schema, statement, args, return_shape=ResultShape.NONE)
            else:
                self.run_sql(statement, args, return_shape=ResultShape.NONE)

    def run_sql_in(self, schema, sql, arguments=None, return_shape=ResultShape.MANY_MANY):
        """
        Executes a non-schema-qualified query against a specific schema.

        :param schema: Schema to run the query in
        :param sql: Query
        :param arguments: Query arguments
        :param return_shape: ReturnShape to coerce the result into.
        """
        self.run_sql("SET search_path TO %s", (schema,), return_shape=ResultShape.NONE)
        result = self.run_sql(sql, arguments, return_shape=return_shape)
        self.run_sql("SET search_path TO public", (schema,), return_shape=ResultShape.NONE)
        return result

    def table_exists(self, schema, table_name):
        """
        Check if a table exists on the engine.

        :param schema: Schema name
        :param table_name: Table name
        """
        return self.run_sql("""SELECT table_name from information_schema.tables
                           WHERE table_schema = %s AND table_name = %s""", (schema, table_name[:63]),
                            ResultShape.ONE_ONE) is not None

    def schema_exists(self, schema):
        """
        Check if a schema exists on the engine.

        :param schema: Schema name
        """
        return self.run_sql("""SELECT 1 from information_schema.schemata
                           WHERE schema_name = %s""", (schema,), return_shape=ResultShape.ONE_ONE) is not None

    def create_schema(self, schema):
        """Create a schema if it doesn't exist"""
        return self.run_sql(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(schema)),
                            return_shape=ResultShape.NONE)

    def copy_table(self, source_schema, source_table, target_schema, target_table, with_pk_constraints=True,
                   limit=None, offset=None, order_by_pk=False):
        """Copy a table in the same engine, optionally applying primary key constraints as well."""
        query_args = []
        if not self.table_exists(target_schema, target_table):
            query = SQL("CREATE TABLE {}.{} AS SELECT * FROM {}.{}").format(
                Identifier(target_schema), Identifier(target_table),
                Identifier(source_schema), Identifier(source_table))
        else:
            query = SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
                Identifier(target_schema), Identifier(target_table),
                Identifier(source_schema), Identifier(source_table))
        pks = self.get_primary_keys(source_schema, source_table)
        if order_by_pk and pks:
            query += SQL(" ORDER BY ") + SQL(",").join(Identifier(p[0]) for p in pks)
        if limit:
            query += SQL(" LIMIT %s")
            query_args.append(limit)
        if offset:
            query += SQL(" OFFSET %s")
            query_args.append(offset)
        if with_pk_constraints and pks:
            query += SQL(";ALTER TABLE {}.{} ADD PRIMARY KEY (").format(
                Identifier(target_schema), Identifier(target_table)) + SQL(',').join(
                SQL("{}").format(Identifier(c)) for c, _ in pks) + SQL(")")
        self.run_sql(query, query_args, return_shape=ResultShape.NONE)

    def delete_table(self, schema, table):
        """Drop a table from a schema if it exists"""
        if self.get_table_type(schema, table) not in ('FOREIGN TABLE', 'FOREIGN'):
            self.run_sql(SQL("DROP TABLE IF EXISTS {}.{}").format(Identifier(schema), Identifier(table)))
        else:
            self.run_sql(SQL("DROP FOREIGN TABLE IF EXISTS {}.{}").format(Identifier(schema), Identifier(table)))

    def rename_table(self, schema, table, new_table):
        """Rename a table"""
        self.run_sql(SQL("ALTER TABLE {}.{} RENAME TO {}").format(Identifier(schema), Identifier(table),
                                                                  Identifier(new_table)))

    def delete_schema(self, schema):
        """Delete a schema if it exists, including all the tables in it."""
        self.run_sql(
            SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(schema)),
            return_shape=ResultShape.NONE)

    def get_all_tables(self, schema):
        """Get all tables in a given schema."""
        return self.run_sql("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (schema,),
                            return_shape=ResultShape.MANY_ONE)

    def get_table_type(self, schema, table):
        """Get the type of the table (BASE or FOREIGN)
        """
        return self.run_sql("SELECT table_type FROM information_schema.tables WHERE table_schema = %s"
                            " AND table_name = %s", (schema, table),
                            return_shape=ResultShape.ONE_ONE)

    def get_table_size(self, schema, table):
        """Return the table disk usage, in bytes."""
        raise NotImplementedError()

    def get_primary_keys(self, schema, table):
        """Get a list of (column_name, column_type) denoting the primary keys of a given table."""
        raise NotImplementedError()

    def dump_table_creation(self, schema, tables, created_schema):
        """
        Dumps the basic table schema (column names, data types, is_nullable) for one or more tables into SQL statements.

        :param schema: Schema to dump tables from
        :param tables: Tables to dump
        :param created_schema: The new schema that the tables will be created under.
        :return: An SQL statement that reconstructs the schema for the given tables.
        """
        queries = []

        for table in tables:
            cols = self.run_sql("""SELECT column_name, data_type, is_nullable
                           FROM information_schema.columns
                           WHERE table_name = %s AND table_schema = %s""", (table, schema))
            target = SQL("{}.{}").format(Identifier(created_schema), Identifier(table))
            query = SQL("CREATE TABLE {} (").format(target) + SQL(','.join(
                "{} %s " % ctype + ("NOT NULL" if not cnull else "") for _, ctype, cnull in cols)).format(
                *(Identifier(cname) for cname, _, _ in cols))

            pks = self.get_primary_keys(schema, table)
            if pks:
                query += SQL(", PRIMARY KEY (") + SQL(',').join(SQL("{}").format(Identifier(c)) for c, _ in pks) + SQL(
                    "))")
            else:
                query += SQL(")")

            queries.append(query)
        return SQL(';').join(queries)

    def create_table(self, schema, table, schema_spec, unlogged=False):
        """
        Creates a table using a previously-dumped table schema spec

        :param schema: Schema to create the table in
        :param table: Table name to create
        :param schema_spec: A list of (ordinal_position, column_name, data_type, is_pk) specifying the table schema
        :param unlogged: If True, the table won't be reflected in the WAL or scanned by the analyzer/autovacuum.
        """

        schema_spec = sorted(schema_spec)

        target = SQL("{}.{}").format(Identifier(schema), Identifier(table))
        query = SQL("CREATE " + ("UNLOGGED" if unlogged else "") + " TABLE {} (").format(target) \
                + SQL(','.join("{} %s " % ctype for _, _, ctype, _ in schema_spec)) \
                    .format(*(Identifier(cname) for _, cname, _, _ in schema_spec))

        pk_cols = [cname for _, cname, _, is_pk in schema_spec if is_pk]
        if pk_cols:
            query += SQL(", PRIMARY KEY (") + SQL(',').join(SQL("{}").format(Identifier(c)) for c in pk_cols) + SQL(
                "))")
        else:
            query += SQL(")")
        if unlogged:
            query += SQL(" WITH(autovacuum_enabled=false)")
        self.run_sql(query, return_shape=ResultShape.NONE)

    def dump_table_sql(self, schema, table_name, stream, columns='*', where='', where_args=None):
        """
        Dump the table contents in the SQL format
        :param schema: Schema the table is located in
        :param table_name: Name of the table
        :param stream: A file-like object to write the result into.
        :param columns: SQL column spec. Default '*'.
        :param where: Optional, an SQL WHERE clause
        :param where_args: Arguments for the optional WHERE clause.
        """
        raise NotImplementedError()

    def get_column_names_types(self, schema, table_name):
        """Returns a list of (column, type) in a given table."""
        return self.run_sql("""SELECT column_name, data_type FROM information_schema.columns
                           WHERE table_schema = %s
                           AND table_name = %s""", (schema, table_name))

    def get_full_table_schema(self, schema, table_name):
        """
        Generates a list of (column ordinal, name, data type, is_pk), used to detect schema changes like columns being
        dropped/added/renamed or type changes.
        """
        results = self.run_sql("""SELECT ordinal_position, column_name, data_type FROM information_schema.columns
                           WHERE table_schema = %s
                           AND table_name = %s
                           ORDER BY ordinal_position""", (schema, table_name))

        def _convert_type(ctype):
            # We don't keep a lot of type information, so e.g. char(5) gets turned into char
            # which defaults into char(1).
            return ctype if ctype != 'character' else 'character varying'

        # Do we need to make sure the PK has the same type + ordinal position here?
        pks = [pk for pk, _ in self.get_primary_keys(schema, table_name)]
        return [(o, n, _convert_type(dt), (n in pks)) for o, n, dt in results]

    def initialize(self, skip_audit=False):
        """Does any required initialization of the engine

        :param skip_audit: If True, skips installation of audit triggers for engines that don't need change tracking.
        """

    def lock_table(self, schema, table):
        """Acquire an exclusive lock on a given table, released when the transaction commits / rolls back."""
        raise NotImplementedError()


class ChangeEngine(SQLEngine, ABC):
    """An SQL engine that can perform change tracking on a set of tables."""

    def get_tracked_tables(self):
        """
        :return: A list of (table_schema, table_name) that the engine currently tracks for changes
        """
        raise NotImplementedError()

    def track_tables(self, tables):
        """
        Start engine-specific change tracking on a list of tables.

        :param tables: List of (table_schema, table_name) to start tracking
        """
        raise NotImplementedError()

    def untrack_tables(self, tables):
        """
        Stop engine-specific change tracking on a list of tables and delete any pending changes.

        :param tables: List of (table_schema, table_name) to start tracking
        """
        raise NotImplementedError()

    def has_pending_changes(self, schema):
        """
        Return True if the tracked schema has pending changes and False if it doesn't.
        """
        raise NotImplementedError()

    def discard_pending_changes(self, schema, table=None):
        """
        Discard recorded pending changes for a tracked table or the whole schema
        """
        raise NotImplementedError()

    def get_pending_changes(self, schema, table, aggregate=False):
        """
        Return pending changes for a given tracked table

        :param schema: Schema the table belongs to
        :param table: Table to return changes for
        :param aggregate: Whether to aggregate changes or return them completely
        :return: If aggregate is True: tuple with numbers of `(added_rows, removed_rows, updated_rows)`.
            If aggregate is False: A changeset. The changeset is a list of
            `(pk, action (0 for Insert, 1 for Delete, 2 for Update), action_data)`
            where `action_data` is `None` for Delete and `{'c': [column_names], 'v': [column_values]}` that
            have been inserted/updated otherwise.
        """
        raise NotImplementedError()

    def get_changed_tables(self, schema):
        """
        List tracked tables that have pending changes

        :param schema: Schema to check for changes
        :return: List of tables with changed contents
        """
        raise NotImplementedError()

    def get_change_key(self, schema, table):
        """
        Returns the key used to identify a row in a change (list of column name, column type).
        If the tracked table has a PK, we use that; if it doesn't, the whole row is used.
        """
        return self.get_primary_keys(schema, table) or self.get_column_names_types(schema, table)


class ObjectEngine:
    """
    Routines for storing/applying objects as well as sharing them with other engines.
    """

    def store_fragment(self, inserted, deleted, schema, table, source_schema, source_table):
        """
        Store a fragment of a changed table in another table

        :param inserted: List of PKs that have been updated/inserted
        :param deleted: List of PKs that have been deleted
        :param schema: Schema to store the change in
        :param table: Table to store the change in
        :param source_schema: Schema the source table is located in
        :param source_table: Name of the source table
        """
        raise NotImplementedError()

    def apply_fragments(self, objects, target_schema, target_table, extra_quals=None,
                        extra_qual_args=None):
        """
        Apply multiple fragments to a target table as a single-query batch operation.

        :param objects: List of tuples `(object_schema, object_table)` that the objects are stored in.
        :param target_schema: Schema to apply the fragment to
        :param target_table: Table to apply the fragment to
        :param extra_quals: Optional, extra SQL (Composable) clauses to filter new rows in the fragment on
            (e.g. SQL("a = %s"))
        :param extra_qual_args: Optional, a tuple of arguments to use with `extra_quals`
        """
        raise NotImplementedError()

    def dump_object(self, schema, table, stream):
        """
        Dump a table to a stream using an engine-specific binary format.

        :param schema: Schema the table lives in
        :param table: Table to dump
        :param stream: A file-like stream to dump the object into
        """
        raise NotImplementedError()

    def load_object(self, schema, table, stream):
        """
        Load a table from a stream using an engine-specific binary format.

        :param schema: Schema to create the table in. Must already exist.
        :param table: Table to create. Must not exist.
        :param stream: A file-like stream to load the object from
        """
        raise NotImplementedError()

    def upload_objects(self, objects, remote_engine):
        """
        Upload objects from the local cache to the remote engine

        :param objects: List of object IDs to upload
        :param remote_engine: A remote ObjectEngine to upload the objects to.
        """
        raise NotImplementedError()

    def download_objects(self, objects, remote_engine):
        """
        Download objects from the remote engine to the local cache

        :param objects: List of object IDs to download
        :param remote_engine: A remote ObjectEngine to download the objects from.

        :return List of object IDs that were downloaded.
        """
        raise NotImplementedError()


# Name of the current global engine, 'LOCAL' for the local.
# Can be overridden via normal configuration routes, e.g.
# $ SG_ENGINE=remote_engine sgr init
# will initialize the remote engine instead.
_ENGINE = CONFIG['SG_ENGINE'] or 'LOCAL'

# Map of engine names -> Engine instances
_ENGINES = {}


def get_engine(name=None, use_socket=False):
    """
    Get the current global engine or a named remote engine

    :param name: Name of the remote engine as specified in the config. If None, the current global engine
        is returned.
    :param use_socket: Use a local UNIX socket instead of PG_HOST, PG_PORT for LOCAL engine connections.
    """
    if not name:
        if isinstance(_ENGINE, SQLEngine):
            return _ENGINE
        name = _ENGINE
    if name not in _ENGINES:
        # Here we'd get the engine type/backend (Postgres/MySQL etc)
        # and instantiate the actual Engine class.
        # As we only have PostgresEngine, we instantiate that.
        from .postgres.engine import PostgresEngine
        if name == 'LOCAL':
            conn_params = {c: CONFIG[c] for c in _ENGINE_SPECIFIC_CONFIG}
            if use_socket:
                conn_params['SG_ENGINE_HOST'] = None
                conn_params['SG_ENGINE_PORT'] = None
        else:
            conn_params = {c: CONFIG['remotes'][name][c] for c in _ENGINE_SPECIFIC_CONFIG}
        _ENGINES[name] = PostgresEngine(conn_params=conn_params, name=name)
    return _ENGINES[name]


@contextmanager
def switch_engine(engine):
    """
    Switch the global engine to a different one. The engine will
    get switched back on exit from the context manager.

    :param engine: Name of the engine or an SQLEngine instance
    """
    if not isinstance(engine, str) and not isinstance(engine, SQLEngine):
        raise ValueError("engine must be an engine name or an SQLEngine, not %r!" % type(engine))
    global _ENGINE
    _prev_engine = _ENGINE
    try:
        _ENGINE = engine
        yield
    finally:
        _ENGINE = _prev_engine


def get_remote_connection_params(remote_name):
    """
    Get connection parameters for a Splitgraph remote.

    :param remote_name: Name of the remote. Must be specified in the config file.
    :return: A tuple of (hostname, port, username, password, database)
    """
    pdict = CONFIG['remotes'][remote_name]
    return (pdict['SG_ENGINE_HOST'] or None, int(pdict['SG_ENGINE_PORT']) if pdict['SG_ENGINE_PORT'] else None,
            pdict['SG_ENGINE_USER'], pdict['SG_ENGINE_PWD'], pdict['SG_ENGINE_DB_NAME'])
