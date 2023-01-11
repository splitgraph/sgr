import itertools
import re
import threading
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union, cast

from psycopg2.sql import SQL, Composed, Identifier

from splitgraph.core.overlay import (
    WRITE_LOWER_PREFIX,
    WRITE_MERGED_PREFIX,
    WRITE_UPPER_PREFIX,
)
from splitgraph.core.types import TableColumn, TableSchema
from splitgraph.engine import ResultShape


def validate_type(t: str) -> str:
    if not re.match(r"[\w ()\[\]]", t):
        raise ValueError("Invalid type %s!" % t)
    return t


class SavepointStack(threading.local):
    def __init__(self):
        self.stack: List[str] = []


class SQLEngine(ABC):
    """Abstraction for a Splitgraph SQL backend. Requires any overriding classes to implement `run_sql` as well as
    a few other functions. Together with the `information_schema` (part of the SQL standard), this class uses those
    functions to implement some basic database management methods like listing, deleting, creating, dumping
    and loading tables."""

    def __init__(self) -> None:
        self._savepoint_stack = SavepointStack()

    @contextmanager
    def savepoint(self, name: str) -> Iterator[None]:
        """At the beginning of this context manager, a savepoint is initialized and any database
        error that occurs in run_sql results in a rollback to this savepoint rather than the
        rollback of the whole transaction. At exit, the savepoint is released."""
        self.run_sql(SQL("SAVEPOINT ") + Identifier(name))
        self._savepoint_stack.stack.append(name)
        try:
            yield
            # Don't catch any exceptions here: the implementer's run_sql method is supposed
            # to do that and roll back to the savepoint.
        finally:
            # If the savepoint wasn't rolled back to, release it.
            if self._savepoint_stack.stack and self._savepoint_stack.stack[-1] == name:
                self._savepoint_stack.stack.pop()
                self.run_sql(SQL("RELEASE SAVEPOINT ") + Identifier(name))

    @abstractmethod
    def run_sql(self, statement, arguments=None, return_shape=ResultShape.MANY_MANY, named=False):
        """Run an arbitrary SQL statement with some arguments, return an iterator of results.
        If the statement doesn't return any results, return None. If named=True, return named
        tuples when possible."""

    @abstractmethod
    def commit(self):
        """Commit the engine's backing connection"""

    @abstractmethod
    def close(self):
        """Commit and close the engine's backing connection"""

    @abstractmethod
    def rollback(self):
        """Rollback the engine's backing connection"""

    @abstractmethod
    def run_sql_batch(self, statement, arguments, schema=None):
        """Run a parameterized SQL statement against multiple sets of arguments.

        :param statement: Statement to run
        :param arguments: Query arguments
        :param schema: Schema to run the statement in"""

    def run_sql_in(
        self,
        schema: str,
        sql: Union[Composed, str],
        arguments: None = None,
        return_shape: ResultShape = ResultShape.MANY_MANY,
    ) -> Any:
        """
        Executes a non-schema-qualified query against a specific schema.

        :param schema: Schema to run the query in
        :param sql: Query
        :param arguments: Query arguments
        :param return_shape: ReturnShape to coerce the result into.
        """
        self.run_sql("SET search_path TO %s,public", (schema,), return_shape=ResultShape.NONE)
        result = self.run_sql(sql, arguments, return_shape=return_shape)
        self.run_sql("SET search_path TO public", (schema,), return_shape=ResultShape.NONE)
        return result

    def table_exists(self, schema: str, table_name: str) -> bool:
        """
        Check if a table exists on the engine.

        :param schema: Schema name
        :param table_name: Table name
        """
        return (
            self.run_sql(
                """SELECT table_name from information_schema.tables
                           WHERE table_schema = %s AND table_name = %s""",
                (schema, table_name[:63]),
                ResultShape.ONE_ONE,
            )
            is not None
        )

    def schema_exists(self, schema: str) -> bool:
        """
        Check if a schema exists on the engine.

        :param schema: Schema name
        """
        return (
            self.run_sql(
                """SELECT 1 from information_schema.schemata
                           WHERE schema_name = %s""",
                (schema,),
                return_shape=ResultShape.ONE_ONE,
            )
            is not None
        )

    def create_schema(self, schema: str) -> None:
        """Create a schema if it doesn't exist"""
        self.run_sql(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(schema)))

    def copy_table(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        with_pk_constraints: bool = True,
        cursor_fields: Optional[Dict[str, str]] = None,
    ) -> None:
        """Copy a table in the same engine, optionally applying primary key constraints as well."""

        if cursor_fields:
            where_clause = (
                SQL(" WHERE (")
                + SQL(",").join(map(Identifier, cursor_fields))
                + SQL(") > (")
                + SQL(",").join(itertools.repeat(SQL("%s"), len(cursor_fields)))
                + SQL(")")
            )
            query_args = list(cursor_fields.values())
        else:
            where_clause = SQL("")
            query_args = []

        if not self.table_exists(target_schema, target_table):
            query = (
                SQL("CREATE TABLE {}.{} AS SELECT * FROM {}.{}").format(
                    Identifier(target_schema),
                    Identifier(target_table),
                    Identifier(source_schema),
                    Identifier(source_table),
                )
                + where_clause
            )
        else:
            query = (
                SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
                    Identifier(target_schema),
                    Identifier(target_table),
                    Identifier(source_schema),
                    Identifier(source_table),
                )
                + where_clause
            )
        pks = self.get_primary_keys(source_schema, source_table)

        if with_pk_constraints and pks:
            query += (
                SQL(";ALTER TABLE {}.{} ADD PRIMARY KEY (").format(
                    Identifier(target_schema), Identifier(target_table)
                )
                + SQL(",").join(SQL("{}").format(Identifier(c)) for c, _ in pks)
                + SQL(")")
            )
        self.run_sql(query, query_args)

    def delete_table(self, schema: str, table: str) -> None:
        """Drop a table from a schema if it exists"""
        table_type = self.get_table_type(schema, table)
        # Add CASCADE to drop dependent tables like views etc.
        if table_type == "VIEW":
            self.run_sql(
                SQL("DROP VIEW IF EXISTS {}.{} CASCADE").format(
                    Identifier(schema), Identifier(table)
                )
            )
        elif table_type not in ("FOREIGN TABLE", "FOREIGN"):
            self.run_sql(
                SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                    Identifier(schema), Identifier(table)
                )
            )
        else:
            self.run_sql(
                SQL("DROP FOREIGN TABLE IF EXISTS {}.{}").format(
                    Identifier(schema), Identifier(table)
                )
            )

    def delete_schema(self, schema: str) -> None:
        """Delete a schema if it exists, including all the tables in it."""
        self.run_sql(
            SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(schema)),
            return_shape=ResultShape.NONE,
        )

    def get_all_tables(self, schema: str, include_overlay_components: bool = False) -> List[str]:
        """
        Get all tables in a given schema.

        :param schema: Schema to fetch the table from.
        :param include_overlay_components: Controls whether to return upper and lower tables of on a overlay view.
        """
        query = SQL("SELECT table_name FROM information_schema.tables WHERE table_schema = %s")
        args = [schema]

        if not include_overlay_components:
            query += SQL(" AND table_name NOT LIKE ALL (ARRAY[%s, %s, %s])")
            args += [
                WRITE_LOWER_PREFIX + "%",
                WRITE_UPPER_PREFIX + "%",
                WRITE_MERGED_PREFIX + "%",
            ]

        return cast(
            List[str],
            self.run_sql(query, args, return_shape=ResultShape.MANY_ONE),
        )

    def get_table_type(self, schema: str, table: str) -> Optional[str]:
        """Get the type of the table (BASE TABLE, FOREIGN or VIEW)"""
        result = self.run_sql(
            "SELECT table_type FROM information_schema.tables WHERE table_schema = %s"
            " AND table_name = %s",
            (schema, table),
            return_shape=ResultShape.ONE_ONE,
        )
        return cast(Optional[str], result)

    @abstractmethod
    def get_primary_keys(self, schema, table):
        """Get a list of (column_name, column_type) denoting the primary keys of a given table."""

    @staticmethod
    def dump_table_creation(
        schema: Optional[str],
        table: str,
        schema_spec: "TableSchema",
        unlogged: bool = False,
        temporary: bool = False,
        include_comments: bool = False,
    ) -> Tuple[Composed, Tuple]:
        """
        Dumps the DDL for a table using a previously-dumped table schema spec

        :param schema: Schema to create the table in
        :param table: Table name to create
        :param schema_spec: TableSchema
        :param unlogged: If True, the table won't be reflected in the WAL or scanned by the analyzer/autovacuum.
        :param temporary: If True, a temporary table is created (the schema parameter is ignored)
        :param include_comments: If True, also adds COMMENT statements for columns that have them.
        :return: An SQL statement that reconstructs the table schema + args to be mogrified into it.
        """
        flavour = ""
        if unlogged:
            flavour = "UNLOGGED"
        if temporary:
            flavour = "TEMPORARY"

        schema_spec = sorted(schema_spec)

        if temporary:
            target = Identifier(table)
        else:
            target = SQL("{}.{}").format(Identifier(schema), Identifier(table))
        query = (
            SQL("CREATE " + flavour + " TABLE ")
            + target
            + SQL(
                " (" + ",".join("{} %s " % validate_type(col.pg_type) for col in schema_spec)
            ).format(*(Identifier(col.name) for col in schema_spec))
        )

        pk_cols = [col.name for col in schema_spec if col.is_pk]
        if pk_cols:
            query += (
                SQL(", PRIMARY KEY (")
                + SQL(",").join(SQL("{}").format(Identifier(c)) for c in pk_cols)
                + SQL("))")
            )
        else:
            query += SQL(")")
        if unlogged:
            query += SQL(" WITH(autovacuum_enabled=false)")
        query += SQL(";")

        args = []
        if include_comments:
            for col in schema_spec:
                if col.comment:
                    query += SQL("COMMENT ON COLUMN {}.{} IS %s;").format(
                        target, Identifier(col.name)
                    )
                    args.append(col.comment)

        return query, tuple(args)

    def create_table(
        self,
        schema: Optional[str],
        table: str,
        schema_spec: "TableSchema",
        unlogged: bool = False,
        temporary: bool = False,
        include_comments: bool = False,
    ) -> None:
        """
        Creates a table using a previously-dumped table schema spec

        :param schema: Schema to create the table in
        :param table: Table name to create
        :param schema_spec: TableSchema
        :param unlogged: If True, the table won't be reflected in the WAL or scanned by the analyzer/autovacuum.
        :param temporary: If True, a temporary table is created (the schema parameter is ignored)
        :param include_comments: If True, also adds COMMENT statements for columns that have them.
        """
        query, args = self.dump_table_creation(
            schema, table, schema_spec, unlogged, temporary, include_comments
        )
        self.run_sql(query, args)

    @abstractmethod
    def dump_table_sql(
        self,
        schema,
        table_name,
        stream,
        columns="*",
        where="",
        where_args=None,
        target_schema=None,
        target_table=None,
    ):
        """
        Dump the table contents in the SQL format
        :param schema: Schema the table is located in
        :param table_name: Name of the table
        :param stream: A file-like object to write the result into.
        :param columns: SQL column spec. Default '*'.
        :param where: Optional, an SQL WHERE clause
        :param where_args: Arguments for the optional WHERE clause.
        :param target_schema: Schema to create the table in (default same as `schema`)
        :param target_table: Name of the table to insert data into (default same as `table_name`)
        """

    def get_full_table_schema(self, schema: str, table_name: str) -> "TableSchema":
        """
        Generates a list of (column ordinal, name, data type, is_pk, column comment),
        used to detect schema changes like columns being dropped/added/renamed or type changes.

        NB this doesn't work for temporary tables (pg_temp) and returns an empty schema.
        """
        assert schema != "pg_temp"

        results = self.run_sql(
            "SELECT c.attnum, c.attname, "
            "pg_catalog.format_type(c.atttypid, c.atttypmod), pgd.description "
            "FROM pg_attribute c JOIN pg_class t ON c.attrelid = t.oid "
            "JOIN pg_namespace n ON t.relnamespace = n.oid "
            "LEFT JOIN pg_description pgd ON pgd.objoid = t.oid AND pgd.objsubid = c.attnum "
            "WHERE n.nspname = %s AND t.relname = %s AND NOT c.attisdropped "
            "AND c.attnum >= 0 ORDER BY c.attnum ",
            (schema, table_name),
        )

        def _convert_type(ctype):
            # We don't keep a lot of type information, so e.g. char(5) gets turned into char
            # which defaults into char(1).
            return ctype if ctype != "character" else "character varying"

        # Do we need to make sure the PK has the same type + ordinal position here?
        pks = [pk for pk, _ in self.get_primary_keys(schema, table_name)]

        return [TableColumn(o, n, _convert_type(dt), (n in pks), c) for o, n, dt, c in results]

    @abstractmethod
    def initialize(self):
        """Does any required initialization of the engine"""

    @abstractmethod
    def lock_table(self, schema, table):
        """Acquire an exclusive lock on a given table, released when the transaction commits / rolls back."""


class ChangeEngine(SQLEngine, ABC):
    """An SQL engine that can perform change tracking on a set of tables."""

    @abstractmethod
    def get_tracked_tables(self):
        """
        :return: A list of (table_schema, table_name) that the engine currently tracks for changes
        """

    @abstractmethod
    def track_tables(self, tables):
        """
        Start engine-specific change tracking on a list of tables.

        :param tables: List of (table_schema, table_name) to start tracking
        """

    @abstractmethod
    def untrack_tables(self, tables):
        """
        Stop engine-specific change tracking on a list of tables and delete any pending changes.

        :param tables: List of (table_schema, table_name) to start tracking
        """

    @abstractmethod
    def has_pending_changes(self, schema):
        """
        Return True if the tracked schema has pending changes and False if it doesn't.
        """

    @abstractmethod
    def discard_pending_changes(self, schema, table=None):
        """
        Discard recorded pending changes for a tracked table or the whole schema
        """

    @abstractmethod
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

    @abstractmethod
    def get_changed_tables(self, schema):
        """
        List tracked tables that have pending changes

        :param schema: Schema to check for changes
        :return: List of tables with changed contents
        """

    @abstractmethod
    def get_change_key(self, schema: str, table: str) -> List[Tuple[str, str]]:
        """
        Returns the key used to identify a row in a change (list of column name, column type).
        If the tracked table has a PK, we use that; if it doesn't, the whole row is used.
        """


class ObjectEngine(ABC):
    """
    Routines for storing/applying objects as well as sharing them with other engines.
    """

    @abstractmethod
    def get_object_schema(self, object_id):
        """
        Get the schema of a given object, returned as a list of
        (ordinal, column_name, column_type, is_pk).

        :param object_id: ID of the object
        """

    @abstractmethod
    def get_object_size(self, object_id):
        """
        Return the on-disk footprint of this object, in bytes
        :param object_id: ID of the object
        """

    @abstractmethod
    def delete_objects(self, object_ids):
        """
        Delete one or more objects from the engine.

        :param object_ids: IDs of objects to delete
        """

    @abstractmethod
    def store_fragment(
        self, inserted, deleted, schema, table, source_schema, source_table, source_schema_spec
    ):
        """
        Store a fragment of a changed table in another table

        :param inserted: List of PKs that have been updated/inserted
        :param deleted: List of PKs that have been deleted
        :param schema: Schema to store the change in
        :param table: Table to store the change in
        :param source_schema: Schema the source table is located in
        :param source_table: Name of the source table
        :param source_schema_spec: Schema of the source table (optional)
        """

    @abstractmethod
    def apply_fragments(
        self,
        objects,
        target_schema,
        target_table,
        extra_quals=None,
        extra_qual_args=None,
        schema_spec=None,
        progress_every: Optional[int] = None,
    ):
        """
        Apply multiple fragments to a target table as a single-query batch operation.

        :param objects: List of tuples `(object_schema, object_table)` that the objects are stored in.
        :param target_schema: Schema to apply the fragment to
        :param target_table: Table to apply the fragment to
        :param extra_quals: Optional, extra SQL (Composable) clauses to filter new rows in the fragment on
            (e.g. SQL("a = %s"))
        :param extra_qual_args: Optional, a tuple of arguments to use with `extra_quals`
        :param schema_spec: Optional, list of (ordinal, column_name, column_type, is_pk).
            If not specified, uses the schema of target_table.
        :param progress_every: If set, will report the materialization progress via
            tqdm every `progress_every` objects.
        """

    @abstractmethod
    def upload_objects(self, objects, remote_engine):
        """
        Upload objects from the local cache to the remote engine

        :param objects: List of object IDs to upload
        :param remote_engine: A remote ObjectEngine to upload the objects to.
        """

    @abstractmethod
    def download_objects(self, objects, remote_engine):
        """
        Download objects from the remote engine to the local cache

        :param objects: List of object IDs to download
        :param remote_engine: A remote ObjectEngine to download the objects from.

        :return List of object IDs that were downloaded.
        """

    @abstractmethod
    def dump_object(self, object_id, stream, schema):
        """
        Dump an object into a series of SQL statements

        :param object_id: Object ID
        :param stream: Text stream to dump the object into
        :param schema: Schema the object lives in
        """

    @abstractmethod
    def store_object(
        self,
        object_id: str,
        source_query: Union[bytes, Composed, str, SQL],
        schema_spec: TableSchema,
        source_query_args: Optional[Sequence[Any]],
        overwrite: bool,
    ):
        """
        Stores a Splitgraph object using a source query in the actual format
        implemented by this engine.

        :param object_id: Name of the object
        :param source_query: SELECT query that produces data required by the object
        :param schema_spec: Schema of the source table
        :param source_query_args: Arguments to mogrify into the source query.
        :param overwrite: If True, will overwrite the object if it already exists.
        """
