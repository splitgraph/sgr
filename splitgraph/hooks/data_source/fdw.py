import json
import logging
import re
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Tuple, cast

import psycopg2
from psycopg2.sql import SQL, Composed, Identifier

from splitgraph.config import DEFAULT_CHUNK_SIZE
from splitgraph.core.common import get_temporary_table_id
from splitgraph.core.repository import Repository
from splitgraph.core.types import (
    Credentials,
    IntrospectionResult,
    MountError,
    PreviewResult,
    SyncState,
    TableColumn,
    TableInfo,
    TableParams,
    TableSchema,
    dict_to_table_schema_params,
)
from splitgraph.engine import ResultShape
from splitgraph.engine.base import validate_type
from splitgraph.exceptions import (
    DataSourceError,
    TableNotFoundError,
    get_exception_name,
)
from splitgraph.hooks.data_source.base import (
    MountableDataSource,
    SyncableDataSource,
    get_ingestion_state,
    prepare_new_image,
)
from splitgraph.hooks.data_source.utils import merge_jsonschema
from splitgraph.ingestion.airbyte.data_source import delete_schema_at_end
from splitgraph.ingestion.common import add_timestamp_tags
from splitgraph.ingestion.singer.common import store_ingestion_state

if TYPE_CHECKING:
    from splitgraph.engine.postgres.psycopg import PsycopgEngine


class ForeignDataWrapperDataSource(MountableDataSource, SyncableDataSource, ABC):
    credentials_schema: Dict[str, Any] = {
        "type": "object",
    }

    params_schema: Dict[str, Any] = {
        "type": "object",
    }

    table_params_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "cursor_fields": {
                "type": "array",
                "items": {"type": "string"},
                "title": "Replication cursor",
                "description": "Column(s) to use as a replication cursor. "
                "This must be always increasing in the source table and is used to track "
                "which rows should be replicated.",
            }
        },
    }

    commandline_help: str = ""
    commandline_kwargs_help: str = ""

    supports_mount = True
    supports_load = True
    supports_sync = True

    @classmethod
    def from_commandline(cls, engine, commandline_kwargs) -> "ForeignDataWrapperDataSource":
        """Instantiate an FDW data source from commandline arguments."""
        from splitgraph.cloud.models import ExternalTableRequest

        # Normally these are supposed to be more user-friendly and FDW-specific (e.g.
        # not forcing the user to pass in a JSON with five levels of nesting etc).
        params = deepcopy(commandline_kwargs)

        # By convention, the "tables" object can be:
        #   * A list of tables to import
        #   * A dictionary table_name -> {"schema": schema, "options": table options}
        tables = params.pop("tables", None)
        if isinstance(tables, dict):
            tables = dict_to_table_schema_params(
                {k: ExternalTableRequest.parse_obj(v) for k, v in tables.items()}
            )

        # Extract credentials from the cmdline params
        credentials = Credentials({})
        for k in cast(Dict[str, Any], cls.credentials_schema.get("properties", {})).keys():
            if k in params:
                credentials[k] = params[k]

        result = cls(engine, credentials, params, tables)

        return result

    def get_user_options(self) -> Mapping[str, str]:
        return {}

    def get_server_options(self) -> Mapping[str, str]:
        return {}

    def get_table_options(
        self, table_name: str, tables: Optional[TableInfo] = None
    ) -> Dict[str, str]:
        tables = tables or self.tables

        if not isinstance(tables, dict):
            return {}

        return {
            k: str(v)
            for k, v in tables.get(table_name, cast(Tuple[TableSchema, TableParams], ({}, {})))[
                1
            ].items()
            if k != "cursor_fields"
        }

    def get_table_schema(self, table_name: str, table_schema: TableSchema) -> TableSchema:
        # Hook to override the table schema that's passed in
        return table_schema

    def get_remote_schema_name(self) -> str:
        """Override this if the FDW supports IMPORT FOREIGN SCHEMA"""
        raise NotImplementedError

    @abstractmethod
    def get_fdw_name(self):
        pass

    def mount(
        self,
        schema: str,
        tables: Optional[TableInfo] = None,
        overwrite: bool = True,
    ) -> Optional[List[MountError]]:
        self._validate_table_params(tables)
        tables = tables or self.tables or []

        fdw = self.get_fdw_name()
        server_id = "%s_%s_server" % (schema, fdw)
        init_fdw(
            self.engine,
            server_id,
            self.get_fdw_name(),
            self.get_server_options(),
            self.get_user_options(),
            overwrite=overwrite,
        )

        # Allow mounting tables into existing schemas
        self.engine.run_sql(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(schema)))

        errors = self._create_foreign_tables(schema, server_id, tables)
        if errors:
            for e in errors:
                logging.warning("Error mounting %s: %s: %s", e.table_name, e.error, e.error_text)
        return errors

    def _create_foreign_tables(
        self, schema: str, server_id: str, tables: TableInfo
    ) -> List[MountError]:
        if isinstance(tables, list):
            try:
                remote_schema = self.get_remote_schema_name()
            except NotImplementedError:
                raise NotImplementedError(
                    "The FDW does not support IMPORT FOREIGN SCHEMA! Pass a tables dictionary."
                )
            return import_foreign_schema(self.engine, schema, remote_schema, server_id, tables)
        else:
            for table_name, (table_schema, _) in tables.items():
                logging.info("Mounting table %s", table_name)
                query, args = create_foreign_table(
                    schema,
                    server_id,
                    table_name,
                    schema_spec=self.get_table_schema(table_name, table_schema),
                    extra_options=self.get_table_options(table_name, tables),
                )
                self.engine.run_sql(query, args)
            return []

    def introspect(self) -> IntrospectionResult:
        # Ability to override introspection by e.g. contacting the remote database without having
        # to mount the actual table. By default, just call out into mount()

        # Local import here since this data source gets imported by the commandline entry point
        import jsonschema

        from splitgraph.core.common import get_temporary_table_id

        tmp_schema = get_temporary_table_id()
        try:
            mount_errors = self.mount(tmp_schema) or []

            table_options = dict(self._get_foreign_table_options(tmp_schema))

            # Sanity check for adapters: validate the foreign table options that we get back
            # to make sure they're still appropriate.
            for v in table_options.values():
                jsonschema.validate(v, self.table_params_schema)

            result = IntrospectionResult(
                {
                    t: (
                        self.engine.get_full_table_schema(tmp_schema, t),
                        cast(TableParams, table_options.get(t, {})),
                    )
                    for t in self.engine.get_all_tables(tmp_schema)
                }
            )

            # Add errors to the result as well
            for error in mount_errors:
                result[error.table_name] = error
            return result
        finally:
            self.engine.rollback()
            self.engine.delete_schema(tmp_schema)
            self.engine.commit()

    def _preview_table(self, schema: str, table: str, limit: int = 10) -> List[Dict[str, Any]]:
        from splitgraph.engine import ResultShape

        result_json = cast(
            List[Dict[str, Any]],
            self.engine.run_sql(
                SQL(
                    "WITH p AS MATERIALIZED(SELECT * FROM {}.{} LIMIT %s) "
                    "SELECT row_to_json(p.*) FROM p"
                ).format(Identifier(schema), Identifier(table)),
                (limit,),
                return_shape=ResultShape.MANY_ONE,
            ),
        )
        return result_json

    def _get_foreign_table_options(self, schema: str) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Get a list of options the foreign tables in this schema were instantiated with
        :return: List of tables and their options
        """
        # We use this to suggest table options during introspection. With FDWs, we do this by
        # mounting the data source first (using IMPORT FOREIGN SCHEMA) and then scraping the
        # foreign table options it inferred.

        # Downstream FDWs can override this: if they remap some table options into different
        # FDW options (e.g. "remote_schema" on the data source side turns into "schema" on the
        # FDW side), they have to map them back in this routine (otherwise the introspection will
        # suggest "schema", which is wrong.

        # This is also used for type remapping, since table params can only be strings on PG.
        # TODO: this will lead to a bunch of serialization/deserialization code duplication
        #   in data sources. One potential solution is, at least in Multicorn-backed wrappers
        #   that we control, passing a JSON as a single table param instead.

        return cast(
            List[Tuple[str, Dict[str, str]]],
            self.engine.run_sql(
                "SELECT foreign_table_name, json_object_agg(option_name, option_value) "
                "FROM information_schema.foreign_table_options "
                "WHERE foreign_table_schema = %s GROUP BY foreign_table_name",
                (schema,),
            ),
        )

    def preview(self, tables: Optional[TableInfo]) -> PreviewResult:
        # Preview data in tables mounted by this FDW / data source

        # Local import here since this data source gets imported by the commandline entry point
        from splitgraph.core.common import get_temporary_table_id

        tmp_schema = get_temporary_table_id()
        try:
            # Seed the result with the tables that failed to mount
            result = PreviewResult(
                {e.table_name: e for e in self.mount(tmp_schema, tables=tables) or []}
            )

            # Commit so that errors don't cancel the mount
            self.engine.commit()
            for t in self.engine.get_all_tables(tmp_schema):
                if t in result:
                    continue
                try:
                    result[t] = self._preview_table(tmp_schema, t)
                except psycopg2.DatabaseError as e:
                    logging.warning("Could not preview data for table %s", t, exc_info=e)
                    result[t] = MountError(
                        table_name=t, error=get_exception_name(e), error_text=str(e).strip()
                    )
            return result
        finally:
            self.engine.rollback()
            self.engine.delete_schema(tmp_schema)
            self.engine.commit()

    def load(self, repository: "Repository", tables: Optional[TableInfo] = None) -> str:
        return self.sync(repository, image_hash=None, tables=tables, use_state=False)

    def _load(self, schema: str, tables: Optional[TableInfo] = None) -> None:
        pass

    def _mount_and_copy(
        self,
        schema: str,
        tables: Optional[TableInfo] = None,
        cursor_values: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> None:
        from splitgraph.core.common import get_temporary_table_id

        cursor_values = cursor_values or {}

        tmp_schema = get_temporary_table_id()
        try:
            errors = self.mount(tmp_schema, tables=tables)
            if errors:
                raise DataSourceError("Error mounting tables for ingestion")

            self.engine.commit()

            for t in self.engine.get_all_tables(tmp_schema):
                try:
                    self.engine.copy_table(
                        tmp_schema, t, schema, t, cursor_fields=cursor_values.get(t)
                    )
                    self.engine.commit()
                except psycopg2.DatabaseError as e:
                    logging.exception("Error ingesting table %s", t, exc_info=e)
                    raise DataSourceError(
                        "Error ingesting table %s: %s: %s" % (t, get_exception_name(e), e)
                    )
        finally:
            self.engine.rollback()
            self.engine.delete_schema(tmp_schema)
            self.engine.commit()

    def _sync(
        self,
        schema: str,
        state: Optional[SyncState] = None,
        tables: Optional[TableInfo] = None,
    ) -> SyncState:
        # We override the main sync() instead
        pass

    def _get_cursor_value(
        self, schema: str, table: str, cursor_fields: List[str]
    ) -> Optional[List[str]]:
        query = (
            SQL("SELECT ")
            + SQL(",").join(Identifier(p) for p in cursor_fields)
            + SQL(" FROM {}.{} ORDER BY ").format(Identifier(schema), Identifier(table))
            + SQL(",").join(Identifier(p) + SQL(" DESC") for p in cursor_fields)
        ) + SQL(" LIMIT 1")

        result = self.engine.run_sql(query, return_shape=ResultShape.ONE_MANY)
        return [str(r) for r in result] if result else None

    def sync(
        self,
        repository: Repository,
        image_hash: Optional[str] = None,
        tables: Optional[TableInfo] = None,
        use_state: bool = True,
    ) -> str:

        self._validate_table_params(tables)
        tables = tables or self.tables

        # Load ingestion state
        base_image, new_image_hash = prepare_new_image(
            repository, image_hash, comment=f"{self.get_name()} data load"
        )

        if use_state:
            # If we are doing sync-after-load, the previous image won't have an_sg_ingestion_state
            # table. Luckily, we can just query the previous image to find out the max values of
            # our cursors (this is going to be slower than storing the state in the image, but
            # then we don't want to do that after a load).
            state = get_ingestion_state(repository, image_hash)
            if state:
                cursor_values = state.get("cursor_values")
            elif base_image:
                with base_image.query_schema() as s:
                    cursor_values = self._get_cursor_values(s, tables, {})
        else:
            state = None
            cursor_values = None
        logging.info("Current ingestion state: %s", state)

        # Set up a staging schema for the data
        staging_schema = get_temporary_table_id()

        with delete_schema_at_end(repository.object_engine, staging_schema):
            repository.object_engine.delete_schema(staging_schema)
            repository.object_engine.create_schema(staging_schema)
            repository.commit_engines()

            self._mount_and_copy(
                staging_schema,
                tables,
                cursor_values=cursor_values,
            )

            logging.info("Storing tables as Splitgraph images")
            for table_name in repository.object_engine.get_all_tables(staging_schema):
                logging.info("Storing %s", table_name)
                new_schema = repository.object_engine.get_full_table_schema(
                    staging_schema, table_name
                )

                if base_image:
                    try:
                        current_schema = base_image.get_table(table_name).table_schema
                        # Compare schemas ignoring ordinals/comments
                        if [(c.name, c.pg_type, c.is_pk) for c in current_schema] != [
                            (c.name, c.pg_type, c.is_pk) for c in new_schema
                        ]:
                            raise AssertionError(
                                "Schema for %s changed! Old: %s, new: %s"
                                % (
                                    table_name,
                                    current_schema,
                                    new_schema,
                                )
                            )
                    except TableNotFoundError:
                        pass

                repository.objects.record_table_as_base(
                    repository,
                    table_name,
                    new_image_hash,
                    chunk_size=DEFAULT_CHUNK_SIZE,
                    source_schema=staging_schema,
                    source_table=table_name,
                    table_schema=new_schema,
                )

            # Store the ingestion state if we're running a sync (instead of a full reload).
            # This is so that we don't have a _sg_ingestion_state table hanging around
            # when doing something like a CSV upload.
            if use_state:
                new_state = {
                    "cursor_values": self._get_cursor_values(
                        staging_schema, tables, old_cursor_values=cursor_values or {}
                    )
                }
                store_ingestion_state(
                    repository,
                    new_image_hash,
                    current_state=state,
                    new_state=json.dumps(new_state) if new_state else "{}",
                )
            add_timestamp_tags(repository, new_image_hash)
            repository.commit_engines()

        return new_image_hash

    def _get_cursor_values(
        self, schema: str, tables: Optional[TableInfo], old_cursor_values: Dict[str, Dict[str, str]]
    ) -> Dict[str, Dict[str, str]]:
        cursor_values: Dict[str, Dict[str, str]] = {}
        cursor_fields: Dict[str, List[str]] = {}

        if tables and isinstance(tables, dict):
            for table, (_, table_params) in tables.items():
                if "cursor_fields" in table_params:
                    cursor_fields[table] = list(table_params["cursor_fields"])

        for table_name in self.engine.get_all_tables(schema):
            if table_name in cursor_fields:
                table_cursor_fields = cursor_fields[table_name]
                cursor_value = self._get_cursor_value(schema, table_name, table_cursor_fields)

                if cursor_value:
                    cursor_values[table_name] = dict(zip(table_cursor_fields, cursor_value))

                elif old_cursor_values.get(table_name):
                    cursor_values[table_name] = old_cursor_values[table_name]

        return cursor_values


def init_fdw(
    engine: "PsycopgEngine",
    server_id: str,
    wrapper: str,
    server_options: Optional[Mapping[str, Optional[str]]] = None,
    user_options: Optional[Mapping[str, str]] = None,
    role: Optional[str] = None,
    overwrite: bool = True,
) -> None:
    """
    Sets up a foreign data server on the engine.

    :param engine: PostgresEngine
    :param server_id: Name to call the foreign server, must be unique. Will be deleted if exists.
    :param wrapper: Name of the foreign data wrapper (must be installed as an extension on the engine)
    :param server_options: Dictionary of FDW options
    :param user_options: Dictionary of user options
    :param role: The name of the role for which the user mapping is created; defaults to public.
    :param overwrite: If the server already exists, delete and recreate it.
    """
    from psycopg2.sql import SQL, Identifier

    if overwrite:
        engine.run_sql(SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(server_id)))

    create_server = SQL("CREATE SERVER IF NOT EXISTS {} FOREIGN DATA WRAPPER {}").format(
        Identifier(server_id), Identifier(wrapper)
    )

    if server_options:
        server_keys, server_vals = zip(*server_options.items())
        create_server += _format_options(server_keys)
        engine.run_sql(create_server, [str(v) for v in server_vals])
    else:
        engine.run_sql(create_server)

    if user_options:
        create_mapping = SQL("CREATE USER MAPPING IF NOT EXISTS FOR {} SERVER {}").format(
            SQL("PUBLIC") if role is None else Identifier(role), Identifier(server_id)
        )
        user_keys, user_vals = zip(*user_options.items())
        create_mapping += _format_options(user_keys)
        engine.run_sql(create_mapping, [str(v) for v in user_vals])


def _format_options(option_names):
    from psycopg2.sql import SQL, Identifier

    return (
        SQL(" OPTIONS (")
        + SQL(",").join(Identifier(o) + SQL(" %s") for o in option_names)
        + SQL(")")
    )


def import_foreign_schema(
    engine: "PsycopgEngine",
    mountpoint: str,
    remote_schema: str,
    server_id: str,
    tables: List[str],
    options: Optional[Dict[str, str]] = None,
) -> List[MountError]:
    from psycopg2.sql import SQL, Identifier

    # Construct a query: import schema limit to (%s, %s, ...) from server mountpoint_server into mountpoint
    query = SQL("IMPORT FOREIGN SCHEMA {} ").format(Identifier(remote_schema))
    if tables:
        query += SQL("LIMIT TO (") + SQL(",").join(Identifier(t) for t in tables) + SQL(")")
    query += SQL("FROM SERVER {} INTO {}").format(Identifier(server_id), Identifier(mountpoint))
    args: List[str] = []

    if options:
        query += _format_options(options.keys())
        args.extend(options.values())

    engine.run_sql(query, args)

    # Some of our FDWs use the PG notices as a side channel to pass errors and information back
    # to the user, as IMPORT FOREIGN SCHEMA is all-or-nothing. If some of the tables failed to
    # mount, we return those + the reasons.
    import_errors: List[MountError] = []
    if engine.notices:
        for notice in engine.notices:
            match = re.match(r".*SPLITGRAPH: (.*)\n", notice)
            if not match:
                continue
            notice_j = json.loads(match.group(1))
            import_errors.append(
                MountError(
                    table_name=notice_j["table_name"],
                    error=notice_j["error"],
                    error_text=notice_j["error_text"],
                )
            )

    return import_errors


def _identifier(c: str) -> Identifier:
    """
    Create a psycopg2 composable Identifier escaping percentage signs in column names. These
    cause issues when using argument interpolation, as psycopg2 treats them as arguments.
    """
    return Identifier(c.replace("%", "%%").replace("{", "{{").replace("}", "}}"))


def create_foreign_table(
    schema: str,
    server: str,
    table_name: str,
    schema_spec: TableSchema,
    extra_options: Optional[Dict[str, str]] = None,
) -> Tuple[Composed, List[str]]:
    table_options = extra_options or {}

    query = SQL("CREATE FOREIGN TABLE {}.{} (").format(Identifier(schema), Identifier(table_name))
    query += SQL(",".join("{} %s " % validate_type(col.pg_type) for col in schema_spec)).format(
        *(_identifier(col.name) for col in schema_spec)
    )
    query += SQL(") SERVER {}").format(Identifier(server))

    args: List[str] = []
    if table_options:
        table_opts, table_optvals = zip(*table_options.items())
        query += SQL(" OPTIONS(")
        query += SQL(",").join(_identifier(o) + SQL(" %s") for o in table_opts) + SQL(");")
        args.extend(table_optvals)

    for col in schema_spec:
        if col.comment:
            query += SQL("COMMENT ON COLUMN {}.{}.{} IS %s;").format(
                Identifier(schema),
                Identifier(table_name),
                _identifier(col.name),
            )
            args.append(col.comment)
    return query, args


class PostgreSQLDataSource(ForeignDataWrapperDataSource):
    @classmethod
    def get_name(cls) -> str:
        return "PostgreSQL"

    @classmethod
    def get_description(cls) -> str:
        return (
            "Data source for PostgreSQL databases that supports live querying, "
            "based on postgres_fdw"
        )

    credentials_schema = {
        "type": "object",
        "properties": {
            "username": {
                "type": "string",
                "title": "Username",
            },
            "password": {
                "type": "string",
                "title": "Password",
            },
        },
        "required": ["username", "password"],
    }

    params_schema = {
        "type": "object",
        "properties": {
            "host": {
                "type": "string",
                "title": "Host",
                "description": "Hostname or IP address of the PostgreSQL instance",
            },
            "port": {"type": "integer", "title": "Port", "description": "Port of the database"},
            "dbname": {"type": "string", "title": "Database", "description": "Database name"},
            "remote_schema": {
                "type": "string",
                "title": "Schema name",
                "description": "Schema name to import data from",
                "default": "public",
            },
            "use_remote_estimate": {
                "type": "boolean",
                "default": False,
                "title": "Use remote EXPLAIN",
                "description": "Issue remote EXPLAIN commands to obtain cost estimates",
            },
            "fetch_size": {
                "type": "integer",
                "title": "Fetch size",
                "description": "Number of rows from the remote server to get in each fetch operation",
                "default": 10000,
            },
        },
        "required": ["host", "port", "dbname", "remote_schema"],
    }

    table_params_schema = merge_jsonschema(
        ForeignDataWrapperDataSource.table_params_schema,
        {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "title": "Table name"},
                "schema_name": {"type": "string", "title": "Schema name"},
            },
            "required": [],
        },
    )

    commandline_help: str = """Mount a Postgres database.

Mounts a schema on a remote Postgres database as a set of foreign tables locally."""

    commandline_kwargs_help: str = """dbname: Database name (required)
remote_schema: Remote schema name (required)
extra_server_args: Dictionary of extra arguments to pass to the foreign server
tables: Tables to mount (default all). If a list, then will use IMPORT FOREIGN SCHEMA.
If a dictionary, must have the format
    {"table_name": {"schema": {"col_1": "type_1", ...},
                    "options": {[get passed to CREATE FOREIGN TABLE]}}}.
    """

    _icon_file = "postgresql.svg"

    def get_server_options(self):
        return {
            "host": self.params["host"],
            "port": str(self.params["port"]),
            "dbname": self.params["dbname"],
            "use_remote_estimate": "true" if self.params.get("use_remote_estimate") else "false",
            "fetch_size": int(self.params.get("fetch_size", 10000)),
            **self.params.get("extra_server_args", {}),
        }

    def get_user_options(self):
        return {"user": self.credentials["username"], "password": self.credentials["password"]}

    def get_table_options(self, table_name: str, tables: Optional[TableInfo] = None):
        options = super().get_table_options(table_name, tables)
        options["schema_name"] = options.get("schema_name", self.params["remote_schema"])
        return options

    def get_fdw_name(self):
        return "postgres_fdw"

    def get_remote_schema_name(self) -> str:
        return str(self.params["remote_schema"])


class MongoDataSource(ForeignDataWrapperDataSource):
    credentials_schema = {
        "type": "object",
        "properties": {
            "username": {"type": "string", "title": "Username"},
            "password": {"type": "string", "title": "Password"},
        },
        "required": ["username", "password"],
    }

    params_schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string", "title": "Host"},
            "port": {"type": "integer", "title": "Port"},
        },
        "required": ["host", "port"],
    }

    table_params_schema = merge_jsonschema(
        ForeignDataWrapperDataSource.table_params_schema,
        {
            "type": "object",
            "properties": {
                "database": {"type": "string", "title": "Database name"},
                "collection": {"type": "string", "title": "Collection name"},
            },
            "required": ["database", "collection"],
        },
    )

    commandline_help = """Mount a Mongo database.

Mounts one or more collections on a remote Mongo database as a set of foreign tables locally."""

    commandline_kwargs_help = """tables: A dictionary of form
```
{
    "table_name": {
        "schema": {"col1": "type1"...},
        "options": {"database": <dbname>, "collection": <collection>}
    }
}
```
"""

    _icon_file = "mongodb.svg"

    @classmethod
    def get_name(cls) -> str:
        return "MongoDB"

    @classmethod
    def get_description(cls) -> str:
        return "Data source for MongoDB databases that supports live querying, based on mongo_fdw"

    def get_server_options(self):
        return {
            "address": self.params["host"],
            "port": str(self.params["port"]),
        }

    def get_user_options(self):
        return {"username": self.credentials["username"], "password": self.credentials["password"]}

    def get_fdw_name(self):
        return "mongo_fdw"

    def get_table_schema(self, table_name, table_schema):
        # Add the "_id" column to the schema if it's not already there.
        if any(c.name == "_id" for c in table_schema):
            return table_schema

        return table_schema + [TableColumn(table_schema[-1].ordinal + 1, "_id", "NAME", False)]


class MySQLDataSource(ForeignDataWrapperDataSource):
    credentials_schema = {
        "type": "object",
        "properties": {
            "username": {"type": "string", "title": "Username"},
            "password": {"type": "string", "title": "Password"},
        },
        "required": ["username", "password"],
    }

    params_schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string", "title": "Host"},
            "port": {"type": "integer", "title": "Port"},
            "dbname": {"type": "string", "title": "Database name"},
        },
        "required": ["host", "port", "dbname"],
    }

    commandline_help: str = """Mount a MySQL database.

Mounts a schema on a remote MySQL database as a set of foreign tables locally."""

    commandline_kwargs_help: str = """dbname: Remote MySQL database name (required)
tables: Tables to mount (default all). If a list, then will use IMPORT FOREIGN SCHEMA.
If a dictionary, must have the format
    {"table_name": {"schema": {"col_1": "type_1", ...},
                    "options": {[get passed to CREATE FOREIGN TABLE]}}}.
        """

    _icon_file = "mysql.svg"

    @classmethod
    def get_name(cls) -> str:
        return "MySQL"

    @classmethod
    def get_description(cls) -> str:
        return "Data source for MySQL databases that supports live querying, based on mysql_fdw"

    def get_server_options(self):
        return {
            "host": self.params["host"],
            "port": str(self.params["port"]),
            **self.params.get("extra_server_args", {}),
        }

    def get_user_options(self):
        return {"username": self.credentials["username"], "password": self.credentials["password"]}

    def get_table_options(self, table_name: str, tables: Optional[TableInfo] = None):
        options = super().get_table_options(table_name, tables)
        options["dbname"] = options.get("dbname", self.params["dbname"])
        return options

    def get_fdw_name(self):
        return "mysql_fdw"

    def get_remote_schema_name(self) -> str:
        return str(self.params["dbname"])


class ElasticSearchDataSource(ForeignDataWrapperDataSource):
    credentials_schema = {
        "type": "object",
        "properties": {
            "username": {"type": "string", "title": "Username"},
            "password": {"type": "string", "title": "Password"},
        },
    }

    params_schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string", "title": "Host"},
            "port": {"type": "integer", "title": "Port"},
        },
        "required": ["host", "port"],
    }

    table_params_schema = merge_jsonschema(
        ForeignDataWrapperDataSource.table_params_schema,
        {
            "type": "object",
            "properties": {
                "index": {
                    "type": "string",
                    "title": "Index name or pattern",
                    "description": 'ES index name or pattern to use, for example, "events-*"',
                },
                "type": {
                    "type": "string",
                    "title": "doc_type (ES6 or earlier)",
                    "description": "Pre-ES7 doc_type, not required in ES7 or later",
                },
                "query_column": {
                    "type": "string",
                    "title": "Query column name",
                    "description": "Allows you to do `WHERE query = '{" "match" ": ...}'`",
                    "default": "query",
                },
                "score_column": {
                    "type": "string",
                    "title": "Score column name",
                    "description": "Name of the column with the document score",
                },
                "scroll_size": {
                    "type": "integer",
                    "title": "Scroll size",
                    "description": "Fetch size, default 1000",
                    "default": 1000,
                },
                "scroll_duration": {
                    "type": "string",
                    "title": "Scroll duration",
                    "description": "How long to hold the scroll context open for, default 10m",
                    "default": "10m",
                },
            },
            "required": ["index"],
        },
    )

    commandline_help = """Mount an ElasticSearch instance.

Mount a set of tables proxying to a remote ElasticSearch index.

This uses a fork of postgres-elasticsearch-fdw behind the scenes. You can add a column
`query` to your table and set it as `query_column` to pass advanced ES queries and aggregations.
For example:

\b
```
$ sgr mount elasticsearch target_schema -c elasticsearch:9200 -o@- <<EOF
    {
      "tables": {
        "table_1": {
          "schema": {
            "id": "text",
            "@timestamp": "timestamp",
            "query": "text",
            "col_1": "text",
            "col_2": "boolean"
          },
          "options": {
              "index": "index-pattern*",
              "rowid_column": "id",
              "query_column": "query"
          }
        }
      }
    }
EOF
\b
```
"""

    _icon_file = "elasticsearch.svg"

    @classmethod
    def get_name(cls) -> str:
        return "Elasticsearch"

    @classmethod
    def get_description(cls) -> str:
        return (
            "Data source for Elasticsearch indexes that supports live querying, "
            "based on pg_es_fdw"
        )

    def get_server_options(self):
        result = {
            "wrapper": "pg_es_fdw.ElasticsearchFDW",
            "host": self.params["host"],
            "port": self.params["port"],
        }

        for key in ["username", "password"]:
            if key in self.credentials:
                result[key] = self.credentials[key]

        return result

    def get_fdw_name(self):
        return "multicorn"
