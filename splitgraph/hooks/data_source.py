import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union, List, TYPE_CHECKING

from jsonschema import validate
from psycopg2.sql import SQL, Identifier

from splitgraph.core.common import get_temporary_table_id
from splitgraph.core.types import TableSchema

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine

Credentials = Dict[str, Any]
Params = Dict[str, Any]


class DataSource(ABC):
    params_schema: Dict[str, Any]
    credentials_schema: Dict[str, Any]

    def __init__(self, engine: "PostgresEngine", credentials: Credentials, params: Params):
        self.engine = engine

        validate(instance=credentials, schema=self.credentials_schema)
        validate(instance=params, schema=self.params_schema)

        self.credentials = credentials
        self.params = params

    @abstractmethod
    def introspect(self) -> Dict[str, TableSchema]:
        pass


# TODO
#
# These are basically FDW-specific options for each table that go into the CREATE FOREIGN TABLE
# statement -- in some cases we want the user to be able to define them (e.g. Mongo -- define
# the collection and the db where each table lives / ES -- index for each table) so they have
# to be in the JSONSchema. Sometimes we sometimes want to be able to  override the schema
# from the web UI, sometimes the "table options" are just a list of tables to import (e.g.
# postgres_fdw), sometimes the schema is injected from the in-db external repo schema and
# options are deserialized etc etc. Need to reconcile this with current mount handlers
# (e.g. Mongo/ES which don't work) and Socrata.
_table_options_schema = {
    "type": "object",
    "additionalProperties": {
        "schema": {"type": "object", "additionalProperties": {"type": "string"}},
        "options": {"type": "object", "additionalProperties": {"type": "string"}},
    },
}


class ForeignDataWrapperDataSource(DataSource, ABC):
    credentials_schema = {
        "type": "object",
        "properties": {"username": {"type": "string"}, "password": {"type": "string"},},
    }

    params_schema = {
        "type": "object",
        "properties": {"tables": _table_options_schema,},
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_user_options(self):
        return {}

    def get_server_options(self):
        return {}

    def get_table_options(self, table_name: str):
        return self.params.get("tables", {}).get("options", {}).get(table_name)

    def get_table_schema(self, table_name: str):
        return self.params.get("tables", {}).get("options", {}).get(table_name)

    def get_remote_schema_name(self) -> str:
        """Override this if the FDW supports IMPORT FOREIGN SCHEMA"""
        raise NotImplemented

    @abstractmethod
    def get_fdw_name(self):
        pass

    def mount(
        self, schema: str, tables: Optional[Union[List[str], Dict[str, TableSchema]]] = None,
    ):

        if tables is None:
            tables = []

        fdw = self.get_fdw_name()
        server_id = "%s_%s_server" % (schema, fdw)
        init_fdw(
            self.engine,
            server_id,
            self.get_fdw_name(),
            self.get_server_options(),
            self.get_user_options(),
            overwrite=False,
        )

        # Allow mounting tables into existing schemas
        self.engine.run_sql(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(schema)))

        self._create_foreign_tables(schema, server_id, tables)

    def _create_foreign_tables(self, schema, server_id, tables):
        if isinstance(tables, list):
            try:
                remote_schema = self.get_remote_schema_name()
            except NotImplemented:
                raise NotImplemented(
                    "The FDW does not support IMPORT FOREIGN SCHEMA! Pass a tables dictionary."
                )
            _import_foreign_schema(self.engine, schema, remote_schema, server_id, tables)
        else:
            for table_name, table_schema in tables.items():
                logging.info("Mounting table %s", table_name)

                query, args = create_foreign_table(
                    schema,
                    server_id,
                    table_name,
                    table_schema,
                    extra_options=self.get_table_options(table_name),
                )
                self.engine.run_sql(query, args)

    def introspect(self) -> Dict[str, TableSchema]:
        # Ability to override introspection by e.g. contacting the remote database without having
        # to mount the actual table. By default, just call out into mount()

        schema = get_temporary_table_id()
        try:
            self.mount(schema)
            result = {
                t: self.engine.get_full_table_schema(schema, t)
                for t in self.engine.get_all_tables(schema)
            }
            return result
        finally:
            self.engine.delete_schema(schema)


class PostgreSQLDataSource(ForeignDataWrapperDataSource):
    params_schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string"},
            "port": {"type": "integer"},
            "dbname": {"type": "string"},
            "remote_schema": {"type": "string"},
        },
        "required": ["host", "port", "dbname", "remote_schema"],
    }

    def get_server_options(self):
        return {
            "host": self.params["host"],
            "port": str(self.params["port"]),
            "dbname": self.params["dbname"],
            **self.params.get("extra_server_args", {}),
        }

    def get_user_options(self):
        return {"user": self.credentials["username"], "password": self.credentials["password"]}

    def get_table_options(self, table_name: str):
        return {"schema_name": self.params["remote_schema"]}

    def get_fdw_name(self):
        return "postgres_fdw"

    def get_remote_schema_name(self) -> str:
        return self.params["remote_schema"]


class MongoDataSource(ForeignDataWrapperDataSource):
    def get_server_options(self):
        return {
            "address": self.params["host"],
            "port": str(self.params["port"]),
        }

    def get_user_options(self):
        return {"username": self.credentials["username"], "password": self.credentials["password"]}

    def get_table_options(self, table_name: str):
        return {"database": None, "collection": None}

    def get_fdw_name(self):
        return "mongo_fdw"

    def get_table_schema(self):
        # TODO patch out the table schema (passed in)
        table_schema = table_options.get("schema", {})
        table_schema["_id"] = "NAME"


class MySQLDataSource(ForeignDataWrapperDataSource):
    params_schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string"},
            "port": {"type": "integer"},
            "remote_schema": {"type": "string"},
        },
        "required": ["host", "port", "remote_schema"],
    }

    def get_server_options(self):
        return {
            "host": self.params["host"],
            "port": str(self.params["port"]),
            **self.params.get("extra_server_args", {}),
        }

    def get_user_options(self):
        return {"username": self.credentials["username"], "password": self.credentials["password"]}

    def get_table_options(self, table_name: str):
        return {"schema_name": self.params["remote_schema"]}

    def get_fdw_name(self):
        return "mysql_fdw"

    def get_remote_schema_name(self) -> str:
        return self.params["remote_schema"]


class ElasticSearchDataSource(ForeignDataWrapperDataSource):
    """
    A dictionary of form
        `{"table_name":
            {"schema": {"col1": "type1"...},
             "index": <es index>,
             "type": <es doc_type, optional in ES7 and later>,
             "query_column": <column to pass ES query in>,
             "score_column": <column to return document score>,
             "scroll_size": <fetch size, default 1000>,
             "scroll_duration": <how long to hold the scroll context open for, default 10m>},
             ...}`
    """

    def get_server_options(self):
        return {
            "wrapper": "pg_es_fdw.ElasticsearchFDW",
            "host": self.params["host"],
            "port": self.params["port"],
            "username": self.credentials["username"],
            "password": self.credentials["password"],
        }

    # TODO support for table options (e.g. tables -> schema, options)
    def get_fdw_name(self):
        return "multicorn"


def init_fdw(
    engine: "PostgresEngine",
    server_id: str,
    wrapper: str,
    server_options: Optional[Dict[str, Union[str, None]]] = None,
    user_options: Optional[Dict[str, str]] = None,
    overwrite: bool = True,
) -> None:
    """
    Sets up a foreign data server on the engine.

    :param engine: PostgresEngine
    :param server_id: Name to call the foreign server, must be unique. Will be deleted if exists.
    :param wrapper: Name of the foreign data wrapper (must be installed as an extension on the engine)
    :param server_options: Dictionary of FDW options
    :param user_options: Dictionary of user options
    :param overwrite: If the server already exists, delete and recreate it.
    """
    from psycopg2.sql import Identifier, SQL

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
        create_mapping = SQL("CREATE USER MAPPING IF NOT EXISTS FOR PUBLIC SERVER {}").format(
            Identifier(server_id)
        )
        user_keys, user_vals = zip(*user_options.items())
        create_mapping += _format_options(user_keys)
        engine.run_sql(create_mapping, [str(v) for v in user_vals])


def _format_options(option_names):
    from psycopg2.sql import Identifier, SQL

    return (
        SQL(" OPTIONS (")
        + SQL(",").join(Identifier(o) + SQL(" %s") for o in option_names)
        + SQL(")")
    )


def _import_foreign_schema(
    engine: "PostgresEngine", mountpoint: str, remote_schema: str, server_id: str, tables: List[str]
) -> None:
    from psycopg2.sql import Identifier, SQL

    # Construct a query: import schema limit to (%s, %s, ...) from server mountpoint_server into mountpoint
    query = SQL("IMPORT FOREIGN SCHEMA {} ").format(Identifier(remote_schema))
    if tables:
        query += SQL("LIMIT TO (") + SQL(",").join(Identifier(t) for t in tables) + SQL(")")
    query += SQL("FROM SERVER {} INTO {}").format(Identifier(server_id), Identifier(mountpoint))
    engine.run_sql(query)


def create_foreign_table(
    schema: str,
    server: str,
    table_name: str,
    schema_spec: TableSchema,
    extra_options: Optional[Dict[str, str]] = None,
):
    table_options = extra_options or {}

    table_opts, table_optvals = zip(*table_options.items())

    query = SQL("CREATE FOREIGN TABLE {}.{} (").format(Identifier(schema), Identifier(table_name))
    query += SQL(",".join("{} %s " % col.pg_type for col in schema_spec)).format(
        *(Identifier(col.name) for col in schema_spec)
    )
    query += SQL(") SERVER {} OPTIONS (").format(Identifier(server))
    query += SQL(",").join(Identifier(o) + SQL(" %s") for o in table_opts) + SQL(");")
    args = list(table_optvals)
    for col in schema_spec:
        if col.comment:
            query += SQL("COMMENT ON COLUMN {}.{}.{} IS %s;").format(
                Identifier(schema), Identifier(table_name), Identifier(col.name),
            )
            args.append(col.comment)
    return query, args
