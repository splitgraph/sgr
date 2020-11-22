import logging
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Optional, Mapping, Dict, List, Any, cast, Union, TYPE_CHECKING

import psycopg2
from psycopg2.sql import SQL, Identifier

from splitgraph.core.types import dict_to_tableschema, TableSchema, TableColumn
from splitgraph.hooks.data_source.base import (
    Credentials,
    Params,
    TableInfo,
    PreviewResult,
    MountableDataSource,
)

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine


_table_options_schema = {
    "type": "object",
    "additionalProperties": {
        "options": {"type": "object", "additionalProperties": {"type": "string"}},
    },
}


class ForeignDataWrapperDataSource(MountableDataSource, ABC):
    credentials_schema = {
        "type": "object",
        "properties": {"username": {"type": "string"}, "password": {"type": "string"}},
        "required": ["username", "password"],
    }

    params_schema = {
        "type": "object",
        "properties": {"tables": _table_options_schema},
    }

    commandline_help: str = ""
    commandline_kwargs_help: str = ""

    supports_mount = True

    def __init__(
        self,
        engine: "PostgresEngine",
        credentials: Optional[Credentials] = None,
        params: Optional[Params] = None,
    ):
        self.tables: Optional[TableInfo] = None
        super().__init__(engine, credentials or {}, params or {})

    @classmethod
    def from_commandline(cls, engine, commandline_kwargs) -> "ForeignDataWrapperDataSource":
        """Instantiate an FDW data source from commandline arguments."""
        # Normally these are supposed to be more user-friendly and FDW-specific (e.g.
        # not forcing the user to pass in a JSON with five levels of nesting etc).
        params = deepcopy(commandline_kwargs)

        # By convention, the "tables" object can be:
        #   * A list of tables to import
        #   * A dictionary table_name -> {"schema": schema, **mount_options}
        table_kwargs = params.pop("tables", None)
        tables: Optional[TableInfo]

        if isinstance(table_kwargs, dict):
            tables = dict_to_tableschema(
                {t: to["schema"] for t, to in table_kwargs.items() if "schema" in to}
            )
            params["tables"] = {
                t: {"options": to["options"]} for t, to in table_kwargs.items() if "options" in to
            }
        elif isinstance(table_kwargs, list):
            tables = table_kwargs
        else:
            tables = None

        credentials = {
            "username": params.pop("username"),
            "password": params.pop("password"),
        }
        result = cls(engine, credentials, params)
        result.tables = tables
        return result

    def get_user_options(self) -> Mapping[str, str]:
        return {}

    def get_server_options(self) -> Mapping[str, str]:
        return {}

    def get_table_options(self, table_name: str) -> Mapping[str, str]:
        return {
            k: str(v)
            for k, v in self.params.get("tables", {}).get(table_name, {}).get("options", {}).items()
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
        self, schema: str, tables: Optional[TableInfo] = None, overwrite: bool = True,
    ):
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

        self._create_foreign_tables(schema, server_id, tables)

    def _create_foreign_tables(self, schema, server_id, tables):
        if isinstance(tables, list):
            try:
                remote_schema = self.get_remote_schema_name()
            except NotImplementedError:
                raise NotImplementedError(
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
                    schema_spec=self.get_table_schema(table_name, table_schema),
                    extra_options=self.get_table_options(table_name),
                )
                self.engine.run_sql(query, args)

    def introspect(self) -> Dict[str, TableSchema]:
        # Ability to override introspection by e.g. contacting the remote database without having
        # to mount the actual table. By default, just call out into mount()

        # Local import here since this data source gets imported by the commandline entry point
        from splitgraph.core.common import get_temporary_table_id

        tmp_schema = get_temporary_table_id()
        try:
            self.mount(tmp_schema)
            result = {
                t: self.engine.get_full_table_schema(tmp_schema, t)
                for t in self.engine.get_all_tables(tmp_schema)
            }
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
                SQL("SELECT row_to_json(t.*) FROM {}.{} t LIMIT %s").format(
                    Identifier(schema), Identifier(table)
                ),
                (limit,),
                return_shape=ResultShape.MANY_ONE,
            ),
        )
        return result_json

    def preview(self, schema: Dict[str, TableSchema]) -> PreviewResult:
        # Preview data in tables mounted by this FDW / data source

        # Local import here since this data source gets imported by the commandline entry point
        from splitgraph.core.common import get_temporary_table_id

        tmp_schema = get_temporary_table_id()
        try:
            self.mount(tmp_schema, tables=schema)
            # Commit so that errors don't cancel the mount
            self.engine.commit()
            result: Dict[str, Union[str, List[Dict[str, Any]]]] = {}
            for t in self.engine.get_all_tables(tmp_schema):
                try:
                    result[t] = self._preview_table(tmp_schema, t)
                except psycopg2.DatabaseError as e:
                    logging.warning("Could not preview data for table %s", t, exc_info=e)
                    result[t] = str(e)
            return result
        finally:
            self.engine.rollback()
            self.engine.delete_schema(tmp_schema)
            self.engine.commit()


def init_fdw(
    engine: "PostgresEngine",
    server_id: str,
    wrapper: str,
    server_options: Optional[Mapping[str, Optional[str]]] = None,
    user_options: Optional[Mapping[str, str]] = None,
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

    query = SQL("CREATE FOREIGN TABLE {}.{} (").format(Identifier(schema), Identifier(table_name))
    query += SQL(",".join("{} %s " % col.pg_type for col in schema_spec)).format(
        *(Identifier(col.name) for col in schema_spec)
    )
    query += SQL(") SERVER {}").format(Identifier(server))

    args: List[str] = []
    if table_options:
        table_opts, table_optvals = zip(*table_options.items())
        query += SQL(" OPTIONS(")
        query += SQL(",").join(Identifier(o) + SQL(" %s") for o in table_opts) + SQL(");")
        args.extend(table_optvals)

    for col in schema_spec:
        if col.comment:
            query += SQL("COMMENT ON COLUMN {}.{}.{} IS %s;").format(
                Identifier(schema), Identifier(table_name), Identifier(col.name),
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

    params_schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string"},
            "port": {"type": "integer"},
            "dbname": {"type": "string"},
            "remote_schema": {"type": "string"},
            "tables": _table_options_schema,
        },
        "required": ["host", "port", "dbname", "remote_schema"],
    }

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
        return str(self.params["remote_schema"])


class MongoDataSource(ForeignDataWrapperDataSource):
    params_schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string"},
            "port": {"type": "integer"},
            "tables": {
                "type": "object",
                "additionalProperties": {
                    "options": {
                        "type": "object",
                        "properties": {
                            "db": {"type": "string"},
                            "coll": {"type": "string"},
                            "required": ["db", "coll"],
                        },
                    },
                    "required": ["options"],
                },
            },
        },
        "required": ["host", "port", "tables"],
    }

    commandline_help = """Mount a Mongo database.

Mounts one or more collections on a remote Mongo database as a set of foreign tables locally."""

    commandline_kwargs_help = """tables: A dictionary of form
```
{
    "table_name": {
        "schema": {"col1": "type1"...},
        "options": {"db": <dbname>, "coll": <collection>} 
    } 
}
```
"""

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

    def get_table_options(self, table_name: str):
        try:
            table_params = self.params["tables"][table_name]["options"]
        except KeyError:
            raise ValueError("No options specified for table %s!" % table_name)

        return {"database": table_params["db"], "collection": table_params["coll"]}

    def get_fdw_name(self):
        return "mongo_fdw"

    def get_table_schema(self, table_name, table_schema):
        # Add the "_id" column to the schema if it's not already there.
        if any(c.name == "_id" for c in table_schema):
            return table_schema

        return table_schema + [TableColumn(table_schema[-1].ordinal + 1, "_id", "NAME", False)]


class MySQLDataSource(ForeignDataWrapperDataSource):
    params_schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string"},
            "port": {"type": "integer"},
            "remote_schema": {"type": "string"},
            "tables": _table_options_schema,
        },
        "required": ["host", "port", "remote_schema"],
    }

    commandline_help: str = """Mount a MySQL database.

Mounts a schema on a remote MySQL database as a set of foreign tables locally."""

    commandline_kwargs_help: str = """remote_schema: Remote schema name (required)
tables: Tables to mount (default all). If a list, then will use IMPORT FOREIGN SCHEMA.
If a dictionary, must have the format
    {"table_name": {"schema": {"col_1": "type_1", ...},
                    "options": {[get passed to CREATE FOREIGN TABLE]}}}.
        """

    @classmethod
    def get_name(cls) -> str:
        return "MySQL"

    @classmethod
    def get_description(cls) -> str:
        return "Data source for MySQL databases that supports live querying, " "based on mysql_fdw"

    def get_server_options(self):
        return {
            "host": self.params["host"],
            "port": str(self.params["port"]),
            **self.params.get("extra_server_args", {}),
        }

    def get_user_options(self):
        return {"username": self.credentials["username"], "password": self.credentials["password"]}

    def get_table_options(self, table_name: str):
        return {"dbname": self.params["remote_schema"]}

    def get_fdw_name(self):
        return "mysql_fdw"

    def get_remote_schema_name(self) -> str:
        return str(self.params["remote_schema"])


class ElasticSearchDataSource(ForeignDataWrapperDataSource):
    credentials_schema = {
        "type": "object",
        "properties": {
            "username": {"type": ["string", "null"]},
            "password": {"type": ["string", "null"]},
        },
    }

    params_schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string"},
            "port": {"type": "integer"},
            "tables": {
                "type": "object",
                "additionalProperties": {
                    "options": {
                        "type": "object",
                        "properties": {
                            "index": {
                                "type": "string",
                                "description": 'ES index name or pattern to use, for example, "events-*"',
                            },
                            "type": {
                                "type": "string",
                                "description": "Pre-ES7 doc_type, not required in ES7 or later",
                            },
                            "query_column": {
                                "type": "string",
                                "description": "Name of the column to use to pass queries in",
                            },
                            "score_column": {
                                "type": "string",
                                "description": "Name of the column with the document score",
                            },
                            "scroll_size": {
                                "type": "integer",
                                "description": "Fetch size, default 1000",
                            },
                            "scroll_duration": {
                                "type": "string",
                                "description": "How long to hold the scroll context open for, default 10m",
                            },
                        },
                    },
                },
            },
        },
        "required": ["host", "port", "tables"],
    }

    commandline_help = """Mount an ElasticSearch instance.

Mount a set of tables proxying to a remote ElasticSearch index.

This uses a fork of postgres-elasticsearch-fdw behind the scenes. You can add a column
`query` to your table and set it as `query_column` to pass advanced ES queries and aggregations.
For example:

```
sgr mount elasticsearch -c elasticsearch:9200 -o@- <<EOF
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
```
"""

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
        return {
            "wrapper": "pg_es_fdw.ElasticsearchFDW",
            "host": self.params["host"],
            "port": self.params["port"],
            "username": self.credentials["username"],
            "password": self.credentials["password"],
        }

    def get_fdw_name(self):
        return "multicorn"
