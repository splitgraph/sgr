import contextlib
import itertools
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple, cast

import requests
from psycopg2.sql import SQL, Identifier

from splitgraph.core.types import (
    Credentials,
    IntrospectionResult,
    Params,
    TableColumn,
    TableInfo,
    TableParams,
)
from splitgraph.engine.postgres.engine import _quote_ident
from splitgraph.hooks.data_source.base import LoadableDataSource

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine

LIST_TABLES_QUERY = """
SELECT * FROM
(SELECT
	tbl_name
FROM sqlite_master
WHERE type='table') t
JOIN pragma_table_info(tbl_name) s
ORDER BY 1,2;
"""


# based on https://stackoverflow.com/a/16696317
def download_file(url: str, local_fh: tempfile._TemporaryFileWrapper) -> int:
    total_bytes_written = 0
    with requests.get(url, stream=True, verify=os.environ["SSL_CERT_FILE"]) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=8192):
            total_bytes_written += local_fh.write(chunk)
    local_fh.flush()
    return total_bytes_written


@contextmanager
def minio_file(url: str) -> Generator[str, None, None]:
    with tempfile.NamedTemporaryFile(mode="wb", delete=True) as local_fh:
        download_file(url, local_fh)
        yield local_fh.name


def query_connection(
    con: sqlite3.Connection, sql: str, parameters: Optional[Dict[str, str]] = None
) -> List[Any]:
    with contextlib.closing(con.cursor()) as cursor:
        cursor.execute(sql, parameters or {})
        return cursor.fetchall()


@contextmanager
def db_from_minio(url: str) -> Generator[sqlite3.Connection, None, None]:
    with minio_file(url) as f:
        with contextlib.closing(sqlite3.connect(f)) as con:
            yield con


# partly based on https://stackoverflow.com/questions/1942586/comparison-of-database-column-types-in-mysql-postgresql-and-sqlite-cross-map
def sqlite_to_postgres_type(sqlite_type: str) -> str:
    if sqlite_type == "DATETIME":
        return "TIMESTAMP WITHOUT TIME ZONE"
    # from: https://www.sqlite.org/datatype3.html#determination_of_column_affinity
    # If the declared type contains the string "INT" then it is assigned INTEGER affinity.
    if "INT" in sqlite_type:
        return "INTEGER"
    # If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
    if "CHAR" in sqlite_type or "CLOB" in sqlite_type or "TEXT" in sqlite_type:
        return "TEXT"
    # If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
    if "BLOB" in sqlite_type:
        return "BLOB"
    # If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
    if "REAL" in sqlite_type or "FLOA" in sqlite_type or "DOUB" in sqlite_type:
        return "REAL"
    # Otherwise, the affinity is NUMERIC. TODO: Precision and scale
    return "NUMERIC"


def sqlite_connection_to_introspection_result(con: sqlite3.Connection) -> IntrospectionResult:
    schema = IntrospectionResult({})
    for (
        table_name,
        column_id,
        column_name,
        column_type,
        _notnull,
        _default_value,
        pk,
    ) in query_connection(con, LIST_TABLES_QUERY, {}):
        table = schema.get(table_name, ([], TableParams({})))
        assert isinstance(table, tuple)
        table[0].append(
            TableColumn(column_id + 1, column_name, sqlite_to_postgres_type(column_type), pk != 0)
        )
        schema[table_name] = table
    return schema


class SQLiteDataSource(LoadableDataSource):

    table_params_schema: Dict[str, Any] = {"type": "object", "properties": {}}

    params_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "url": {
                "type": "string",
                "description": "HTTP URL to the SQLite file",
                "title": "URL",
            }
        },
    }

    supports_mount = False
    supports_load = True
    supports_sync = False

    _icon_file = "sqlite.svg"  # TODO

    def _load(self, schema: str, tables: Optional[TableInfo] = None):
        with db_from_minio(str(self.params.get("url"))) as con:
            introspection_result = sqlite_connection_to_introspection_result(con)
            for table_name, table_definition in introspection_result.items():
                assert isinstance(table_definition, tuple)
                schema_spec = table_definition[0]
                self.engine.create_table(
                    schema=schema,
                    table=table_name,
                    schema_spec=schema_spec,
                )
                table_contents = query_connection(
                    con, "SELECT * FROM {}".format(_quote_ident(table_name))  #  nosec
                )
                self.engine.run_sql_batch(
                    SQL("INSERT INTO {0}.{1} ").format(Identifier(schema), Identifier(table_name))
                    + SQL(" VALUES (" + ",".join(itertools.repeat("%s", len(schema_spec))) + ")"),
                    # TODO: break this up into multiple batches for larger sqlite files
                    table_contents,
                )  # nosec

    def introspect(self) -> IntrospectionResult:
        with db_from_minio(str(self.params.get("url"))) as con:
            return sqlite_connection_to_introspection_result(con)

    def __init__(
        self,
        engine: "PostgresEngine",
        credentials: Credentials,
        params: Params,
        tables: Optional[TableInfo] = None,
    ):
        super().__init__(engine, credentials, params, tables)

    @classmethod
    def get_name(cls) -> str:
        return "SQLite files"

    @classmethod
    def get_description(cls) -> str:
        return "SQLite files"

    def get_remote_schema_name(self) -> str:
        # We ignore the schema name and use the bucket/prefix passed in the params instead.
        return "data"
