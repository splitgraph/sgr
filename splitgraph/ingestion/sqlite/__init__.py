import contextlib
import itertools
import math
import os
import re
import sqlite3
import tempfile
from contextlib import contextmanager
from datetime import datetime
from numbers import Number
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Union

import requests
from psycopg2.sql import SQL, Identifier

from splitgraph.core.types import (
    Credentials,
    IntrospectionResult,
    MountError,
    Params,
    PreviewResult,
    TableColumn,
    TableInfo,
    TableParams,
    TableSchema,
)
from splitgraph.engine.postgres.engine import _quote_ident
from splitgraph.hooks.data_source.base import LoadableDataSource, PreviewableDataSource

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

RE_SINGLE_PARAM_TYPE = re.compile(r"^([A-Z ]+)\(\s*([0-9]+)\s*\)$")
RE_DOUBLE_PARAM_TYPE = re.compile(r"^([A-Z ]+)\(\s*([0-9]+)\s*,\s*([0-9]+)\s*\)$")
# from: https://www.sqlite.org/datatype3.html#affinity_name_examples
VARCHAR_ALIASES = {
    "CHARACTER",
    "VARCHAR",
    "VARYING CHARACTER",
    "NCHAR",
    "NATIVE CHARACTER",
    "NVARCHAR",
}

# based on https://stackoverflow.com/a/16696317
def download_file(url: str, local_fh: tempfile._TemporaryFileWrapper) -> int:
    total_bytes_written = 0
    with requests.get(url, stream=True, verify=os.environ.get("SSL_CERT_FILE", True)) as r:
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
            con.row_factory = sqlite3.Row
            yield con


# partly based on https://stackoverflow.com/questions/1942586/comparison-of-database-column-types-in-mysql-postgresql-and-sqlite-cross-map
def sqlite_to_postgres_type(raw_sqlite_type: str) -> str:
    sqlite_type = raw_sqlite_type.upper()
    match = re.search(RE_SINGLE_PARAM_TYPE, sqlite_type)
    if match:
        (type_name, param) = match.groups()
        if type_name in VARCHAR_ALIASES:
            type_name = "VARCHAR"
        return "%s(%s)" % (type_name, param)
    match = re.search(RE_DOUBLE_PARAM_TYPE, sqlite_type)
    if match:
        # Only NUMERIC and DECIMAL have double parameters, which both exist
        # in PostgreSQL as well.
        return "%s(%s,%s)" % match.groups()
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
        return "BYTEA"
    # If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
    if "REAL" in sqlite_type or "FLOA" in sqlite_type or "DOUB" in sqlite_type:
        return "REAL"
    # Otherwise, the affinity is NUMERIC.
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


BINARY_DATA_MESSAGE = "[binary data]"


def sql_quote_str(s: str) -> str:
    return s.replace("'", "''")


def emit_value(value: Any) -> str:
    if value is None:
        return "NULL"

    if isinstance(value, float):
        if math.isnan(value):
            return "NULL"
        return f"{value:.20f}"

    if isinstance(value, Number) and not isinstance(value, bool):
        return str(value)

    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"

    quoted = sql_quote_str(str(value))
    return f"'{quoted}'"


def sanitize_preview_row(row: sqlite3.Row) -> Dict[str, Any]:
    return {k: row[k] if type(row[k]) != bytes else BINARY_DATA_MESSAGE for k in row.keys()}


def get_preview_rows(
    con: sqlite3.Connection, table_name: str, limit: Optional[int] = 10
) -> Union[MountError, List[Dict[str, Any]]]:
    # TODO: catch errors and return them as MountErrors
    return [
        sanitize_preview_row(row)
        for row in query_connection(
            con, "SELECT * FROM {} LIMIT {}".format(_quote_ident(table_name), limit)  #  nosec
        )
    ]


SQLITE_IMPLICIT_ROWID_COLUMN_NAME = "ROWID"
# copy 1000 rows in a single iteration
DEFAULT_BATCH_SIZE = 1000


def get_select_query(
    table_name: str,
    primary_keys: List[str],
    end_of_last_batch: Optional[sqlite3.Row],
    batch_size: int,
):
    effective_pks = primary_keys if len(primary_keys) > 0 else [SQLITE_IMPLICIT_ROWID_COLUMN_NAME]
    pk_column_list = ", ".join([_quote_ident(col) for col in effective_pks])
    where_clause = "true"
    if end_of_last_batch is not None:
        last_batch_end_tuple = ", ".join(
            [emit_value(end_of_last_batch[col]) for col in effective_pks]
        )
        where_clause = f"({pk_column_list}) > ({last_batch_end_tuple})"
    return "SELECT {}* FROM {} WHERE {} ORDER BY {} ASC LIMIT {}".format(  #  nosec
        # add the implicit rowid column to the select if no explicit primary
        # key columns exist on table, based on: https://www.sqlite.org/withoutrowid.html
        f"{SQLITE_IMPLICIT_ROWID_COLUMN_NAME}, " if len(primary_keys) == 0 else "",
        _quote_ident(table_name),
        where_clause,
        pk_column_list,
        batch_size,
    )  #  nosec


class SQLiteDataSource(LoadableDataSource, PreviewableDataSource):

    table_params_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "url": {
                "type": "string",
                "description": "HTTP URL to the SQLite file",
                "title": "URL",
            },
        },
    }

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
    _icon_file = "sqlite.svg"

    def _get_url(self, tables: Optional[TableInfo] = None):
        url = str(self.params.get("url"))
        if type(tables) == dict and len(tables) == 1:
            assert isinstance(tables, dict)
            for (_schema, table_params) in tables.values():
                url = table_params.get("url", url)
        return url

    def _batched_copy(
        self,
        con: sqlite3.Connection,
        schema: str,
        table_name: str,
        schema_spec: TableSchema,
        batch_size: int,
    ) -> int:
        primary_keys = [col.name for col in schema_spec if col.is_pk]
        last_batch_row_count = batch_size
        end_of_last_batch: Optional[sqlite3.Row] = None
        total_row_count = 0
        while last_batch_row_count == batch_size:
            table_contents = query_connection(
                con, get_select_query(table_name, primary_keys, end_of_last_batch, batch_size)
            )
            last_batch_row_count = len(table_contents)
            end_of_last_batch = None if last_batch_row_count == 0 else table_contents[-1]
            total_row_count += last_batch_row_count
            insert_table_contents = (
                table_contents if len(primary_keys) > 0 else [row[1:] for row in table_contents]
            )
            self.engine.run_sql_batch(
                SQL("INSERT INTO {0}.{1} ").format(Identifier(schema), Identifier(table_name))
                + SQL(" VALUES (" + ",".join(itertools.repeat("%s", len(schema_spec))) + ")"),
                insert_table_contents,
            )  # nosec
        return total_row_count

    def _load(self, schema: str, tables: Optional[TableInfo] = None):
        with db_from_minio(self._get_url(tables)) as con:
            introspection_result = sqlite_connection_to_introspection_result(con)
            for table_name, table_definition in introspection_result.items():
                assert isinstance(table_definition, tuple)
                schema_spec = table_definition[0]
                self.engine.create_table(
                    schema=schema,
                    table=table_name,
                    schema_spec=schema_spec,
                )
                self._batched_copy(con, schema, table_name, schema_spec, DEFAULT_BATCH_SIZE)

    def introspect(self) -> IntrospectionResult:
        with db_from_minio(str(self._get_url())) as con:
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

    def preview(self, tables: Optional[TableInfo]) -> PreviewResult:
        result = PreviewResult({})
        if type(tables) == dict:
            assert isinstance(tables, dict)
            with db_from_minio(self._get_url(tables)) as con:
                result = PreviewResult(
                    {table_name: get_preview_rows(con, table_name) for table_name in tables.keys()}
                )
        return result
