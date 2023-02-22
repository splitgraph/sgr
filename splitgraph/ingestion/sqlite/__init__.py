import contextlib
import json
import os
import sqlite3
import tempfile
from contextlib import contextmanager
from copy import deepcopy
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, cast

import requests
from psycopg2.sql import SQL, Identifier

from splitgraph.core.types import (
    Credentials,
    IntrospectionResult,
    MountError,
    Params,
    TableColumn,
    TableInfo,
    TableParams,
    TableSchema,
)
from splitgraph.hooks.data_source.base import LoadableDataSource
from splitgraph.hooks.data_source.fdw import (
    ForeignDataWrapperDataSource,
    import_foreign_schema,
)
from splitgraph.hooks.data_source.utils import merge_jsonschema
from splitgraph.ingestion.common import IngestionAdapter, build_commandline_help
from splitgraph.ingestion.csv.common import dump_options, load_options

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine
    from splitgraph.engine.postgres.psycopg import PsycopgEngine

LIST_TABLES_QUERY = """
SELECT * FROM
(SELECT
	tbl_name
FROM sqlite_master
WHERE type='table') t
JOIN pragma_table_info(tbl_name) s
ORDER BY 1,2;
"""

# from https://stackoverflow.com/a/16696317
def download_file(url: str, local_fh: str) -> None:
    # NOTE the stream=True parameter below
    total_bytes_written = 0
    with requests.get(url, stream=True, verify=os.environ["SSL_CERT_FILE"]) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=8192):
            # If you have chunk encoded response uncomment if
            # and set chunk_size parameter to None.
            # if chunk:
            total_bytes_written += local_fh.write(chunk)
    return total_bytes_written


@contextmanager
def minio_file(url: str) -> str:
    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as local_fh:
        size = download_file(url, local_fh)
        print("Downloaded %s bytes" % size)
    yield local_fh.name
    os.remove(local_fh.name)


def query(sqlite_filename: str, query: str) -> List[Any]:
    with contextlib.closing(sqlite3.connect(sqlite_filename)) as con:
        with contextlib.closing(con.cursor()) as cursor:
            cursor.execute(query)
            return cursor.fetchall()


# partly based on https://stackoverflow.com/questions/1942586/comparison-of-database-column-types-in-mysql-postgresql-and-sqlite-cross-map
def sqlite_to_postgres_type(sqlite_type: str) -> str:
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
    # Otherwise, the affinity is NUMERIC.
    return "NUMERIC"


class SQLiteDataSource(LoadableDataSource):

    table_params_schema: Dict[str, Any] = {"type": "object", "properties": {}}

    params_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "url": {
                "type": "string",
                "description": "HTTP URL to the sqlite file",
                "title": "URL",
            }
        },
    }

    supports_mount = False
    supports_load = True
    supports_sync = False

    _icon_file = "sqlite.svg"  # TODO

    def _load(self, schema: str, tables: Optional[TableInfo] = None):
        import pprint

        pprint.pprint("_load", schema, tables, self.params.get("url"))

    def introspect(self) -> IntrospectionResult:
        with minio_file(self.params.get("url")) as f:
            schema: IntrospectionResult = {}
            for (
                table_name,
                column_id,
                column_name,
                column_type,
                _notnull,
                _default_value,
                pk,
            ) in query(f, LIST_TABLES_QUERY):
                table = schema.get(table_name, ([], {}))
                # TODO: convert column_type to postgres type
                table[0].append(
                    TableColumn(
                        column_id + 1, column_name, sqlite_to_postgres_type(column_type), pk == 1
                    )
                )
                schema[table_name] = table
            import pprint

            pprint.pprint("introspect %s " % f)
            pprint.pprint(schema)
            return schema

    def __init__(
        self,
        engine: "PostgresEngine",
        credentials: Credentials,
        params: Params,
        tables: Optional[TableInfo] = None,
    ):
        import json
        import pprint

        pprint.pprint("SQLiteDataSource.__init__ %s " % json.dumps(params))
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
