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


# from https://stackoverflow.com/a/16696317
def download_file(url: str, local_fh: str) -> None:
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True, verify=os.environ["SSL_CERT_FILE"]) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=8192):
            # If you have chunk encoded response uncomment if
            # and set chunk_size parameter to None.
            # if chunk:
            local_fh.write(chunk)


@contextmanager
def minio_file(url: str) -> str:
    with tempfile.NamedTemporaryFile(mode="wb") as local_fh:
        download_file(url, local_fh)
        yield local_fh.name


class SQLiteDataSource(LoadableDataSource):

    table_params_schema: Dict[str, Any] = {"type": "object", "properties": {}}

    params_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "url": {
                "type": "string",
                "description": "HTTP URL to the CSV file",
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
            import pprint

            pprint.pprint("introspect %s " % f)
        result = IntrospectionResult({})
        return result

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
