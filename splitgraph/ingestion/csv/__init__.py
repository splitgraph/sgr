import contextlib
import json
from copy import deepcopy
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, cast

from psycopg2.sql import SQL, Identifier
from splitgraph.core.types import (
    Credentials,
    MountError,
    Params,
    TableInfo,
    TableParams,
    TableSchema,
)
from splitgraph.hooks.data_source.fdw import (
    ForeignDataWrapperDataSource,
    import_foreign_schema,
)
from splitgraph.ingestion.common import IngestionAdapter, build_commandline_help
from splitgraph.ingestion.csv.common import dump_options, load_options

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine, PsycopgEngine


class CSVIngestionAdapter(IngestionAdapter):
    @staticmethod
    def create_ingestion_table(data, engine, schema: str, table: str, **kwargs):
        schema_spec = kwargs.pop("schema_spec")
        engine.delete_table(schema, table)
        engine.create_table(schema, table, schema_spec=schema_spec, include_comments=True)

    @staticmethod
    def data_to_new_table(
        data, engine: "PostgresEngine", schema: str, table: str, no_header: bool = True, **kwargs
    ):
        copy_csv_buffer(data, engine, schema, table, no_header, **kwargs)

    @staticmethod
    def query_to_data(engine, query: str, schema: Optional[str] = None, **kwargs):
        buffer = kwargs.pop("buffer")
        query_to_csv(engine, query, buffer, schema)
        return buffer


csv_adapter = CSVIngestionAdapter()


def copy_csv_buffer(
    data, engine: "PsycopgEngine", schema: str, table: str, no_header: bool = False, **kwargs
):
    """Copy CSV data from a buffer into a given schema/table"""
    with engine.copy_cursor() as cur:
        extra_args = [not no_header]

        copy_command = SQL("COPY {}.{} FROM STDIN WITH (FORMAT CSV, HEADER %s").format(
            Identifier(schema), Identifier(table)
        )
        for k, v in kwargs.items():
            if k in ("encoding", "delimiter", "escape") and v:
                copy_command += SQL(", " + k + " %s")
                extra_args.append(v)
        copy_command += SQL(")")

        cur.copy_expert(
            cur.mogrify(copy_command, extra_args),
            data,
        )


def query_to_csv(engine: "PsycopgEngine", query, buffer, schema: Optional[str] = None):
    copy_query = SQL("COPY (") + SQL(query) + SQL(") TO STDOUT WITH (FORMAT CSV, HEADER TRUE);")
    if schema:
        copy_query = SQL("SET search_path TO {},public;").format(Identifier(schema)) + copy_query

    with engine.copy_cursor() as cur:
        cur.copy_expert(copy_query, buffer)


class CSVDataSource(ForeignDataWrapperDataSource):
    credentials_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {"s3_access_key": {"type": "string"}, "s3_secret_key": {"type": "string"}},
    }

    params_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "connection": {
                "type": "object",
                "oneOf": [
                    {
                        "type": "object",
                        "required": ["connection_type", "url"],
                        "properties": {
                            "connection_type": {"type": "string", "const": "http"},
                            "url": {"type": "string", "description": "HTTP URL to the CSV file"},
                        },
                    },
                    {
                        "type": "object",
                        "required": ["connection_type", "s3_endpoint", "s3_bucket"],
                        "properties": {
                            "connection_type": {"type": "string", "const": "s3"},
                            "s3_endpoint": {
                                "type": "string",
                                "description": "S3 endpoint (including port if required)",
                            },
                            "s3_region": {
                                "type": "string",
                                "description": "Region of the S3 bucket",
                            },
                            "s3_secure": {
                                "type": "boolean",
                                "description": "Whether to use HTTPS for S3 access",
                            },
                            "s3_bucket": {
                                "type": "string",
                                "description": "Bucket the object is in",
                            },
                            "s3_object": {
                                "type": "string",
                                "description": "Limit the import to a single object",
                            },
                            "s3_object_prefix": {
                                "type": "string",
                                "description": "Prefix for object in S3 bucket",
                            },
                        },
                    },
                ],
            },
            "autodetect_header": {
                "type": "boolean",
                "description": "Detect whether the CSV file has a header automatically",
                "default": True,
            },
            "autodetect_dialect": {
                "type": "boolean",
                "description": "Detect the CSV file's dialect (separator, quoting characters etc) automatically",
                "default": True,
            },
            "autodetect_encoding": {
                "type": "boolean",
                "description": "Detect the CSV file's encoding automatically",
                "default": True,
            },
            "autodetect_sample_size": {
                "type": "integer",
                "description": "Sample size, in bytes, for encoding/dialect/header detection",
                "default": 65536,
            },
            "schema_inference_rows": {
                "type": "integer",
                "description": "Number of rows to use for schema inference",
                "default": 100000,
            },
            "encoding": {
                "type": "string",
                "description": "Encoding of the CSV file",
                "default": "utf-8",
            },
            "ignore_decode_errors": {
                "type": "boolean",
                "description": "Ignore errors when decoding the file",
                "default": False,
            },
            "header": {
                "type": "boolean",
                "description": "First line of the CSV file is its header",
                "default": True,
            },
            "delimiter": {
                "type": "string",
                "description": "Character used to separate fields in the file",
                "default": ",",
            },
            "quotechar": {
                "type": "string",
                "description": "Character used to quote fields",
                "default": '"',
            },
        },
    }

    table_params_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "url": {"type": "string", "description": "HTTP URL to the CSV file"},
            "s3_object": {"type": "string", "description": "S3 object of the CSV file"},
            # Add various CSV dialect overrides to the table params schema.
            **{k: v for k, v in params_schema["properties"].items() if k != "connection"},
        },
    }

    supports_mount = True
    supports_load = True
    supports_sync = False

    _icon_file = "csv.svg"

    commandline_help = """Mount CSV files in S3/HTTP.

If passed an URL, this will live query a CSV file on an HTTP server. If passed
S3 access credentials, this will scan a bucket for CSV files, infer their schema
and make them available to query over SQL.  

For example:  

\b
```
sgr mount csv target_schema -o@- <<EOF
  {
    "s3_endpoint": "cdn.mycompany.com:9000",
    "s3_access_key": "ABCDEF",
    "s3_secret_key": "GHIJKL",
    "s3_bucket": "data",
    "s3_object_prefix": "csv_files/current/",
    "autodetect_header": true,
    "autodetect_dialect": true,
    "autodetect_encoding": true
  }
EOF
```
"""

    commandline_kwargs_help: str = (
        build_commandline_help(credentials_schema) + "\n" + build_commandline_help(params_schema)
    )

    def __init__(
        self,
        engine: "PostgresEngine",
        credentials: Credentials,
        params: Params,
        tables: Optional[TableInfo] = None,
    ):
        # TODO this is a hack to automatically accept both old and new versions of CSV params.
        #  We might need a more robust data source config migration system.
        params = CSVDataSource.migrate_params(params)
        super().__init__(engine, credentials, params, tables)

    def get_fdw_name(self):
        return "multicorn"

    @classmethod
    def get_name(cls) -> str:
        return "CSV files in S3/HTTP"

    @classmethod
    def get_description(cls) -> str:
        return "CSV files in S3/HTTP"

    @classmethod
    def from_commandline(cls, engine, commandline_kwargs) -> "CSVDataSource":
        params = deepcopy(commandline_kwargs)
        credentials = Credentials({})
        for k in ["s3_access_key", "s3_secret_key"]:
            if k in params:
                credentials[k] = params[k]
        return cls(engine, credentials, params)

    @classmethod
    def migrate_params(cls, params: Params) -> Params:
        params = deepcopy(params)
        if "connection" in params:
            return params

        if "url" in params:
            params["connection"] = {"connection_type": "http", "url": params["url"]}
            del params["url"]
        else:
            connection = {"connection_type": "s3"}
            for key in [
                "s3_endpoint",
                "s3_region",
                "s3_secure",
                "s3_bucket",
                "s3_object",
                "s3_object_prefix",
            ]:
                with contextlib.suppress(KeyError):
                    connection[key] = params.pop(key)

            params["connection"] = connection
        return params

    def get_table_options(
        self, table_name: str, tables: Optional[TableInfo] = None
    ) -> Dict[str, str]:
        tables = tables or self.tables

        if not isinstance(tables, dict):
            return {}

        result = {
            k: v
            for k, v in tables.get(table_name, cast(Tuple[TableSchema, TableParams], ({}, {})))[
                1
            ].items()
        }

        # Set a default s3_object if we're using S3 and not HTTP
        if "url" not in result:
            result["s3_object"] = result.get(
                "s3_object", self.params.get("s3_object_prefix", "") + table_name
            )
        return dump_options(result)

    def _create_foreign_tables(
        self, schema: str, server_id: str, tables: TableInfo
    ) -> List[MountError]:
        # Override _create_foreign_tables (actual mounting code) to support TableInfo structs
        # where the schema is empty. This is so that we can call this data source with a limited
        # list of CSV files and their delimiters / other params and have it introspect just
        # those tables (instead of e.g. scanning the whole bucket).
        errors: List[MountError] = []

        if isinstance(tables, dict):
            to_introspect = {
                table_name: table_options
                for table_name, (table_schema, table_options) in tables.items()
                if not table_schema
            }
            if to_introspect:
                # This FDW's implementation of IMPORT FOREIGN SCHEMA supports passing a JSON of table
                # options in options
                errors.extend(
                    import_foreign_schema(
                        self.engine,
                        schema,
                        self.get_remote_schema_name(),
                        server_id,
                        tables=list(to_introspect.keys()),
                        options={"table_options": json.dumps(to_introspect)},
                    )
                )

            # Create the remaining tables (that have a schema) as usual.
            tables = {
                table_name: (table_schema, table_options)
                for table_name, (table_schema, table_options) in tables.items()
                if table_schema
            }
            if not tables:
                return errors

        errors.extend(super()._create_foreign_tables(schema, server_id, tables))
        return errors

    def _get_foreign_table_options(self, schema: str) -> List[Tuple[str, Dict[str, Any]]]:
        options = super()._get_foreign_table_options(schema)

        # Deserialize things like booleans from the foreign table options that the FDW inferred
        # for us.
        return [(t, load_options(d)) for t, d in options]

    def get_server_options(self):
        options: Dict[str, Any] = {}
        for k in self.params_schema["properties"].keys():
            # Flatten the options and extract connection parameters
            if k in self.params:
                if k != "connection":
                    options[k] = self.params[k]
                else:
                    options.update(self.params[k])

        for k in self.credentials_schema["properties"].keys():
            if k in self.credentials:
                options[k] = self.credentials[k]
        # Serialize all options to JSON so that we can be sure they're all strings.
        options = dump_options(options)
        options["wrapper"] = "splitgraph.ingestion.csv.fdw.CSVForeignDataWrapper"

        return options

    def get_remote_schema_name(self) -> str:
        # We ignore the schema name and use the bucket/prefix passed in the params instead.
        return "data"

    @staticmethod
    def _get_url(merged_options: Dict[str, Any], expiry: int = 3600) -> List[Tuple[str, str]]:
        from splitgraph.ingestion.csv.common import get_s3_params

        if merged_options.get("url"):
            # Currently all URLs are public anyway, so return it directly
            return [("text/csv", merged_options["url"])]
        else:
            # Instantiate the Minio client and pre-sign a URL
            s3_client, s3_bucket, s3_object_prefix = get_s3_params(merged_options)
            s3_object = merged_options.get("s3_object")
            if not s3_object:
                return []
            return [
                (
                    "text/csv",
                    s3_client.presigned_get_object(
                        bucket_name=s3_bucket,
                        object_name=s3_object,
                        expires=timedelta(seconds=expiry),
                    ),
                )
            ]

    def get_raw_url(
        self, tables: Optional[TableInfo] = None, expiry: int = 3600
    ) -> Dict[str, List[Tuple[str, str]]]:
        tables = tables or self.tables
        if not tables:
            return {}
        result: Dict[str, List[Tuple[str, str]]] = {}

        # Merge the table options to take care of overrides and use them to get URLs
        # for each table.
        server_options = self.get_server_options()
        for table in tables:
            table_options = self.get_table_options(table, tables)
            full_options = {**server_options, **table_options}
            del full_options["wrapper"]
            result[table] = self._get_url(load_options(full_options))

        return result
