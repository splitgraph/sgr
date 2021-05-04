import json
from copy import deepcopy
from typing import Optional, TYPE_CHECKING, Dict, List, Tuple, Any

from psycopg2.sql import SQL, Identifier

from splitgraph.core.types import TableInfo, MountError, Credentials
from splitgraph.hooks.data_source.fdw import ForeignDataWrapperDataSource, import_foreign_schema
from splitgraph.ingestion.common import IngestionAdapter, build_commandline_help

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PsycopgEngine


class CSVIngestionAdapter(IngestionAdapter):
    @staticmethod
    def create_ingestion_table(data, engine, schema: str, table: str, **kwargs):
        schema_spec = kwargs.pop("schema_spec")
        engine.delete_table(schema, table)
        engine.create_table(schema, table, schema_spec=schema_spec, include_comments=True)

    @staticmethod
    def data_to_new_table(
        data, engine: "PsycopgEngine", schema: str, table: str, no_header: bool = True, **kwargs
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
    credentials_schema = {
        "type": "object",
        "properties": {"s3_access_key": {"type": "string"}, "s3_secret_key": {"type": "string"}},
    }

    params_schema = {
        "type": "object",
        "properties": {
            "url": {"type": "string", "description": "HTTP URL to the CSV file"},
            "s3_endpoint": {
                "type": "string",
                "description": "S3 endpoint (including port if required)",
            },
            "s3_region": {"type": "string", "description": "Region of the S3 bucket"},
            "s3_secure": {"type": "boolean", "description": "Whether to use HTTPS for S3 access"},
            "s3_bucket": {"type": "string", "description": "Bucket the object is in"},
            "s3_object": {"type": "string", "description": "Limit the import to a single object"},
            "s3_object_prefix": {"type": "string", "description": "Prefix for object in S3 bucket"},
            "autodetect_header": {
                "type": "boolean",
                "description": "Detect whether the CSV file has a header automatically",
            },
            "autodetect_dialect": {
                "type": "boolean",
                "description": "Detect the CSV file's dialect (separator, quoting characters etc) automatically",
            },
            "autodetect_encoding": {
                "type": "boolean",
                "description": "Detect the CSV file's encoding automatically",
            },
            "autodetect_sample_size": {
                "type": "integer",
                "description": "Sample size, in bytes, for encoding/dialect/header detection",
            },
            "schema_inference_rows": {
                "type": "integer",
                "description": "Number of rows to use for schema inference",
            },
            "encoding": {"type": "string", "description": "Encoding of the CSV file"},
            "ignore_decode_errors": {
                "type": "boolean",
                "description": "Ignore errors when decoding the file",
            },
            "header": {
                "type": "boolean",
                "description": "First line of the CSV file is its header",
            },
            "delimiter": {
                "type": "string",
                "description": "Character used to separate fields in the file",
            },
            "quotechar": {"type": "string", "description": "Character used to quote fields"},
        },
        "oneOf": [{"required": ["url"]}, {"required": ["s3_endpoint", "s3_bucket"]}],
    }

    table_params_schema = {
        "type": "object",
        "properties": {
            "url": {"type": "string", "description": "HTTP URL to the CSV file"},
            "s3_object": {"type": "string", "description": "S3 object of the CSV file"},
        },
    }

    supports_mount = True
    supports_load = True
    supports_sync = False

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

    def get_table_options(
        self, table_name: str, tables: Optional[TableInfo] = None
    ) -> Dict[str, str]:
        result = super().get_table_options(table_name, tables)

        # Set a default s3_object if we're using S3 and not HTTP
        if "url" not in result:
            result["s3_object"] = result.get(
                "s3_object", self.params.get("s3_object_prefix", "") + table_name
            )
        return result

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
        def _destring_table_options(table_options):
            for k in ["autodetect_header", "autodetect_encoding", "autodetect_dialect", "header"]:
                if k in table_options:
                    table_options[k] = table_options[k].lower() == "true"
            return table_options

        return [(t, _destring_table_options(d)) for t, d in options]

    def get_server_options(self):
        options: Dict[str, Optional[str]] = {
            "wrapper": "splitgraph.ingestion.csv.fdw.CSVForeignDataWrapper"
        }
        for k in self.params_schema["properties"].keys():
            if k in self.params:
                options[k] = str(self.params[k])
        for k in self.credentials_schema["properties"].keys():
            if k in self.credentials:
                options[k] = str(self.credentials[k])
        return options

    def get_remote_schema_name(self) -> str:
        # We ignore the schema name and use the bucket/prefix passed in the params instead.
        return "data"
