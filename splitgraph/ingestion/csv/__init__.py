from copy import deepcopy
from typing import Optional, TYPE_CHECKING, Dict, Mapping, cast

from psycopg2.sql import SQL, Identifier

from splitgraph.hooks.data_source.fdw import ForeignDataWrapperDataSource
from splitgraph.ingestion.common import IngestionAdapter

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
            cur.mogrify(copy_command, extra_args), data,
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
            "tables": {
                "type": "object",
                "additionalProperties": {
                    "options": {"type": "object", "additionalProperties": {"type": "string"}},
                },
            },
            "url": {"type": "string", "description": "HTTP URL to the CSV file"},
            "s3_endpoint": {
                "type": "string",
                "description": "S3 endpoint (including port if required)",
            },
            "s3_region": {"type": "string", "description": "Region of the S3 bucket"},
            "s3_secure": {"type": "boolean", "description": "Whether to use HTTPS for S3 access"},
            "s3_bucket": {"type": "string", "description": "Bucket the object is in"},
            "s3_object_prefix": {"type": "string", "description": "Prefix for object in S3 bucket"},
            "autodetect_header": {
                "type": "boolean",
                "description": "Detect whether the CSV file has a header automatically",
            },
            "autodetect_dialect": {
                "type": "boolean",
                "description": "Detect the CSV file's dialect (separator, quoting characters etc) automatically",
            },
            "header": {
                "type": "boolean",
                "description": "First line of the CSV file is its header",
            },
            "separator": {
                "type": "string",
                "description": "Character used to separate fields in the file",
            },
            "quotechar": {"type": "string", "description": "Character used to quote fields"},
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

```
sgr mount csv target_schema -o@- <<EOF
  {
    "s3_endpoint": "cdn.mycompany.com:9000",
    "s3_access_key": "ABCDEF",
    "s3_secret_key": "GHIJKL",
    "s3_bucket": "data",
    "s3_object_prefix": "csv_files/current/",
    "autodetect_header": true,
    "autodetect_dialect": true
  }
EOF
```
"""

    commandline_kwargs_help: str = """url: HTTP URL (either the URL or S3 parameters are required)
s3_endpoint: S3 host and port (required)
s3_secure: Use SSL (default true)
s3_access_key: S3 access key (optional)
s3_secret_key: S3 secret key (optional)
s3_bucket: S3 bucket name (required)
s3_object_prefix: Prefix for object IDs to mount (optional)
autodetect_header: Detect whether the CSV file has a header automatically
autodetect_dialect: Detect the file's separator, quoting characters etc. automatically
header: Treats the first line as a header
separator: Override the character used as a CSV separator
quotechar: Override the character used as a CSV quoting character
tables: Objects to mount (default all). If a list, will import only these objects. 
If a dictionary, must have the format
    {"table_name": {"schema": {"col_1": "type_1", ...},
                    "options": {[get passed to CREATE FOREIGN TABLE]}}}.
    """

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
        credentials = {
            "s3_access_key": params.pop("s3_access_key", None),
            "s3_secret_key": params.pop("s3_secret_key", None),
        }
        return cls(engine, credentials, params)

    def get_table_options(self, table_name: str) -> Mapping[str, str]:
        result = cast(Dict[str, str], super().get_table_options(table_name))
        result["s3_object"] = result.get(
            "s3_object", self.params.get("s3_object_prefix", "") + table_name
        )
        return result

    def get_server_options(self):
        options: Dict[str, Optional[str]] = {
            "wrapper": "splitgraph.ingestion.csv.fdw.CSVForeignDataWrapper"
        }
        for k in [
            "s3_endpoint",
            "s3_region",
            "s3_secure",
            "s3_bucket",
            "s3_object_prefix",
            "s3_object",
            "url",
            "autodetect_dialect",
            "autodetect_header",
            "header",
            "separator",
            "quotechar",
        ]:
            if k in self.params:
                options[k] = str(self.params[k])
        for k in ["s3_access_key", "s3_secret_key"]:
            if k in self.credentials:
                options[k] = str(self.credentials[k])
        return options

    def get_remote_schema_name(self) -> str:
        # We ignore the schema name and use the bucket/prefix passed in the params instead.
        return "data"
