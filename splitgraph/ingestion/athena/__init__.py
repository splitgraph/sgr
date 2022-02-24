from typing import TYPE_CHECKING, Any, Dict, Optional

from splitgraph.core.types import Credentials, Params, TableInfo
from splitgraph.hooks.data_source.fdw import ForeignDataWrapperDataSource
from splitgraph.ingestion.common import build_commandline_help

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine


class AmazonAthenaDataSource(ForeignDataWrapperDataSource):
    credentials_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "aws_access_key_id": {"type": "string", "title": "AWS Access Key Id"},
            "aws_secret_access_key": {"type": "string", "title": "AWS Secret Access Key"},
        },
        "required": ["aws_access_key_id", "aws_secret_access_key"],
    }

    params_schema = {
        "type": "object",
        "properties": {
            "region_name": {
                "type": "string",
                "title": "S3 region",
                "description": "Region of the S3 bucket",
            },
            "schema_name": {
                "type": "string",
                "title": "Schema",
                "description": "Athena database name",
            },
            "s3_staging_dir": {
                "title": "S3 results folder",
                "type": "string",
                "description": "Folder for storing query output",
            },
        },
        "required": ["region_name", "schema_name", "s3_staging_dir"],
    }

    supports_mount = True
    supports_load = True
    supports_sync = False

    commandline_help = """Mount a Amazon Athena database.

This will mount an Athena schema or a table:

\b
```
$ sgr mount athena s3 -o@- <<EOF
{
    "aws_access_key_id": "ABCD",
    "aws_secret_access_key": "abcd",
    "region_name": "eu-west-3",
    "schema_name": "mydatabase",
    "s3_staging_dir": "s3://my-bucket/output/",
}
EOF
```
    """

    commandline_kwargs_help: str = (
        build_commandline_help(credentials_schema) + "\n" + build_commandline_help(params_schema)
    )

    _icon_file = "athena.svg"

    def __init__(
        self,
        engine: "PostgresEngine",
        credentials: Credentials,
        params: Params,
        tables: Optional[TableInfo] = None,
    ):
        super().__init__(engine, credentials, params, tables)

    def get_fdw_name(self):
        return "multicorn"

    @classmethod
    def get_name(cls) -> str:
        return "Amazon Athena"

    @classmethod
    def get_description(cls) -> str:
        return "Query data in Amazon S3 files and folders"

    def get_table_options(
        self, table_name: str, tables: Optional[TableInfo] = None
    ) -> Dict[str, str]:
        result = super().get_table_options(table_name, tables)
        result["tablename"] = result.get("tablename", table_name)
        return result

    def get_server_options(self):
        options: Dict[str, Optional[str]] = {
            "wrapper": "multicorn.sqlalchemyfdw.SqlAlchemyFdw",
            "db_url": self._build_db_url(),
            "cast_quals": "true",
        }

        # For some reason, in SQLAlchemy, if this is not passed
        # to the FDW params (even if it is in the DB URL), it doesn't
        # schema-qualify tables and server-side cursors don't work for scanning
        # (loads the whole table instead of scrolling through it).
        if "schema" in self.params:
            options["schema"] = self.params["schema"]

        return options

    def _build_db_url(self) -> str:
        """Construct the SQLAlchemy Amazon Athena db_url"""

        aws_access_key_id = self.credentials["aws_access_key_id"]
        aws_secret_access_key = self.credentials["aws_secret_access_key"]
        region_name = self.params["region_name"]
        schema_name = self.params["schema_name"]
        s3_staging_dir = self.params["s3_staging_dir"]

        db_url = (
            f"awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@"
            f"athena.{region_name}.amazonaws.com:443/"
            f"{schema_name}?s3_staging_dir={s3_staging_dir}"
        )

        return db_url

    def get_remote_schema_name(self) -> str:
        if "schema_name" not in self.params:
            raise ValueError("Cannot IMPORT FOREIGN SCHEMA without a schema_name!")
        return str(self.params["schema_name"])
