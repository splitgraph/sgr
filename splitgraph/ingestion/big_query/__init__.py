from typing import TYPE_CHECKING, Any, Dict, Optional

from splitgraph.core.types import Credentials, Params, TableInfo
from splitgraph.hooks.data_source.fdw import ForeignDataWrapperDataSource
from splitgraph.ingestion.common import build_commandline_help

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine


class BigQueryDataSource(ForeignDataWrapperDataSource):
    credentials_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "credentials_path": {"type": "string", "title": "Location of the GCP credentials"},
        },
    }

    params_schema = {
        "type": "object",
        "properties": {
            "project": {
                "type": "string",
                "title": "GCP project name",
                "description": "Name of the GCP project to use",
            },
            "dataset_name": {
                "type": "string",
                "title": "Big Query dataset",
                "description": "Name of the dataset in Big Query",
            },
        },
    }

    supports_mount = True
    supports_load = True
    supports_sync = False

    commandline_help = """Mount a GCP Big Query project/dataset.

This will mount Big Query project/dataset:

\b
```
$ sgr mount big_query bq -o@- <<EOF
{
    "credentials_path": "/path/to/my/creds.json",
    "project": "my-project-name",
    "dataset_name": "hacker_news"
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
        return "Google Big Query"

    @classmethod
    def get_description(cls) -> str:
        return "Query data in GCP Big Query datasets"

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
        }

        # For some reason, in SQLAlchemy, if this is not passed
        # to the FDW params (even if it is in the DB URL), it doesn't
        # schema-qualify tables and server-side cursors don't work for scanning
        # (loads the whole table instead of scrolling through it).
        if "schema" in self.params:
            options["schema"] = self.params["schema"]

        return options

    def _build_db_url(self) -> str:
        """Construct the SQLAlchemy GCP Big Query db_url"""

        db_url = "bigquery://"

        if "project" in self.params:
            db_url += self.params["project"] + "/" + self.params["dataset_name"]

        if "credentials_path" in self.credentials:
            # base64 encode and append as `credentials_base64` param
            pass

        return db_url

    def get_remote_schema_name(self) -> str:
        if "dataset_name" not in self.params:
            raise ValueError("Cannot IMPORT FOREIGN SCHEMA without a dataset_name!")
        return str(self.params["dataset_name"])
