import base64
from copy import deepcopy
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
            "credentials": {
                "type": "string",
                "title": "GCP credentials",
                "description": "GCP credentials in JSON format",
            },
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
        "required": ["project", "dataset_name"],
    }

    supports_mount = True
    supports_load = True
    supports_sync = False

    commandline_help = """Mount a GCP Big Query project/dataset.

This will mount a Big Query dataset:

\b
```
$ sgr mount bigquery bq -o@- <<EOF
{
    "credentials": "/path/to/my/creds.json",
    "project": "my-project-name",
    "dataset_name": "my_dataset"
}
EOF
```
    """

    commandline_kwargs_help: str = (
        build_commandline_help(credentials_schema) + "\n" + build_commandline_help(params_schema)
    )

    _icon_file = "bigquery.svg"

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
        return "Google BigQuery"

    @classmethod
    def get_description(cls) -> str:
        return "Query data in GCP BigQuery datasets"

    @classmethod
    def from_commandline(cls, engine, commandline_kwargs) -> "BigQueryDataSource":
        params = deepcopy(commandline_kwargs)
        credentials = Credentials({})

        if "credentials" in params:
            with open(params["credentials"], "r") as credentials_file:
                credentials_str = credentials_file.read()

            params.pop("credentials")
            credentials["credentials"] = credentials_str

        return cls(engine, credentials, params)

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

        db_url = f"bigquery://{self.params['project']}/{self.params['dataset_name']}"

        if "credentials" in self.credentials:
            # base64 encode the credentials
            credentials_str = self.credentials["credentials"]
            credentials_base64 = base64.urlsafe_b64encode(credentials_str.encode()).decode()
            db_url += f"?credentials_base64={credentials_base64}"

        return db_url

    def get_remote_schema_name(self) -> str:
        if "dataset_name" not in self.params:
            raise ValueError("Cannot IMPORT FOREIGN SCHEMA without a dataset_name!")
        return str(self.params["dataset_name"])
