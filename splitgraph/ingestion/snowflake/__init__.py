import base64
import json
import urllib.parse
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Dict, Optional

from splitgraph.core.types import Credentials, Params, TableInfo
from splitgraph.hooks.data_source.fdw import ForeignDataWrapperDataSource
from splitgraph.ingestion.common import build_commandline_help

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine


def _encode_private_key(privkey: str):
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    if "PRIVATE KEY" in privkey:
        # Strip various markers and newlines from the private key, leaving
        # just the b64-encoded body.
        p_key = serialization.load_pem_private_key(
            privkey.strip().encode("ascii"), password=None, backend=default_backend()
        )
        return base64.b64encode(
            p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        ).decode()

    return privkey


class SnowflakeDataSource(ForeignDataWrapperDataSource):
    credentials_schema: Dict[str, Any] = {
        "type": "object",
        "properties": {
            "username": {"type": "string", "description": "Username"},
            "secret": {
                "type": "object",
                "oneOf": [
                    {
                        "type": "object",
                        "required": ["secret_type", "password"],
                        "properties": {
                            "secret_type": {"type": "string", "const": "password"},
                            "password": {"type": "string", "description": "Password"},
                        },
                    },
                    {
                        "type": "object",
                        "required": ["secret_type", "private_key"],
                        "properties": {
                            "secret_type": {"type": "string", "const": "private_key"},
                            "private_key": {
                                "type": "string",
                                "description": "Private key in PEM format",
                            },
                        },
                    },
                ],
            },
            "account": {
                "type": "string",
                "description": "Account Locator, e.g. xy12345.us-east-2.aws. For more information, see https://docs.snowflake.com/en/user-guide/connecting.html",
            },
        },
        "required": ["username", "account"],
    }

    params_schema = {
        "type": "object",
        "properties": {
            "database": {"type": "string", "description": "Snowflake database name"},
            "schema": {"type": "string", "description": "Snowflake schema"},
            "warehouse": {"type": "string", "description": "Warehouse name"},
            "role": {"type": "string", "description": "Role"},
            "batch_size": {
                "type": "integer",
                "description": "Default fetch size for remote queries",
            },
            "envvars": {
                "type": "object",
                "description": "Environment variables to set on the engine side",
            },
        },
        "required": ["database"],
    }

    table_params_schema = {
        "type": "object",
        "properties": {
            "subquery": {
                "type": "string",
                "description": "Subquery for this table to run on the server side",
            }
        },
    }
    supports_mount = True
    supports_load = True
    supports_sync = False

    commandline_help = """Mount a Snowflake database.

This will mount a remote Snowflake schema or a table. You can also get a mounted table to point to the result of a subquery that will be executed on the Snowflake instance. For example:

\b
```
$ sgr mount snowflake test_snowflake -o@- <<EOF
{
    "username": "username",
    "password": "password",
    "account": "acc-id.west-europe.azure",
    "database": "SNOWFLAKE_SAMPLE_DATA",
    "schema": "TPCH_SF100"
    "envvars": {"HTTPS_PROXY": "http://proxy.company.com"}
}
EOF
\b
$ sgr mount snowflake test_snowflake_subquery -o@- <<EOF
{
    "username": "username",
    "private_key": "MIIEvQIBAD...",
    "account": "acc-id.west-europe.azure",
    "database": "SNOWFLAKE_SAMPLE_DATA",
    "tables": {
        "balances": {
            "schema": {
                "n_nation": "varchar",
                "segment": "varchar",
                "avg_balance": "numeric"
            },
            "options": {
                "subquery": "SELECT n_nation AS nation, c_mktsegment AS segment, AVG(c_acctbal) AS avg_balance FROM TPCH_SF100.customer c JOIN TPCH_SF100.nation n ON c_nationkey = n_nationkey"
            }
        }
    }
}
EOF
```
    """

    commandline_kwargs_help: str = (
        build_commandline_help(credentials_schema)
        + "\n"
        + build_commandline_help(params_schema)
        + "\n"
        + "The schema parameter is required when subquery isn't used."
    )

    _icon_file = "snowflake.svg"

    def __init__(
        self,
        engine: "PostgresEngine",
        credentials: Credentials,
        params: Params,
        tables: Optional[TableInfo] = None,
    ):
        # TODO this is a hack to automatically accept both old and new versions of CSV params.
        #  We might need a more robust data source config migration system.
        credentials = SnowflakeDataSource.migrate_credentials(credentials)
        super().__init__(engine, credentials, params, tables)

    def get_fdw_name(self):
        return "multicorn"

    @classmethod
    def get_name(cls) -> str:
        return "Snowflake"

    @classmethod
    def get_description(cls) -> str:
        return "Schema, table or a subquery from a Snowflake database"

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

        if "envvars" in self.params:
            options["envvars"] = json.dumps(self.params["envvars"])

        if "batch_size" in self.params:
            options["batch_size"] = str(self.params["batch_size"])

        if self.credentials["secret"]["secret_type"] == "private_key":
            options["connect_args"] = json.dumps(
                {"private_key": _encode_private_key(self.credentials["secret"]["private_key"])}
            )

        return options

    @classmethod
    def migrate_credentials(cls, credentials: Credentials) -> Credentials:
        credentials = deepcopy(credentials)
        if "private_key" in credentials:
            credentials["secret"] = {
                "secret_type": "private_key",
                "private_key": credentials.pop("private_key"),
            }
        elif "password" in credentials:
            credentials["secret"] = {
                "secret_type": "password",
                "password": credentials.pop("password"),
            }

        return credentials

    def _build_db_url(self) -> str:
        """Construct the SQLAlchemy Snowflake db_url"""

        uname = self.credentials["username"]
        if self.credentials["secret"]["secret_type"] == "password":
            uname += f":{self.credentials['secret']['password']}"

        db_url = f"snowflake://{uname}@{self.credentials['account']}"
        if "database" in self.params:
            db_url += f"/{self.params['database']}"
            if "schema" in self.params:
                db_url += f"/{self.params['schema']}"

        extra_params = {}

        if "warehouse" in self.params:
            extra_params["warehouse"] = self.params["warehouse"]

        if "role" in self.params:
            extra_params["role"] = self.params["role"]

        if extra_params:
            db_url += "?" + urllib.parse.urlencode(extra_params)

        return db_url

    def get_remote_schema_name(self) -> str:
        if "schema" not in self.params:
            raise ValueError("Cannot IMPORT FOREIGN SCHEMA without a schema!")
        return str(self.params["schema"])
