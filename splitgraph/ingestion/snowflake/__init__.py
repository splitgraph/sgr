import base64
import json
import urllib.parse
from typing import Dict, Optional, cast, Mapping, Any

from splitgraph.hooks.data_source.fdw import ForeignDataWrapperDataSource
from splitgraph.ingestion.common import build_commandline_help


def _encode_private_key(privkey: str):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend

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
            "password": {"type": "string", "description": "Password"},
            "account": {
                "type": "string",
                "description": "Account Locator, e.g. xy12345.us-east-2.aws. For more information, see https://docs.snowflake.com/en/user-guide/connecting.html",
            },
            "private_key": {"type": "string", "description": "Private key in PEM format",},
        },
        "required": ["username", "account"],
        "oneOf": [{"required": ["password"]}, {"required": ["private_key"]}],
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
            "database": {"type": "string", "description": "Snowflake database name"},
            "schema": {"type": "string", "description": "Snowflake schema"},
            "warehouse": {"type": "string", "description": "Warehouse name"},
            "role": {"type": "string", "description": "Role"},
            "envvars": {
                "type": "object",
                "description": "Environment variables to set on the engine side",
            },
        },
        "required": ["database"],
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

    def get_fdw_name(self):
        return "multicorn"

    @classmethod
    def get_name(cls) -> str:
        return "Snowflake"

    @classmethod
    def get_description(cls) -> str:
        return "Schema, table or a subquery from a Snowflake database"

    def get_table_options(self, table_name: str) -> Mapping[str, str]:
        result = cast(Dict[str, str], super().get_table_options(table_name))
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

        if "private_key" in self.credentials:
            options["connect_args"] = json.dumps(
                {"private_key": _encode_private_key(self.credentials["private_key"])}
            )

        return options

    def _build_db_url(self) -> str:
        """Construct the SQLAlchemy Snowflake db_url"""

        uname = self.credentials["username"]
        if "password" in self.credentials:
            uname += f":{self.credentials['password']}"

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
