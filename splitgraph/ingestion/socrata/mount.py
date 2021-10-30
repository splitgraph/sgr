"""Splitgraph mount handler for Socrata datasets"""
import json
import logging
from copy import deepcopy
from typing import Dict, List, Optional, Tuple

from psycopg2.sql import SQL, Identifier
from splitgraph.core.types import Credentials, MountError, TableInfo
from splitgraph.exceptions import RepositoryNotFoundError
from splitgraph.hooks.data_source.fdw import (
    ForeignDataWrapperDataSource,
    create_foreign_table,
)


class SocrataDataSource(ForeignDataWrapperDataSource):
    credentials_schema = {
        "type": "object",
        "properties": {"app_token": {"type": "string", "description": "Socrata app token"}},
    }

    params_schema = {
        "type": "object",
        "properties": {
            "domain": {
                "type": "string",
                "description": "Socrata domain, for example, data.albanyny.gov",
            },
            "batch_size": {
                "type": "integer",
                "description": "Amount of rows to fetch from Socrata per request (limit parameter)",
                "minimum": 1,
                "default": 1000,
                "maximum": 50000,
            },
        },
        "required": ["domain"],
    }

    table_params_schema = {
        "type": "object",
        "properties": {
            "socrata_id": {"type": "string", "description": "Socrata dataset ID, e.g. xzkq-xp2w"}
        },
        "required": ["socrata_id"],
    }

    @classmethod
    def get_name(cls) -> str:
        return "Socrata"

    @classmethod
    def get_description(cls) -> str:
        return "Data source for remote Socrata datasets that uses SoQL for live queries"

    def get_fdw_name(self):
        return "multicorn"

    @classmethod
    def from_commandline(cls, engine, commandline_kwargs) -> "SocrataDataSource":
        params = deepcopy(commandline_kwargs)

        # Convert the old-style "tables" param ({"table_name": "some_id"})
        # to the schema this data source expects
        #   {"table_name": [table schema], {"socrata_id": "some_id"}}
        # Note that we don't actually care about the table schema in this data source,
        # since it reintrospects on every mount.

        tables = params.pop("tables", [])
        if isinstance(tables, dict) and isinstance(next(iter(tables.values())), str):
            tables = {k: ([], {"socrata_id": v}) for k, v in tables.items()}

        credentials = Credentials({})
        return cls(engine, credentials, params, tables)

    def get_server_options(self):
        options: Dict[str, Optional[str]] = {
            "wrapper": "splitgraph.ingestion.socrata.fdw.SocrataForeignDataWrapper"
        }
        for k in ["domain", "batch_size"]:
            if self.params.get(k):
                options[k] = str(self.params[k])
        if self.credentials.get("app_token"):
            options["app_token"] = str(self.credentials["app_token"])
        return options

    def _create_foreign_tables(
        self, schema: str, server_id: str, tables: TableInfo
    ) -> List[MountError]:
        from psycopg2.sql import SQL
        from sodapy import Socrata

        logging.info("Getting Socrata metadata")
        client = Socrata(domain=self.params["domain"], app_token=self.credentials.get("app_token"))

        tables = self.tables or tables
        if isinstance(tables, list):
            sought_ids = tables
        else:
            sought_ids = [t[1]["socrata_id"] for t in tables.values()]

        try:
            datasets = client.datasets(ids=sought_ids, only=["dataset"])
        except Exception as e:
            if "Unknown response format: text/html" in str(e):
                # If the Socrata dataset/domain isn't found, sodapy doesn't catch it directly
                # and instead stumbles on an unexpected content-type of the 404 page it's served.
                # We catch that and reraise a more friendly message.
                raise RepositoryNotFoundError("Socrata domain or dataset not found!") from e
            raise

        if not datasets:
            raise RepositoryNotFoundError("Socrata domain or dataset not found!")

        mount_statements, mount_args = generate_socrata_mount_queries(
            sought_ids, datasets, schema, server_id, tables
        )

        self.engine.run_sql(SQL(";").join(mount_statements), mount_args)
        return []

    def get_raw_url(
        self, tables: Optional[TableInfo] = None, expiry: int = 3600
    ) -> Dict[str, List[Tuple[str, str]]]:
        tables = tables or self.tables
        if not tables:
            return {}
        result: Dict[str, List[Tuple[str, str]]] = {}

        for table in tables:
            table_options = super().get_table_options(table, tables)
            full_options = {**self.params, **table_options}
            domain = full_options.get("domain")
            socrata_id = full_options.get("socrata_id")
            if not domain or not socrata_id:
                continue

            result[table] = [
                (
                    "text/csv",
                    f"https://{domain}/api/views/{socrata_id}/rows.csv?accessType=DOWNLOAD",
                )
            ]

        return result


def generate_socrata_mount_queries(sought_ids, datasets, mountpoint, server_id, tables: TableInfo):
    # Local imports since this module gets run from commandline entrypoint on startup.

    from splitgraph.core.output import pluralise, slugify
    from splitgraph.ingestion.socrata.querying import socrata_to_sg_schema

    found_ids = {d["resource"]["id"] for d in datasets}
    logging.info("Loaded metadata for %s", pluralise("Socrata table", len(found_ids)))

    tables_inv = _get_table_map(found_ids, sought_ids, tables)

    mount_statements = []
    mount_args = []
    for dataset in datasets:
        socrata_id = dataset["resource"]["id"]
        table_name = tables_inv.get(socrata_id) or slugify(
            dataset["resource"]["name"]
        ) + "_" + socrata_id.replace("-", "_")
        schema_spec, column_map = socrata_to_sg_schema(dataset)
        sql, args = create_foreign_table(
            schema=mountpoint,
            server=server_id,
            table_name=table_name,
            schema_spec=schema_spec,
            extra_options={"column_map": json.dumps(column_map), "table": socrata_id},
        )

        description = dataset["resource"].get("description")
        if description:
            sql += SQL("COMMENT ON FOREIGN TABLE {}.{} IS %s").format(
                Identifier(mountpoint), Identifier(table_name)
            )
            args.append(description)

        mount_statements.append(sql)
        mount_args.extend(args)

    return mount_statements, mount_args


def _get_table_map(found_ids, sought_ids, tables: TableInfo) -> Dict[str, str]:
    """Get a map of Socrata ID -> local table name"""
    from splitgraph.core.output import truncate_list

    if isinstance(tables, (dict, list)) and tables:
        missing_ids = [d for d in found_ids if d not in sought_ids]
        if missing_ids:
            raise ValueError(
                "Some Socrata tables couldn't be found! Missing tables: %s"
                % truncate_list(missing_ids)
            )

        if isinstance(tables, dict):
            return {to["socrata_id"]: p for p, (ts, to) in tables.items()}
    return {}
