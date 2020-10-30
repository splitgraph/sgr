"""Splitgraph mount handler for Socrata datasets"""
import json
import logging
from copy import deepcopy
from typing import Optional, Dict

from psycopg2.sql import SQL, Identifier

from splitgraph.exceptions import RepositoryNotFoundError
from splitgraph.hooks.data_source.fdw import create_foreign_table, ForeignDataWrapperDataSource


class SocrataDataSource(ForeignDataWrapperDataSource):
    credentials_schema = {
        "type": "object",
        "properties": {
            "app_token": {"type": ["string", "null"], "description": "Socrata app token, optional"}
        },
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
                "description": "Amount of rows to fetch from Socrata per request (limit parameter). Maximum 50000.",
            },
            "tables": {
                "type": "object",
            },
        },
        "required": ["domain"],
    }

    """
    tables: A dictionary mapping PostgreSQL table names to Socrata table IDs. For example,
        {"salaries": "xzkq-xp2w"}. If skipped, ALL tables in the Socrata endpoint will be mounted.
    """

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
        credentials = {"app_token": params.pop("app_token", None)}
        return cls(engine, credentials, params)

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

    def _create_foreign_tables(self, schema, server_id, tables):
        from sodapy import Socrata
        from psycopg2.sql import SQL

        logging.info("Getting Socrata metadata")
        client = Socrata(domain=self.params["domain"], app_token=self.credentials.get("app_token"))
        tables = self.params.get("tables")
        sought_ids = tables.values() if tables else []

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


def generate_socrata_mount_queries(sought_ids, datasets, mountpoint, server_id, tables):
    # Local imports since this module gets run from commandline entrypoint on startup.

    from splitgraph.core.output import slugify
    from splitgraph.core.output import truncate_list
    from splitgraph.core.output import pluralise
    from splitgraph.ingestion.socrata.querying import socrata_to_sg_schema

    found_ids = set(d["resource"]["id"] for d in datasets)
    logging.info("Loaded metadata for %s", pluralise("Socrata table", len(found_ids)))

    if tables:
        missing_ids = [d for d in found_ids if d not in sought_ids]
        if missing_ids:
            raise ValueError(
                "Some Socrata tables couldn't be found! Missing tables: %s"
                % truncate_list(missing_ids)
            )

        tables_inv = {s: p for p, s in tables.items()}
    else:
        tables_inv = {}

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
