"""Splitgraph mount handler for Socrata datasets"""
import json
import logging
from typing import Optional, Dict, Any

from psycopg2.sql import SQL, Identifier

from splitgraph.exceptions import RepositoryNotFoundError
from splitgraph.hooks.mount_handlers import init_fdw


def mount_socrata(
    mountpoint: str,
    server,
    port,
    username,
    password,
    domain: str,
    tables: Optional[Dict[str, Any]] = None,
    app_token: Optional[str] = None,
    batch_size: Optional[int] = 10000,
) -> None:
    """
    Mount a Socrata dataset.

    Mounts a remote Socrata dataset and forwards queries to it
    \b

    :param domain: Socrata domain, for example, data.albanyny.gov. Required.
    :param tables: A dictionary mapping PostgreSQL table names to Socrata table IDs. For example,
        {"salaries": "xzkq-xp2w"}. If skipped, ALL tables in the Socrata endpoint will be mounted.
    :param app_token: Socrata app token. Optional.
    :param batch_size: Amount of rows to fetch from Socrata per request (limit parameter). Maximum 50000.
    """
    from splitgraph.engine import get_engine
    from sodapy import Socrata
    from psycopg2.sql import Identifier, SQL

    engine = get_engine()
    logging.info("Mounting Socrata domain...")
    server_id = mountpoint + "_server"

    options: Dict[str, Optional[str]] = {
        "wrapper": "splitgraph.ingestion.socrata.fdw.SocrataForeignDataWrapper",
    }

    if domain:
        options["domain"] = domain
    if app_token:
        options["app_token"] = app_token
    if batch_size:
        options["batch_size"] = str(batch_size)

    init_fdw(
        engine, server_id=server_id, wrapper="multicorn", server_options=options,
    )

    engine.run_sql(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(mountpoint)))

    logging.info("Getting Socrata metadata")
    client = Socrata(domain=domain, app_token=app_token)
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
        sought_ids, datasets, mountpoint, server_id, tables
    )

    engine.run_sql(SQL(";").join(mount_statements), mount_args)


def generate_socrata_mount_queries(sought_ids, datasets, mountpoint, server_id, tables):
    # Local imports since this module gets run from commandline entrypoint on startup.

    from splitgraph.core.output import slugify
    from splitgraph.core.output import truncate_list
    from splitgraph.core.output import pluralise
    from splitgraph.core.table import create_foreign_table
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
            internal_table_name=socrata_id,
            extra_options={"column_map": json.dumps(column_map)},
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
