"""
Commands for publishing tagged Splitgraph images to a remote registry.
"""

import logging
from datetime import datetime

from psycopg2.sql import SQL, Identifier

from splitgraph._data.registry import publish_tag
from splitgraph.commands.checkout import materialized_table
from splitgraph.commands.info import get_tables_at, get_schema_at
from splitgraph.commands.provenance import provenance
from splitgraph.commands.push_pull import merge_push_params
from splitgraph.commands.tagging import get_tagged_id
from splitgraph.connection import get_connection, override_driver_connection, make_conn
from splitgraph.pg_utils import get_full_table_schema

PREVIEW_SIZE = 100


def publish(repository, tag, remote_driver=None, remote_repository=None, readme="", include_provenance=True,
            include_table_previews=True):
    """
    Summarizes the data on a previously-pushed repository and makes it available in the catalog.

    :param repository: Repository to be published. The repository must exist on the remote.
    :param tag: Image tag to be published.
    :param remote_driver: Remote driver or connection string
    :param remote_repository: Remote repository name
    :param readme: Optional README for the repository.
    :param include_provenance: If False, doesn't include the dependencies of the image
    :param include_table_previews: Whether to include data previews for every table in the image.
    :return:
    """
    conn_params, remote_driver, remote_repository = merge_push_params(repository, remote_driver,
                                                                      remote_repository)
    image_hash = get_tagged_id(repository, tag)
    logging.info("Publishing %s:%s (%s)", repository, image_hash, tag)

    dependencies = provenance(repository, image_hash) if include_provenance else None
    previews, schemata = _prepare_extra_data(image_hash, repository, include_table_previews)

    remote_conn = make_conn(*conn_params)
    try:
        with override_driver_connection(remote_conn):
            publish_tag(remote_repository, tag, image_hash, datetime.now(), dependencies, readme, schemata=schemata,
                        previews=previews if include_table_previews else None)
        remote_conn.commit()
    finally:
        remote_conn.close()


def _prepare_extra_data(image_hash, repository, include_table_previews):
    schemata = {}
    previews = {}
    for table_name in get_tables_at(repository, image_hash):
        if include_table_previews:
            logging.info("Generating preview for %s...", table_name)
            with materialized_table(repository, table_name, image_hash) as (tmp_schema, tmp_table):
                conn = get_connection()
                schema = get_full_table_schema(conn, tmp_schema, tmp_table)
                with conn.cursor() as cur:
                    cur.execute(SQL("SELECT * FROM {}.{} LIMIT %s").format(
                        Identifier(tmp_schema), Identifier(tmp_table)), (PREVIEW_SIZE,))
                    previews[table_name] = cur.fetchall()
        else:
            schema = get_schema_at(repository, table_name, image_hash)
        schemata[table_name] = [(cn, ct, pk) for _, cn, ct, pk in schema]
    return previews, schemata
