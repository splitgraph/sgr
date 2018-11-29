import logging
from datetime import datetime

from psycopg2.sql import SQL, Identifier

from splitgraph.commands.checkout import materialized_table
from splitgraph.commands.info import get_tables_at
from splitgraph.commands.provenance import provenance
from splitgraph.commands.repository import get_remote_for, get_schema_at
from splitgraph.commands.tagging import get_tagged_id
from splitgraph.connection import get_connection, override_driver_connection, parse_connection_string, make_conn
from splitgraph.exceptions import SplitGraphException
from splitgraph.pg_utils import get_full_table_schema
from splitgraph.registry_meta_handler import publish_tag

PREVIEW_SIZE = 100


def publish(repository, tag, remote_conn_string=None, remote_repository=None, readme="", include_provenance=True,
            include_table_previews=True):
    """
    Summarizes the data on a previously-pushed repository and makes it available in the catalog.

    :param repository: Repository to be published. The repository must exist on the remote.
    :param tag: Image tag to be published.
    :param remote_conn_string: Connection string to the remote
    :param remote_repository: Remote repository name
    :param readme: Optional README for the repository.
    :param include_provenance: If False, doesn't include the dependencies of the image
    :param include_table_previews: Whether to include data previews for every table in the image.
    :return:
    """
    remote_repository = remote_repository or repository

    remote_info = get_remote_for(repository, 'origin') if not remote_conn_string or not remote_repository \
        else (remote_conn_string, remote_repository)
    if not remote_info:
        raise SplitGraphException("No remote found for repository %s. Has it been pushed?" % repository)

    logging.info("REMOTE INFO: " + str(remote_info))
    remote_conn_string, remote_repository = remote_info
    image_hash = get_tagged_id(repository, tag)
    logging.info("Publishing %s:%s (%s)" % (repository, image_hash, tag))

    dependencies = provenance(repository, image_hash) if include_provenance else None

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

    remote_conn = make_conn(*parse_connection_string(remote_conn_string))
    try:
        with override_driver_connection(remote_conn):
            publish_tag(remote_repository, tag, image_hash, datetime.now(), dependencies, readme, schemata=schemata,
                        previews=previews if include_table_previews else None)
        remote_conn.commit()
    finally:
        remote_conn.close()
