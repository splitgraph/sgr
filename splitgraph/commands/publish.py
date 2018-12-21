"""
Commands for publishing tagged Splitgraph images to a remote registry.
"""

import logging
from datetime import datetime

from psycopg2.sql import SQL, Identifier

from splitgraph._data.registry import publish_tag
from splitgraph.commands.push_pull import merge_push_params
from splitgraph.engine import get_engine, switch_engine

PREVIEW_SIZE = 100


def publish(repository, tag, remote_engine_name=None, remote_repository=None, readme="", include_provenance=True,
            include_table_previews=True):
    """
    Summarizes the data on a previously-pushed repository and makes it available in the catalog.

    :param repository: Repository to be published. The repository must exist on the remote.
    :param tag: Image tag to be published.
    :param remote_engine_name: Remote engine or connection string
    :param remote_repository: Remote repository name
    :param readme: Optional README for the repository.
    :param include_provenance: If False, doesn't include the dependencies of the image
    :param include_table_previews: Whether to include data previews for every table in the image.
    """
    remote_engine_name, remote_repository = merge_push_params(repository, remote_engine_name, remote_repository)
    image_hash = repository.get_tagged_id(tag)
    logging.info("Publishing %s:%s (%s)", repository, image_hash, tag)

    image = repository.get_image(image_hash)
    dependencies = [((r.namespace, r.repository), i) for r, i in image.provenance()] \
        if include_provenance else None
    previews, schemata = _prepare_extra_data(image, repository, include_table_previews)

    remote_engine = get_engine(remote_engine_name)
    try:
        with switch_engine(remote_engine_name):
            publish_tag(remote_repository, tag, image_hash, datetime.now(), dependencies, readme, schemata=schemata,
                        previews=previews if include_table_previews else None)
        remote_engine.commit()
    finally:
        remote_engine.close()


def _prepare_extra_data(image, repository, include_table_previews):
    schemata = {}
    previews = {}
    for table_name in image.get_tables():
        if include_table_previews:
            logging.info("Generating preview for %s...", table_name)
            with repository.materialized_table(table_name, image.image_hash) as (tmp_schema, tmp_table):
                engine = get_engine()
                schema = engine.get_full_table_schema(tmp_schema, tmp_table)
                previews[table_name] = engine.run_sql(SQL("SELECT * FROM {}.{} LIMIT %s").format(
                        Identifier(tmp_schema), Identifier(tmp_table)), (PREVIEW_SIZE,))
        else:
            schema = image.get_table_schema(table_name)
        schemata[table_name] = [(cn, ct, pk) for _, cn, ct, pk in schema]
    return previews, schemata
