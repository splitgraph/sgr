"""Command line tools for building Splitgraph images from Singer taps, including using Splitgraph as a Singer target."""
import io
import sys

import click

from splitgraph.commandline.common import ImageType


@click.group(name="singer")
def singer_group():
    """Build Splitgraph images from Singer taps."""


@click.command(name="target")
@click.argument("image", type=ImageType(default="latest", repository_exists=False))
def singer_target(image,):
    """
    Singer target that loads data into Splitgraph images.

    This will read data from the stdin from a Singer-compatible tap and load it into
    a Splitgraph image, merging data if the image already exists.

    Image must be of the format `[NAMESPACE/]REPOSITORY[:HASH_OR_TAG]` where `HASH_OR_TAG`
    is a tag of an existing image to base the image on. If the repository doesn't exist,
    it will be created.
    """
    from splitgraph.core.engine import repository_exists
    import target_postgres

    from splitgraph.ingestion.singer.db_sync import db_sync_wrapper

    from random import getrandbits
    import traceback
    import logging

    repository, hash_or_tag = image

    if not repository_exists(repository):
        repository.init()

    base_image = repository.images[hash_or_tag]

    # Clone the image
    new_image_hash = "{:064x}".format(getrandbits(256))
    repository.images.add(parent_id=None, image=new_image_hash, comment="Singer tap ingestion")

    repository.engine.run_sql(
        "INSERT INTO splitgraph_meta.tables "
        "(SELECT namespace, repository, %s, table_name, table_schema, object_ids "
        "FROM splitgraph_meta.tables "
        "WHERE namespace = %s AND repository = %s AND image_hash = %s)",
        (new_image_hash, repository.namespace, repository.repository, base_image.image_hash,),
    )

    # Build a staging schema
    staging_schema = "sg_tmp_" + repository.to_schema()
    repository.object_engine.delete_schema(staging_schema)
    repository.object_engine.create_schema(staging_schema)
    repository.commit_engines()

    target_postgres.DbSync = db_sync_wrapper(repository.images[new_image_hash], staging_schema)

    conn_params = repository.engine.conn_params

    # Prepare target_postgres config
    config = {
        "host": conn_params["SG_ENGINE_HOST"],
        "port": int(conn_params["SG_ENGINE_PORT"]),
        "user": conn_params["SG_ENGINE_USER"],
        "password": conn_params["SG_ENGINE_PWD"],
        "dbname": conn_params["SG_ENGINE_DB_NAME"],
        "default_target_schema": repository.to_schema(),
    }

    try:
        singer_messages = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
        target_postgres.persist_lines(config, singer_messages)
    except Exception:
        # TODO:
        #   * the handler in persist_lines actually seems to swallow exceptions, so e.g.
        #     a keyboard interrupt still makes us leave garbage behind.
        #   * delete image if no changes?
        #   * option to delete the previous image / cleanup?
        #   * factor this out from the cmdline
        #   * build out a harness to manage state in the sg image as well
        #   * move some of the more generic code into fragment_manager?
        #   * transient rollback errors in test
        repository.rollback_engines()
        repository.images.delete([new_image_hash])
        repository.commit_engines()
        logging.error(traceback.format_exc())
        raise
    finally:
        repository.object_engine.delete_schema(staging_schema)
        repository.commit_engines()


singer_group.add_command(singer_target)
