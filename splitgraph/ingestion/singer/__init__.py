"""Command line tools for building Splitgraph images from Singer taps, including using Splitgraph as a Singer target."""
import io
import sys
from typing import TYPE_CHECKING

import click

from splitgraph.commandline.common import RepositoryType, ImageType

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository
    from target_postgres import DbSync


@click.group(name="singer")
def singer_group():
    """Build Splitgraph images from Singer taps."""


def db_sync_wrapper(repository: "Repository"):
    from target_postgres import DbSync

    class DbSyncProxy(DbSync):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.repository = repository

        def load_csv(self, file, count, size_bytes):
            super().load_csv(file, count, size_bytes)

        #
        # def create_schema_if_not_exists(self, table_columns_cache=None):
        #     # Override to properly quote the schema name
        #
        #     schema_name = self.schema_name
        #
        #     schema_rows = self.query(
        #         "SELECT LOWER(schema_name) schema_name FROM information_schema.schemata WHERE LOWER(schema_name) = %s",
        #         (schema_name.lower(),),
        #     )
        #
        #     if len(schema_rows) == 0:
        #         query = 'CREATE SCHEMA IF NOT EXISTS "{}"'.format(schema_name.replace('"', '""'))
        #         self.logger.info("Schema '%s' does not exist. Creating... %s", schema_name, query)
        #         self.query(query)
        #
        #         self.grant_privilege(schema_name, self.grantees, self.grant_usage_on_schema)

    return DbSyncProxy


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

    repository, hash_or_tag = image

    if not repository_exists(repository):
        repository.init()

    image = repository.images[hash_or_tag]
    image.checkout()
    repository.commit_engines()

    target_postgres.DbSync = db_sync_wrapper(repository)

    conn_params = repository.engine.conn_params

    # TODO this is just a proof of concept: we init the repo, run the ingestion and commit it.
    # Most of the time, the Singer tap doesn't make any changes to historical data or
    # change the schema and so a much faster way here would be to load the data into an
    # object and then attach that object to an existing table.
    #
    # However, this becomes complicated with the way we keep the index and hash the object
    # where we need to have the row's old values + this target sometimes changing the schema.
    # Basically, we want to override some code in DbSync's load_csv to:
    #  * load data into the temporary table (as normal)
    #  * use LQ to get the old values of rows that would be updated / deleted
    #  * hook into some fragment manager code to get fragment hashes and the index
    #  * store the data as an SG table -- but note each DbSync is for a single table so
    #    we need to be able to edit the image
    # Also note that in case of a schema change, we need to check the whole thing out
    # anyway and recommit it. Also, load_csv might get called multiple times per table.

    # Prepare target_postgres config
    config = {
        "host": conn_params["SG_ENGINE_HOST"],
        "port": int(conn_params["SG_ENGINE_PORT"]),
        "user": conn_params["SG_ENGINE_USER"],
        "password": conn_params["SG_ENGINE_PWD"],
        "dbname": conn_params["SG_ENGINE_DB_NAME"],
        "default_target_schema": '"{}"'.format(repository.to_schema().replace('"', '""')),
    }

    singer_messages = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    target_postgres.persist_lines(config, singer_messages)

    # todo here -- unparent the repo? do nothing if no changes?
    repository.commit(comment="Singer tap ingestion", chunk_size=100000, split_changeset=True)


singer_group.add_command(singer_target)
