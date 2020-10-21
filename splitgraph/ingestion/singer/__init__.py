"""Command line tools for building Splitgraph images from Singer taps, including using Splitgraph as a Singer target."""
import io
import logging
import sys
import traceback
from functools import wraps
from typing import TYPE_CHECKING, Optional, Tuple

import click
from psycopg2._json import Json
from psycopg2.sql import SQL, Identifier

from splitgraph.commandline.common import ImageType
from splitgraph.core.types import TableSchema, TableColumn, Changeset
from splitgraph.engine.postgres.engine import get_change_key
from splitgraph.exceptions import TableNotFoundError
from splitgraph.ingestion.common import merge_tables

if TYPE_CHECKING:
    from splitgraph.core.image import Image
    from target_postgres import DbSync


def log_exception(f):
    """Emit exceptions with full traceback instead of just the error text"""

    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception:
            logging.error(traceback.format_exc())
            raise

    return wrapped


@click.group(name="singer")
def singer_group():
    """Build Splitgraph images from Singer taps."""


def db_sync_wrapper(image: "Image", staging_schema: str):
    from target_postgres import DbSync
    from target_postgres.db_sync import column_type

    class DbSyncProxy(DbSync):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            # The structure here is that we edit an image and write / modify tables in it. This
            # is supposed to be called with an image already existing.
            self.image = image
            self.staging_schema = staging_schema

            self.staging_table: Optional[Tuple[str, str]] = None

        def _sg_schema(self) -> TableSchema:
            stream_schema_message = self.stream_schema_message
            primary_key = stream_schema_message["key_properties"]

            return [
                TableColumn(i, name, column_type(schema_property), name in primary_key, None)
                for i, (name, schema_property) in enumerate(self.flatten_schema.items())
            ]

        def create_indices(self, stream):
            pass

        def _table_name(self):
            return self.stream_schema_message["stream"].replace(".", "_").replace("-", "_").lower()

        def sync_table(self):
            # NB the overridden method never calls self.update_columns() to bring the
            # schema up to date: this is because it compares a quoted name of the
            # table to the unquoted names returned by self.get_tables().

            schema_spec = self._sg_schema()

            # See if the table exists
            try:
                table = self.image.get_table(self._table_name())
            except TableNotFoundError:
                self.staging_table = (self.staging_schema, "staging_" + self._table_name())
                self.logger.info("Creating a staging table at %s.%s", *self.staging_table)
                self.image.repository.object_engine.create_table(
                    schema=self.staging_table[0],
                    table=self.staging_table[1],
                    schema_spec=schema_spec,
                    unlogged=True,
                )
                # Make an empty table (will replace later on when flushing the data)
                self.image.repository.objects.register_tables(
                    self.image.repository,
                    [(self.image.image_hash, self._table_name(), schema_spec, [])],
                )
                self.image.repository.commit_engines()

                return

            if table.table_schema != schema_spec:
                # Materialize the table into a temporary location and update its schema
                self.staging_table = (self.staging_schema, "staging_" + self._table_name())
                self.logger.info(
                    "Schema mismatch, materializing the table into %s.%s and migrating",
                    *self.staging_table,
                )
                table.materialize(self.staging_table[1], self.staging_table[0])

                _migrate_schema(
                    self.image.repository.object_engine,
                    self.staging_table[0],
                    self.staging_table[1],
                    table.table_schema,
                    schema_spec,
                )
                self.image.repository.commit_engines()

        def _build_fake_changeset(
            self, old_schema: str, old_table: str, schema: str, table: str
        ) -> Changeset:
            """Build a fake changeset from the temporary table and the existing table to pass
            to the object manager (store as a Splitgraph diff)."""
            schema_spec = self._sg_schema()

            # PK -> (upserted / deleted, old row, new row)
            # As a memory-saving hack, we only record the values of the old row (read from the
            # current table) -- this is because object creation routines read the inserted rows
            # from the staging table anyway.

            change_key = [c for c, _ in get_change_key(schema_spec)]

            # If hard_delete is set, check the sdc_removed_at flag in the table: if it's
            # not NULL, we mark rows as deleted instead.
            hard_delete = bool(self.connection_config.get("hard_delete"))

            # Query:
            # SELECT (new, pk, columns) AS pk,
            #        (if sdc_removed_at timestamp exists, then the row has been deleted),
            #        (row_to_json(old non-pk cols)) AS old_row
            # FROM new_table n LEFT OUTER JOIN old_table o ON [o.pk = n.pk]
            query = (
                SQL("SELECT ")
                + SQL(",").join(SQL("n.") + Identifier(c) for c in change_key)
                + SQL(",")
                + SQL(
                    ("_sdc_removed_at IS NOT DISTINCT FROM NULL" if hard_delete else "TRUE ")
                    + "AS upserted, "
                )
                # If PK doesn't exist in the new table, old_row is null, else output it
                + SQL("CASE WHEN ")
                + SQL(" AND ").join(SQL("o.{0} IS NULL").format(Identifier(c)) for c in change_key)
                + SQL(" THEN '{}'::json ELSE json_build_object(")
                + SQL(",").join(
                    SQL("%s, o.") + Identifier(c.name)
                    for c in schema_spec
                    if c.name not in change_key
                )
                + SQL(") END AS old_row FROM {}.{} n LEFT OUTER JOIN {}.{} o ON ").format(
                    Identifier(schema),
                    Identifier(table),
                    Identifier(old_schema),
                    Identifier(old_table),
                )
                + SQL(" AND ").join(SQL("o.{0} = n.{0}").format(Identifier(c)) for c in change_key)
                + SQL("WHERE o.* != n.*")
            ).as_string(self.image.repository.object_engine.connection)
            args = [c.name for c in schema_spec if c.name not in change_key]

            result = self.image.repository.object_engine.run_sql(query, args)
            return {tuple(row[:-2]): (row[-2], row[-1], {}) for row in result}

        @log_exception
        def load_csv(self, file, count, size_bytes):
            from splitgraph.ingestion.csv import copy_csv_buffer

            table_name = self._table_name()
            schema_spec = self._sg_schema()
            temp_table = "tmp_" + table_name
            self.logger.info("Loading %d rows into '%s'", count, table_name)

            old_table = self.image.get_table(table_name)

            self.image.repository.engine.create_table(
                schema="pg_temp", table=temp_table, schema_spec=schema_spec, temporary=True
            )

            with open(file, "rb") as f:
                copy_csv_buffer(
                    data=f,
                    engine=self.image.repository.object_engine,
                    schema="pg_temp",
                    table=temp_table,
                    no_header=True,
                    escape="\\",
                )
            schema_spec = self._sg_schema()
            if not self.staging_table:
                self._merge_existing_table(old_table, temp_table)
            else:
                # Kind of like store_table_as_snap
                staging_table_schema, staging_table = self.staging_table
                # Merge pg_temp.temp_table into the staging table
                merge_tables(
                    self.image.object_engine,
                    "pg_temp",
                    temp_table,
                    schema_spec,
                    staging_table_schema,
                    staging_table,
                    schema_spec,
                )

                object_id = self.image.repository.objects.create_base_fragment(
                    staging_table_schema,
                    staging_table,
                    self.image.repository.namespace,
                    table_schema=schema_spec,
                )

                # Overwrite the existing table
                self.image.engine.run_sql(
                    "UPDATE splitgraph_meta.tables "
                    "SET table_schema = %s, object_ids = %s "
                    "WHERE namespace = %s AND repository = %s AND image_hash = %s "
                    "AND table_name = %s",
                    (
                        Json(schema_spec),
                        [object_id],
                        self.image.repository.namespace,
                        self.image.repository.repository,
                        self.image.image_hash,
                        table_name,
                    ),
                )
                self.staging_table = None

            # Commit (deletes temporary tables)
            self.image.repository.commit_engines()

        def _merge_existing_table(self, old_table, temp_table):
            assert self._sg_schema() == old_table.table_schema
            # Find PKs that have been upserted and deleted (make fake changeset)
            with old_table.image.query_schema(commit=False) as s:
                changeset = self._build_fake_changeset(
                    s, old_table.table_name, "pg_temp", temp_table
                )

                inserted = sum(1 for v in changeset.values() if v[0] and not v[1])
                updated = sum(1 for v in changeset.values() if v[0] and v[1])
                deleted = sum(1 for v in changeset.values() if not v[0])
                self.logger.info(
                    "Table %s: inserted %d, updated %d, deleted %d",
                    self._table_name(),
                    inserted,
                    updated,
                    deleted,
                )

                # Store the changeset as a new SG object
                object_ids = self.image.repository.objects._store_changesets(
                    old_table, [changeset], "pg_temp", table_name=temp_table
                )

            # Add the new object to the table.
            # add_table (called by register_tables) does that by default (appends
            # the object to the table if it already exists)
            self.image.repository.objects.register_tables(
                self.image.repository,
                [
                    (
                        self.image.image_hash,
                        old_table.table_name,
                        old_table.table_schema,
                        object_ids,
                    )
                ],
            )

        def delete_rows(self, stream):
            # We delete rows in load_csv if required (by putting them into the DIFF as
            # upserted=False).
            pass

        def create_schema_if_not_exists(self, table_columns_cache=None):
            pass

    return DbSyncProxy


def _migrate_schema(engine, table_schema, table_name, table_schema_spec, new_schema_spec):
    """Migrate the schema of a table to match the schema_spec"""

    old_cols = {c.name: c.pg_type for c in table_schema_spec}
    new_cols = {c.name: c.pg_type for c in new_schema_spec}
    for c in old_cols:
        if c not in new_cols:
            engine.run_sql(
                SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(
                    Identifier(table_schema), Identifier(table_name), Identifier(c),
                )
            )
    for c in new_cols:
        if c not in old_cols:
            engine.run_sql(
                SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                    Identifier(table_schema),
                    Identifier(table_name),
                    Identifier(c),
                    Identifier(new_cols[c]),
                )
            )
        elif new_cols[c] != old_cols[c]:
            engine.run_sql(
                SQL("ALTER TABLE {}.{} ALTER COLUMN {} TYPE {}").format(
                    Identifier(table_schema),
                    Identifier(table_name),
                    Identifier(c),
                    Identifier(new_cols[c]),
                )
            )


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
        # TODO the handler in persist_lines actually seems to swallow exceptions, so e.g.
        #   a keyboard interrupt still makes us leave garbage behind.
        # TODO also: delete image if no changes?
        repository.rollback_engines()
        repository.images.delete([new_image_hash])
        repository.commit_engines()
        logging.error(traceback.format_exc())
        raise
    finally:
        repository.object_engine.delete_schema(staging_schema)
        repository.commit_engines()


singer_group.add_command(singer_target)
