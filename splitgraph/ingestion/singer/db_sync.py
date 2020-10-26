import io
import logging
import sys
import traceback
from typing import Optional, Tuple

import target_postgres
from target_postgres import DbSync
from target_postgres.db_sync import column_type

from splitgraph.core.types import TableSchema, TableColumn
from splitgraph.exceptions import TableNotFoundError
from splitgraph.ingestion.common import merge_tables
from ._utils import _migrate_schema, log_exception, _make_changeset
from ...core.image import Image
from ...core.repository import Repository


class DbSyncProxy(DbSync):
    def __init__(self, *args, **kwargs):
        # The structure here is that we edit an image and write / modify tables in it. This
        # is supposed to be called with an image already existing.
        self.image = kwargs.pop("image")
        self.staging_schema = kwargs.pop("staging_schema")

        super().__init__(*args, **kwargs)

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
            self._overwrite_existing_table(temp_table, table_name, schema_spec)

        # Commit (deletes temporary tables)
        self.image.repository.commit_engines()

    def _overwrite_existing_table(self, temp_table, table_name, schema_spec):
        staging_table_schema, staging_table = self.staging_table

        # Merge the CSV data into the "materialized" table
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
        self.image.repository.objects.overwrite_table(
            self.image.repository, self.image.image_hash, table_name, schema_spec, [object_id]
        )
        self.staging_table = None

    def _merge_existing_table(self, old_table, temp_table):
        # Load the data directly as a Splitgraph object. Note that this can still be
        # improved: currently the Singer loader loads the stream as a CSV, gives it
        # to us, we ingest it into a temporary table, generate a changeset from that
        # (which goes back into Python), give it to the fragment manager, the fragment
        # manager builds an object by loading it into a temporary table _again_ and then
        # into a cstore_fdw file.

        assert self._sg_schema() == old_table.table_schema
        with old_table.image.query_schema(commit=False) as s:
            # If hard_delete is set, check the sdc_removed_at flag in the table: if it's
            # not NULL, we mark rows as deleted instead.
            hard_delete = bool(self.connection_config.get("hard_delete"))

            # Find PKs that have been upserted and deleted (make fake changeset)
            changeset = _make_changeset(
                self.image.object_engine,
                s,
                old_table.table_name,
                "pg_temp",
                temp_table,
                old_table.table_schema,
                upsert_condition="_sdc_removed_at IS NOT DISTINCT FROM NULL"
                if hard_delete
                else "TRUE",
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
            [(self.image.image_hash, old_table.table_name, old_table.table_schema, object_ids,)],
        )

    def delete_rows(self, stream):
        # We delete rows in load_csv if required (by putting them into the DIFF as
        # upserted=False).
        pass

    def create_schema_if_not_exists(self, table_columns_cache=None):
        pass


def db_sync_wrapper(image: "Image", staging_schema: str):
    def wrapped(*args, **kwargs):
        return DbSyncProxy(image=image, staging_schema=staging_schema, *args, **kwargs)

    return wrapped


def run_patched_sync(
    repository: Repository,
    base_image: Optional[Image],
    new_image_hash: str,
    delete_old: bool,
    failure: str,
):
    # Build a staging schema
    staging_schema = "sg_tmp_" + repository.to_schema()
    repository.object_engine.delete_schema(staging_schema)
    repository.object_engine.create_schema(staging_schema)
    repository.commit_engines()

    config = _prepare_config_params(repository)
    old_sync = target_postgres.DbSync

    try:
        target_postgres.DbSync = db_sync_wrapper(repository.images[new_image_hash], staging_schema)
        singer_messages = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
        target_postgres.persist_lines(config, singer_messages)
        if delete_old and base_image:
            repository.images.delete([base_image.image_hash])
    except Exception:
        repository.rollback_engines()
        if failure == "delete-new":
            repository.images.delete([new_image_hash])
        elif failure == "delete-old" and base_image:
            repository.images.delete([base_image.image_hash])
        repository.commit_engines()
        logging.error(traceback.format_exc())
        raise
    finally:
        target_postgres.DbSync = old_sync
        repository.object_engine.delete_schema(staging_schema)
        repository.commit_engines()


def _prepare_config_params(repository):
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
    return config
