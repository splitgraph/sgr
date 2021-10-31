import io
import itertools
import logging
import sys
import traceback
from operator import itemgetter
from typing import BinaryIO, Optional, TextIO, Tuple

import target_postgres
from splitgraph.config import CONFIG
from splitgraph.core.image import Image
from splitgraph.core.repository import Repository
from splitgraph.core.types import TableColumn, TableSchema
from splitgraph.engine.postgres.engine import get_change_key
from splitgraph.exceptions import TableNotFoundError
from splitgraph.ingestion.common import merge_tables
from target_postgres import DbSync
from target_postgres.db_sync import column_type, flatten_key

from .common import _make_changeset, _migrate_schema, log_exception, rollback_at_end


def select_breadcrumb(stream_message, breadcrumb):
    for sub_meta in stream_message["metadata"]:
        if sub_meta["breadcrumb"] == breadcrumb:
            return sub_meta["metadata"]

    raise ValueError("Breadcrumb %s not found!" % breadcrumb)


def get_key_properties(stream_message):
    """Extract the PK from a stream message. Supports both legacy ("key_properties") and
    new ("metadata") Singer taps."""
    if "key_properties" in stream_message:
        return stream_message["key_properties"]

    return select_breadcrumb(stream_message, []).get("table-key-properties", [])


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
        return _get_sg_schema(self.flatten_schema, get_key_properties(stream_schema_message))

    def create_indices(self, stream):
        pass

    @rollback_at_end
    def sync_table(self):
        # NB the overridden method never calls self.update_columns() to bring the
        # schema up to date: this is because it compares a quoted name of the
        # table to the unquoted names returned by self.get_tables().

        schema_spec = self._sg_schema()

        # See if the table exists
        try:
            table = self.image.get_table(get_table_name(self.stream_schema_message))
        except TableNotFoundError:
            self.staging_table = (
                self.staging_schema,
                "staging_" + get_table_name(self.stream_schema_message),
            )
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
                [
                    (
                        self.image.image_hash,
                        get_table_name(self.stream_schema_message),
                        schema_spec,
                        [],
                    )
                ],
            )
            self.image.repository.commit_engines()

            return

        if table.table_schema != schema_spec:
            # Materialize the table into a temporary location and update its schema
            self.staging_table = (
                self.staging_schema,
                "staging_" + get_table_name(self.stream_schema_message),
            )
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
    @rollback_at_end
    def load_csv(self, file, count, size_bytes):
        from splitgraph.ingestion.csv import copy_csv_buffer

        table_name = get_table_name(self.stream_schema_message)
        schema_spec = self._sg_schema()
        temp_table = "tmp_" + table_name
        self.logger.info("Loading %d rows into '%s'", count, table_name)

        old_table = self.image.get_table(table_name)

        self.image.repository.object_engine.create_table(
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
        assert self.staging_table
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
        with old_table.image.query_schema(commit=False):
            # If hard_delete is set, check the sdc_removed_at flag in the table: if it's
            # not NULL, we mark rows as deleted instead.
            hard_delete = bool(self.connection_config.get("hard_delete"))

            # Find PKs that have been upserted and deleted (make fake changeset)
            changeset = _make_changeset(
                self.image.object_engine,
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
                get_table_name(self.stream_schema_message),
                inserted,
                updated,
                deleted,
            )

            # Split the changeset according to the original fragment boundaries so that a single
            # change spanning multiple fragments doesn't force materialization.
            split_changesets = self.image.repository.objects.split_changeset_boundaries(
                changeset, get_change_key(old_table.table_schema), old_table.objects
            )
            self.logger.info(
                "Table %s: split changeset into %d fragment(s)",
                get_table_name(self.stream_schema_message),
                len(split_changesets),
            )

            # Store the changeset as a new SG object
            object_ids = self.image.repository.objects._store_changesets(
                old_table, split_changesets, "pg_temp", table_name=temp_table
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


def _get_sg_schema(flattened_schema, primary_key) -> TableSchema:
    return [
        TableColumn(i, name, column_type(schema_property), name in primary_key, None)
        for i, (name, schema_property) in enumerate(flattened_schema.items())
    ]


# Taken from target-postgres and adapted to not crash on unsupported columns
def _flatten_schema(d, parent_key=None, sep="__", level=0, max_level=0):
    if parent_key is None:
        parent_key = []

    items = []

    if "properties" not in d:
        return {}

    for k, v in d["properties"].items():
        new_key = flatten_key(k, parent_key, sep)
        if "type" in v:
            if "object" in v["type"] and "properties" in v and level < max_level:
                items.extend(
                    _flatten_schema(
                        v, parent_key + [k], sep=sep, level=level + 1, max_level=max_level
                    ).items()
                )
            else:
                items.append((new_key, v))
        else:
            if len(v.values()) > 0:
                if v.get("inclusion") == "unsupported":
                    logging.warning("Unsupported field %s: %s", new_key, v.get("description", ""))
                    continue
                if list(v.values())[0][0]["type"] == "string":
                    list(v.values())[0][0]["type"] = ["null", "string"]
                    items.append((new_key, list(v.values())[0][0]))
                elif list(v.values())[0][0]["type"] == "array":
                    list(v.values())[0][0]["type"] = ["null", "array"]
                    items.append((new_key, list(v.values())[0][0]))
                elif list(v.values())[0][0]["type"] == "object":
                    list(v.values())[0][0]["type"] = ["null", "object"]
                    items.append((new_key, list(v.values())[0][0]))

    sorted_items = sorted(items, key=itemgetter(0))
    for k, g in itertools.groupby(sorted_items, key=itemgetter(0)):
        if len(list(g)) > 1:
            raise ValueError("Duplicate column name produced in schema: {}".format(k))

    return dict(sorted_items)


def get_sg_schema(stream_schema_message, flattening_max_level=0):
    return _get_sg_schema(
        _flatten_schema(stream_schema_message["schema"], max_level=flattening_max_level),
        get_key_properties(stream_schema_message),
    )


def get_table_name(stream_schema_message):
    return stream_schema_message["stream"].replace(".", "_").replace("-", "_").lower()


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
    input_stream: Optional[BinaryIO] = None,
    output_stream: Optional[TextIO] = None,
):
    input_stream = input_stream or sys.stdin.buffer

    # Build a staging schema
    staging_schema = "sg_tmp_" + repository.to_schema()
    repository.object_engine.delete_schema(staging_schema)
    repository.object_engine.create_schema(staging_schema)
    repository.commit_engines()

    config = _prepare_config_params(repository)
    old_sync = target_postgres.DbSync

    stdout = sys.stdout
    target_postgres.DbSync = db_sync_wrapper(repository.images[new_image_hash], staging_schema)
    if output_stream:
        sys.stdout = output_stream
    try:
        singer_messages = io.TextIOWrapper(input_stream, encoding="utf-8")
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
        sys.stdout = stdout
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
        "max_parallelism": _calc_max_threads(conn_params),
    }
    return config


def _calc_max_threads(conn_params):
    """Each loader thread really uses 2 connections (one directly when communicating with
    the engine, one from the in-engine Splitgraph during querying) and this can lead to
    weird deadlocks when we exhaust the connection limit enforced by pgbouncer) -- to avoid
    that, we decrease the number of concurrent loader threads."""
    return max(int(conn_params.get("SG_ENGINE_POOL", CONFIG["SG_ENGINE_POOL"])) // 2 - 1, 1)
