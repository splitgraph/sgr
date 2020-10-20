"""Command line tools for building Splitgraph images from Singer taps, including using Splitgraph as a Singer target."""
import io
import sys
from typing import TYPE_CHECKING, Optional, Tuple, cast, List, Any, Dict

import click
from psycopg2.sql import SQL, Identifier

from splitgraph.commandline.common import ImageType
from splitgraph.core.table import Table
from splitgraph.core.types import TableSchema, TableColumn, Changeset
from splitgraph.engine.postgres.engine import get_change_key
from splitgraph.exceptions import TableNotFoundError
from splitgraph.ingestion.common import merge_tables

if TYPE_CHECKING:
    from splitgraph.core.image import Image
    from target_postgres import DbSync


@click.group(name="singer")
def singer_group():
    """Build Splitgraph images from Singer taps."""


def db_sync_wrapper(image: "Image"):
    from target_postgres import DbSync
    from target_postgres.db_sync import stream_name_to_dict, primary_column_names, column_type

    class DbSyncProxy(DbSync):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

            # The structure here is that we edit an image and write / modify tables in it. This
            # is supposed to be called with an image already existing.
            self.image = image

            self.staging_table: Optional[Tuple[str, str]] = None

        def _sg_schema(self) -> TableSchema:
            stream_schema_message = self.stream_schema_message
            primary_key = (
                primary_column_names(stream_schema_message)
                if len(stream_schema_message["key_properties"]) > 0
                else []
            )

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
                # TODO see if this works
                # self.staging_table = ("pg_temp", "staging_" + self._table_name())
                # self.logger.info("Creating a staging table at %s.%s", *self.staging_table)
                # self.image.repository.object_engine.create_table(
                #     schema=None,
                #     table=self.staging_table[1],
                #     schema_spec=schema_spec,
                #     temporary=True,
                # )
                # Make an empty table (will replace later on when flushing the data)
                self.image.repository.objects.register_tables(
                    self.image.repository,
                    [(self.image.image_hash, self._table_name(), schema_spec, [])],
                )
                self.image.repository.commit_engines()

                return

            if table.table_schema != self._sg_schema():
                # Materialize the table into a temporary location and update its schema
                self.staging_table = ("pg_temp", "staging_" + self._table_name())
                self.logger.info(
                    "Schema mismatch, materializing the table into %s.%s and migrating",
                    *self.staging_table,
                )
                table.materialize(self.staging_table[0], None, temporary=True)

                old_cols = {c.name: c.pg_type for c in table.table_schema}
                new_cols = {c.name: c.pg_type for c in schema_spec}

                for c in old_cols:
                    if c not in new_cols:
                        table.repository.object_engine.run_sql(
                            SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(
                                Identifier(self.staging_table[0]),
                                Identifier(self.staging_table[1]),
                                Identifier(c),
                            )
                        )
                for c in new_cols:
                    if c not in old_cols:
                        table.repository.object_engine.run_sql(
                            SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                                Identifier(self.staging_table[0]),
                                Identifier(self.staging_table[1]),
                                Identifier(c),
                                Identifier(new_cols[c]),
                            )
                        )
                    elif new_cols[c] != old_cols[c]:
                        table.repository.object_engine.run_sql(
                            SQL("ALTER TABLE {}.{} ALTER COLUMN {} TYPE {}").format(
                                Identifier(self.staging_table[0]),
                                Identifier(self.staging_table[1]),
                                Identifier(c),
                                Identifier(new_cols[c]),
                            )
                        )

        def _build_fake_changeset(self, old_table: Table, schema: str, table: str) -> Changeset:
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

            with old_table.image.query_schema(commit=False) as s:
                # Query:
                # SELECT (new, pk, columns) AS pk,
                #        (if sdc_removed_at timestamp exists, then the row has been deleted),
                #        (row_to_json(old non-pk cols)) AS old_row
                # FROM new_table n LEFT OUTER JOIN old_table o ON [o.pk = n.pk]
                query = (
                    SQL("SELECT (")
                    + SQL(",").join(SQL("n.") + Identifier(c) for c in change_key)
                    + SQL(") AS pk, ")
                    + SQL(
                        ("_sdc_removed_at IS NOT DISTINCT FROM NULL" if hard_delete else "TRUE ")
                        + "AS upserted, "
                    )
                    + SQL("row_to_json((")
                    + SQL(",").join(
                        SQL("o.") + Identifier(c.name)
                        for c in schema_spec
                        if c.name not in change_key
                    )
                    + SQL(")) AS old_row FROM {}.{} n LEFT OUTER JOIN {}.{} o ON ").format(
                        Identifier(schema),
                        Identifier(table),
                        Identifier(s),
                        Identifier(old_table.table_name),
                    )
                    + SQL(" AND ").join(
                        SQL("o.{0} = n.{0}").format(Identifier(c)) for c in change_key
                    )
                ).as_string(self.image.repository.object_engine.connection)
                self.logger.info(query)

                result = cast(
                    List[Tuple[Tuple, bool, Dict[str, Any]]],
                    self.image.repository.object_engine.run_sql(query),
                )

            # TODO this issues updates and raises an AssertionError somewhere in sg at commit time
            return {pk: (upserted, old_row, {}) for (pk, upserted, old_row) in result}

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
                        schema_spec,
                        [object_id],
                        self.image.repository.namespace,
                        self.image.repository.repository,
                        self.image.image_hash,
                        table_name,
                    ),
                )
                self.staging_table = None

            # Commit (deletes temporary tables)
            self.image.repository.objects.commit_engines()

        def _merge_existing_table(self, old_table, temp_table):
            assert self._sg_schema() == old_table.table_schema
            # Find PKs that have been upserted and deleted (make fake changeset)
            changeset = self._build_fake_changeset(old_table, "pg_temp", temp_table)

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
                old_table, [changeset], "pg_temp",
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

    repository.commit_engines()

    # TODO here:
    #   sync_table is called from a different thread than read_csv, so we can't
    #   materialize into a temporary table there. Maybe create a schema here,
    #   pass that to the engine to be used as a workspace, delete it at the end?
    #   Could also just recommit/resnap again immediately after a schema change?

    target_postgres.DbSync = db_sync_wrapper(repository.images[new_image_hash])

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

    singer_messages = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    target_postgres.persist_lines(config, singer_messages)


singer_group.add_command(singer_target)
