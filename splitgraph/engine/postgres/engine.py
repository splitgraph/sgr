"""Default Splitgraph engine: uses PostgreSQL to store metadata and actual objects and an audit stored procedure
to track changes, as well as the Postgres FDW interface to upload/download objects to/from other Postgres engines."""
import itertools
import json
import logging
from contextlib import contextmanager
from io import BytesIO, TextIOWrapper
from pathlib import PurePosixPath
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union, cast

import psycopg2
import psycopg2.extensions
from psycopg2.sql import SQL, Composed, Identifier
from tqdm import tqdm

from splitgraph.config import SG_CMD_ASCII, SPLITGRAPH_META_SCHEMA
from splitgraph.core.types import TableColumn, TableSchema
from splitgraph.engine import ResultShape
from splitgraph.engine.base import ChangeEngine, ObjectEngine, validate_type
from splitgraph.engine.config import switch_engine
from splitgraph.engine.postgres.psycopg import _AUDIT_SCHEMA, PsycopgEngine, chunk
from splitgraph.exceptions import IncompleteObjectDownloadError, ObjectMountingError
from splitgraph.hooks.mount_handlers import mount_postgres

CSTORE_SERVER = "cstore_server"
_PACKAGE = "splitgraph"
ROW_TRIGGER_NAME = "audit_trigger_row"
STM_TRIGGER_NAME = "audit_trigger_stm"
REMOTE_TMP_SCHEMA = "tmp_remote_data"
SG_UD_FLAG = "sg_ud_flag"


# PG types we can run max/min/comparisons on

PG_INDEXABLE_TYPES = [
    "bigint",
    "bigserial",
    "bit",
    "character",
    "character varying",
    "cidr",
    "date",
    "double precision",
    "inet",
    "integer",
    "money",
    "numeric",
    "real",
    "smallint",
    "smallserial",
    "serial",
    "text",
    "time",
    "time without time zone",
    "time with time zone",
    "timestamp",
    "timestamp without time zone",
    "timestamp with time zone",
]


def _get(d: Dict[str, Optional[str]], k: str) -> str:
    result = d.get(k)
    assert result
    return result


def get_conn_str(conn_params: Dict[str, Optional[str]]) -> str:
    server, port, username, password, dbname = (
        _get(conn_params, "SG_ENGINE_HOST"),
        _get(conn_params, "SG_ENGINE_PORT"),
        _get(conn_params, "SG_ENGINE_USER"),
        _get(conn_params, "SG_ENGINE_PWD"),
        _get(conn_params, "SG_ENGINE_DB_NAME"),
    )
    return f"postgresql://{username}:{password}@{server}:{port}/{dbname}"


def _quote_ident(val: str) -> str:
    return '"%s"' % val.replace('"', '""')


class AuditTriggerChangeEngine(PsycopgEngine, ChangeEngine):
    """Change tracking based on an audit trigger stored procedure"""

    def get_tracked_tables(self) -> List[Tuple[str, str]]:
        """Return a list of tables that the audit trigger is working on."""
        return cast(
            List[Tuple[str, str]],
            self.run_sql(
                "SELECT DISTINCT event_object_schema, event_object_table "
                "FROM information_schema.triggers WHERE trigger_name IN (%s, %s)",
                (ROW_TRIGGER_NAME, STM_TRIGGER_NAME),
            ),
        )

    def track_tables(self, tables: List[Tuple[str, str]]) -> None:
        """Install the audit trigger on the required tables"""
        self.run_sql(
            SQL(";").join(
                itertools.repeat(
                    SQL("SELECT {}.audit_table(%s)").format(Identifier(_AUDIT_SCHEMA)), len(tables)
                )
            ),
            ["{}.{}".format(_quote_ident(s), _quote_ident(t)) for s, t in tables],
        )

    def untrack_tables(self, tables: List[Tuple[str, str]]) -> None:
        """Remove triggers from tables and delete their pending changes"""
        for trigger in (ROW_TRIGGER_NAME, STM_TRIGGER_NAME):
            self.run_sql(
                SQL(";").join(
                    SQL("DROP TRIGGER IF EXISTS {} ON {}.{}").format(
                        Identifier(trigger), Identifier(s), Identifier(t)
                    )
                    for s, t in tables
                )
            )
        # Delete the actual logged actions for untracked tables
        self.run_sql_batch(
            SQL("DELETE FROM {}.logged_actions WHERE schema_name = %s AND table_name = %s").format(
                Identifier(_AUDIT_SCHEMA)
            ),
            tables,
        )

    def has_pending_changes(self, schema: str) -> bool:
        """
        Return True if the tracked schema has pending changes and False if it doesn't.
        """
        return (
            self.run_sql(
                SQL("SELECT 1 FROM {}.{} WHERE schema_name = %s").format(
                    Identifier(_AUDIT_SCHEMA), Identifier("logged_actions")
                ),
                (schema,),
                return_shape=ResultShape.ONE_ONE,
            )
            is not None
        )

    def discard_pending_changes(self, schema: str, table: Optional[str] = None) -> None:
        """
        Discard recorded pending changes for a tracked schema / table
        """
        query = SQL("DELETE FROM {}.{} WHERE schema_name = %s").format(
            Identifier(_AUDIT_SCHEMA), Identifier("logged_actions")
        )

        if table:
            self.run_sql(
                query + SQL(" AND table_name = %s"), (schema, table), return_shape=ResultShape.NONE
            )
        else:
            self.run_sql(query, (schema,), return_shape=ResultShape.NONE)

    def get_pending_changes(
        self, schema: str, table: str, aggregate: bool = False
    ) -> Union[
        List[Tuple[int, int]], List[Tuple[Tuple[str, ...], bool, Dict[str, Any], Dict[str, Any]]]
    ]:
        """
        Return pending changes for a given tracked table

        :param schema: Schema the table belongs to
        :param table: Table to return changes for
        :param aggregate: Whether to aggregate changes or return them completely
        :return: If aggregate is True: List of tuples of (change_type, number of rows).
            If aggregate is False: List of (primary_key, change_type, change_data)
        """
        if aggregate:
            return [
                (_KIND[k], c)
                for k, c in self.run_sql(
                    SQL(
                        "SELECT action, count(action) FROM {}.{} "
                        "WHERE schema_name = %s AND table_name = %s GROUP BY action"
                    ).format(Identifier(_AUDIT_SCHEMA), Identifier("logged_actions")),
                    (schema, table),
                )
            ]

        ri_cols, _ = zip(*self.get_change_key(schema, table))
        result: List[Tuple[Tuple, bool, Dict, Dict]] = []
        for action, row_data, changed_fields in self.run_sql(
            SQL(
                "SELECT action, row_data, changed_fields FROM {}.{} "
                "WHERE schema_name = %s AND table_name = %s"
            ).format(Identifier(_AUDIT_SCHEMA), Identifier("logged_actions")),
            (schema, table),
        ):
            result.extend(_convert_audit_change(action, row_data, changed_fields, ri_cols))
        return result

    def get_changed_tables(self, schema: str) -> List[str]:
        """Get list of tables that have changed content"""
        return cast(
            List[str],
            self.run_sql(
                SQL(
                    """SELECT DISTINCT(table_name) FROM {}.{}
                               WHERE schema_name = %s"""
                ).format(Identifier(_AUDIT_SCHEMA), Identifier("logged_actions")),
                (schema,),
                return_shape=ResultShape.MANY_ONE,
            ),
        )


class PostgresEngine(AuditTriggerChangeEngine, ObjectEngine):
    """An implementation of the Postgres engine for Splitgraph"""

    def get_object_schema(self, object_id: str) -> "TableSchema":
        result: "TableSchema" = []

        call_result = self.run_api_call("get_object_schema", object_id)
        if isinstance(call_result, str):
            call_result = json.loads(call_result)

        for ordinal, column_name, column_type, is_pk in call_result:
            assert isinstance(ordinal, int)
            assert isinstance(column_name, str)
            assert isinstance(column_type, str)
            assert isinstance(is_pk, bool)
            result.append(TableColumn(ordinal, column_name, column_type, is_pk))

        return result

    def _set_object_schema(self, object_id: str, schema_spec: "TableSchema") -> None:
        # Drop the comments from the schema spec (not stored in the object schema file).
        schema_spec = [s[:4] for s in schema_spec]
        self.run_api_call("set_object_schema", object_id, json.dumps(schema_spec))

    def dump_object_creation(
        self,
        object_id: str,
        schema: str,
        table: Optional[str] = None,
        schema_spec: Optional["TableSchema"] = None,
        if_not_exists: bool = False,
    ) -> bytes:
        """
        Generate the SQL that remounts a foreign table pointing to a Splitgraph object.

        :param object_id: Name of the object
        :param schema: Schema to create the table in
        :param table: Name of the table to mount
        :param schema_spec: Schema of the table
        :param if_not_exists: Add IF NOT EXISTS to the DDL
        :return: SQL in bytes format.
        """
        table = table or object_id

        if not schema_spec:
            schema_spec = self.get_object_schema(object_id)
        query = SQL(
            "CREATE FOREIGN TABLE " + ("IF NOT EXISTS " if if_not_exists else "") + "{}.{} ("
        ).format(Identifier(schema), Identifier(table))
        query += SQL(",".join("{} %s " % validate_type(col.pg_type) for col in schema_spec)).format(
            *(Identifier(col.name) for col in schema_spec)
        )
        # foreign tables/cstore don't support PKs
        query += SQL(") SERVER {} OPTIONS (compression %s, filename %s)").format(
            Identifier(CSTORE_SERVER)
        )

        object_path = self.conn_params["SG_ENGINE_OBJECT_PATH"]
        assert object_path is not None

        with self.connection.cursor() as cur:
            return cast(
                bytes, cur.mogrify(query, ("pglz", str(PurePosixPath(object_path, object_id))))
            )

    def dump_object(self, object_id: str, stream: TextIOWrapper, schema: str) -> None:
        schema_spec = self.get_object_schema(object_id)
        stream.write(
            self.dump_object_creation(object_id, schema=schema, schema_spec=schema_spec).decode(
                "utf-8"
            )
        )
        stream.write(";\n")
        with self.connection.cursor() as cur:
            # Since we can't write into the CStore table directly, we first load the data
            # into a temporary table and then insert that data into the CStore table.
            query, args = self.dump_table_creation(
                None, "cstore_tmp_ingestion", schema_spec, temporary=True
            )
            stream.write(cur.mogrify(query, args).decode("utf-8"))
            stream.write(";\n")
            self.dump_table_sql(
                schema,
                object_id,
                stream,
                target_schema="pg_temp",
                target_table="cstore_tmp_ingestion",
            )
            stream.write(
                SQL("INSERT INTO {}.{} (SELECT * FROM pg_temp.cstore_tmp_ingestion);\n")
                .format(Identifier(schema), Identifier(object_id))
                .as_string(cur)
            )
            stream.write(
                cur.mogrify(
                    "SELECT splitgraph_api.set_object_schema(%s, %s);\n",
                    (object_id, json.dumps(schema_spec)),
                ).decode("utf-8")
            )
            stream.write("DROP TABLE pg_temp.cstore_tmp_ingestion;\n")

    def get_object_size(self, object_id: str) -> int:
        return int(self.run_api_call("get_object_size", object_id))

    def delete_objects(self, object_ids: List[str]) -> None:
        self.unmount_objects(object_ids)
        self.run_api_call_batch("delete_object_files", [(o,) for o in object_ids])

    def rename_object(self, old_object_id: str, new_object_id: str):
        self.unmount_objects([old_object_id])
        self.run_api_call("rename_object_files", old_object_id, new_object_id)
        self.mount_object(new_object_id)

    def unmount_objects(self, object_ids: List[str]) -> None:
        """Unmount objects from splitgraph_meta (this doesn't delete the physical files."""
        unmount_query = SQL(";").join(
            SQL("DROP FOREIGN TABLE IF EXISTS {}.{}").format(
                Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
            )
            for object_id in object_ids
        )
        self.run_sql(unmount_query)

    def sync_object_mounts(self) -> None:
        """Scan through local object storage and synchronize it with the foreign tables in
        splitgraph_meta (unmounting non-existing objects and mounting existing ones)."""
        object_ids = self.run_api_call("list_objects")

        mounted_objects = self.run_sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'FOREIGN'",
            (SPLITGRAPH_META_SCHEMA,),
            return_shape=ResultShape.MANY_ONE,
        )

        if mounted_objects:
            self.unmount_objects(mounted_objects)

        for object_id in object_ids:
            self.mount_object(object_id, schema_spec=self.get_object_schema(object_id))

    def mount_object(
        self,
        object_id: str,
        table: None = None,
        schema: str = SPLITGRAPH_META_SCHEMA,
        schema_spec: Optional["TableSchema"] = None,
    ) -> None:
        """
        Mount an object from local storage as a foreign table.

        :param object_id: ID of the object
        :param table: Table to mount the object into
        :param schema: Schema to mount the object into
        :param schema_spec: Schema of the object.
        """
        query = self.dump_object_creation(object_id, schema, table, schema_spec, if_not_exists=True)
        try:
            self.run_sql(query)
        except psycopg2.errors.UndefinedObject as e:
            raise ObjectMountingError(
                "Error mounting. It's possible that the object was created "
                "using an extension (e.g. PostGIS) that's not installed on this engine."
            ) from e

    def store_fragment(
        self,
        inserted: Any,
        deleted: Any,
        schema: str,
        table: str,
        source_schema: str,
        source_table: str,
        source_schema_spec: Optional[TableSchema] = None,
    ) -> None:
        temporary = schema == "pg_temp"

        schema_spec = source_schema_spec or self.get_full_table_schema(source_schema, source_table)

        # Assuming the schema_spec has the whole tuple as PK if the table has no PK.
        change_key = get_change_key(schema_spec)
        ri_cols, ri_types = zip(*change_key)
        ri_cols = list(ri_cols)
        ri_types = list(ri_types)
        non_ri_cols = [c.name for c in schema_spec if c.name not in ri_cols]
        all_cols = ri_cols + non_ri_cols

        self.create_table(
            schema,
            table,
            schema_spec=add_ud_flag_column(schema_spec),
            temporary=temporary,
        )

        # Store upserts
        # INSERT INTO target_table (sg_ud_flag, col1, col2...)
        #   (SELECT true, t.col1, t.col2, ...
        #    FROM VALUES ((pk1_1, pk1_2), (pk2_1, pk2_2)...)) v JOIN source_table t
        #    ON v.pk1 = t.pk1::pk1_type AND v.pk2::pk2_type = t.pk2...
        #    -- the cast is required since the audit trigger gives us strings for values of updated columns
        #    -- and we're intending to join those with the PKs in the original table.
        if inserted:
            if non_ri_cols:
                query = (
                    SQL("INSERT INTO {}.{} (").format(Identifier(schema), Identifier(table))
                    + SQL(",").join(Identifier(c) for c in [SG_UD_FLAG] + all_cols)
                    + SQL(")")
                    + SQL("(SELECT %s, ")
                    + SQL(",").join(SQL("t.") + Identifier(c) for c in all_cols)
                    + SQL(
                        " FROM (VALUES "
                        + ",".join(
                            itertools.repeat(
                                "(" + ",".join(itertools.repeat("%s", len(inserted[0]))) + ")",
                                len(inserted),
                            )
                        )
                        + ")"
                    )
                    + SQL(" AS v (")
                    + SQL(",").join(Identifier(c) for c in ri_cols)
                    + SQL(")")
                    + SQL("JOIN {}.{} t").format(
                        Identifier(source_schema), Identifier(source_table)
                    )
                    + SQL(" ON ")
                    + SQL(" AND ").join(
                        SQL("t.{0} = v.{0}::%s" % r).format(Identifier(c))
                        for c, r in zip(ri_cols, ri_types)
                    )
                    + SQL(")")
                )
                # Flatten the args
                args = [True] + [p for pk in inserted for p in pk]
            else:
                # If the whole tuple is the PK, there's no point joining on the actual source table
                query = (
                    SQL("INSERT INTO {}.{} (").format(Identifier(schema), Identifier(table))
                    + SQL(",").join(Identifier(c) for c in [SG_UD_FLAG] + ri_cols)
                    + SQL(")")
                    + SQL(
                        "VALUES "
                        + ",".join(
                            itertools.repeat(
                                "(" + ",".join(itertools.repeat("%s", len(inserted[0]) + 1)) + ")",
                                len(inserted),
                            )
                        )
                    )
                )
                args = [p for pk in inserted for p in [True] + list(pk)]
            self.run_sql(query, args)

        # Store the deletes
        # we don't actually have the old values here so we put NULLs (which should be compressed out).
        if deleted:
            query = (
                SQL("INSERT INTO {}.{} (").format(Identifier(schema), Identifier(table))
                + SQL(",").join(Identifier(c) for c in [SG_UD_FLAG] + ri_cols)
                + SQL(")")
                + SQL(
                    "VALUES "
                    + ",".join(
                        itertools.repeat(
                            "(" + ",".join(itertools.repeat("%s", len(deleted[0]) + 1)) + ")",
                            len(deleted),
                        )
                    )
                )
            )
            args = [p for pk in deleted for p in [False] + list(pk)]
            self.run_sql(query, args)

    def store_object(
        self,
        object_id: str,
        source_query: Union[bytes, Composed, str, SQL],
        schema_spec: TableSchema,
        source_query_args=None,
        overwrite=False,
    ) -> None:

        # The physical object storage (/var/lib/splitgraph/objects) and the actual
        # foreign tables in spligraph_meta might be desynced for a variety of reasons,
        # so we have to handle all four cases of (physical object exists/doesn't exist,
        # foreign table exists/doesn't exist).
        object_exists = bool(self.run_api_call("object_exists", object_id))

        # Try mounting the object
        if self.table_exists(SPLITGRAPH_META_SCHEMA, object_id):
            # Foreign table with that name already exists. If it does exist but there's no
            # physical object file, we'll have to write into the table anyway.
            if not object_exists:
                logging.info(
                    "Object storage, %s, mounted but no physical file, recreating",
                    object_id,
                )

            # In addition, there's a corner case where the object was mounted with different FDW
            # parameters (e.g. object path in /var/lib/splitgraph/objects has changed)
            # and we want that to be overwritten in any case, so we remount the object.
            self.unmount_objects([object_id])
            self.mount_object(object_id, schema_spec=schema_spec)
        else:
            self.mount_object(object_id, schema_spec=schema_spec)

        if object_exists:
            if overwrite:
                logging.info("Object %s already exists, will overwrite", object_id)
                self.run_sql(
                    SQL("TRUNCATE TABLE {}.{}").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
                    )
                )
            else:
                logging.info("Object %s already exists, skipping", object_id)
                return

        # At this point, the foreign table mounting the object exists and we've established
        # that it's a brand new table, so insert data into it.
        self.run_sql(
            SQL("INSERT INTO {}.{}").format(
                Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
            )
            + source_query,
            source_query_args,
        )

        # Also store the table schema in a file
        self._set_object_schema(object_id, schema_spec)

    @staticmethod
    def schema_spec_to_cols(schema_spec: "TableSchema") -> Tuple[List[str], List[str]]:
        pk_cols = [p.name for p in schema_spec if p.is_pk]
        non_pk_cols = [p.name for p in schema_spec if not p.is_pk]

        if not pk_cols:
            pk_cols = [p.name for p in schema_spec if p.pg_type in PG_INDEXABLE_TYPES]
            non_pk_cols = [p.name for p in schema_spec if p.pg_type not in PG_INDEXABLE_TYPES]
        return pk_cols, non_pk_cols

    @staticmethod
    def _generate_fragment_application(
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: str,
        cols: Tuple[List[str], List[str]],
        extra_quals: Optional[Composed] = None,
    ) -> Composed:
        ri_cols, non_ri_cols = cols
        all_cols = ri_cols + non_ri_cols

        # First, delete all PKs from staging that are mentioned in the new fragment. This conveniently
        # covers both deletes and updates.

        # Also, alias tables so that we don't have to send around long strings of object IDs.
        query = (
            SQL("DELETE FROM {0}.{2} t USING {1}.{3} s").format(
                Identifier(target_schema),
                Identifier(source_schema),
                Identifier(target_table),
                Identifier(source_table),
            )
            + SQL(" WHERE ")
            + _generate_where_clause("t", ri_cols, "s")
        )

        # At this point, we can insert all rows directly since we won't have any conflicts.
        # We can also apply extra qualifiers to only insert rows that match a certain query,
        # which will result in fewer rows actually being written to the staging table.

        # INSERT INTO target_table (col1, col2...)
        #   (SELECT col1, col2, ...
        #    FROM fragment_table WHERE sg_ud_flag = true (AND optional quals))
        query += (
            SQL(";INSERT INTO {}.{} (").format(Identifier(target_schema), Identifier(target_table))
            + SQL(",").join(Identifier(c) for c in all_cols)
            + SQL(")")
            + SQL("(SELECT ")
            + SQL(",").join(Identifier(c) for c in all_cols)
            + SQL(" FROM {}.{}").format(Identifier(source_schema), Identifier(source_table))
        )
        extra_quals = [extra_quals] if extra_quals else []
        extra_quals.append(SQL("{} = true").format(Identifier(SG_UD_FLAG)))
        if extra_quals:
            query += SQL(" WHERE ") + SQL(" AND ").join(extra_quals)
        query += SQL(")")
        return query

    def apply_fragments(
        self,
        objects: List[Tuple[str, str]],
        target_schema: str,
        target_table: str,
        extra_quals: Optional[Composed] = None,
        extra_qual_args: Optional[Tuple[Any, ...]] = None,
        schema_spec: Optional["TableSchema"] = None,
        progress_every: Optional[int] = None,
    ) -> None:
        if not objects:
            return
        schema_spec = schema_spec or self.get_full_table_schema(target_schema, target_table)
        # Assume that the target table already has the required schema (including PKs)
        # and use that to generate queries to apply fragments.
        cols = self.schema_spec_to_cols(schema_spec)

        if progress_every:
            batches = list(chunk(objects, chunk_size=progress_every))
            with tqdm(total=len(objects), unit="obj") as pbar:
                for batch in batches:
                    self._apply_batch(
                        batch, target_schema, target_table, extra_quals, cols, extra_qual_args
                    )
                    pbar.update(len(batch))
                    pbar.set_postfix({"object": batch[-1][1][:10] + "..."})
        else:
            self._apply_batch(
                objects, target_schema, target_table, extra_quals, cols, extra_qual_args
            )

    def _apply_batch(
        self, objects, target_schema, target_table, extra_quals, cols, extra_qual_args
    ):
        query = SQL(";").join(
            self._generate_fragment_application(
                ss, st, target_schema, target_table, cols, extra_quals
            )
            for ss, st in objects
        )
        self.run_sql(query, (extra_qual_args * len(objects)) if extra_qual_args else None)

    def upload_objects(self, objects: List[str], remote_engine: "PostgresEngine") -> None:

        # We don't have direct access to the remote engine's storage and we also
        # can't use the old method of first creating a CStore table remotely and then
        # mounting it via FDW (because this is done on two separate connections, after
        # the table is created and committed on the first one, it can't be written into.
        #
        # So we have to do a slower method of dumping the table into binary
        # and then piping that binary into the remote engine.
        #
        # Perhaps we should drop direct uploading altogether and require people to use S3 throughout.

        pbar = tqdm(objects, unit="objs", ascii=SG_CMD_ASCII)
        for object_id in pbar:
            pbar.set_postfix(object=object_id[:10] + "...")
            schema_spec = self.get_object_schema(object_id)
            remote_engine.mount_object(object_id, schema_spec=schema_spec)

            # Truncate the remote object in case it already exists (we'll overwrite it).
            remote_engine.run_sql(
                SQL("TRUNCATE TABLE {}.{}").format(
                    Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
                )
            )

            stream = BytesIO()
            with self.copy_cursor() as cur:
                cur.copy_expert(
                    SQL("COPY {}.{} TO STDOUT WITH (FORMAT 'binary')").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
                    ),
                    stream,
                )
            stream.seek(0)
            with remote_engine.copy_cursor() as cur:
                cur.copy_expert(
                    SQL("COPY {}.{} FROM STDIN WITH (FORMAT 'binary')").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
                    ),
                    stream,
                )
            remote_engine._set_object_schema(object_id, schema_spec)
            remote_engine.commit()

    @contextmanager
    def _mount_remote_engine(self, remote_engine: "PostgresEngine") -> Iterator[str]:
        # Switch the global engine to "self" instead of LOCAL since `mount_postgres` uses the global engine
        # (we can't easily pass args into it as it's also invoked from the command line with its arguments
        # used to populate --help)
        user = remote_engine.conn_params["SG_ENGINE_USER"]
        pwd = remote_engine.conn_params["SG_ENGINE_PWD"]
        host = remote_engine.conn_params["SG_ENGINE_FDW_HOST"]
        port = remote_engine.conn_params["SG_ENGINE_FDW_PORT"]
        dbname = remote_engine.conn_params["SG_ENGINE_DB_NAME"]

        # conn_params might contain Nones for some parameters (host/port for Unix
        # socket connection, so here we have to validate that they aren't None).
        assert user is not None
        assert pwd is not None
        assert host is not None
        assert port is not None
        assert dbname is not None

        logging.info(
            "Mounting remote schema %s@%s:%s/%s/%s to %s...",
            user,
            host,
            port,
            dbname,
            SPLITGRAPH_META_SCHEMA,
            REMOTE_TMP_SCHEMA,
        )
        self.delete_schema(REMOTE_TMP_SCHEMA)
        with switch_engine(self):
            mount_postgres(
                mountpoint=REMOTE_TMP_SCHEMA,
                host=host,
                port=int(port),
                username=user,
                password=pwd,
                dbname=dbname,
                remote_schema=SPLITGRAPH_META_SCHEMA,
            )
        try:
            yield REMOTE_TMP_SCHEMA
        finally:
            self.delete_schema(REMOTE_TMP_SCHEMA)

    def download_objects(self, objects: List[str], remote_engine: "PostgresEngine") -> List[str]:
        # Instead of connecting and pushing queries to it from the Python client, we just mount the remote mountpoint
        # into a temporary space (without any checking out) and SELECT the  required data into our local tables.

        with self._mount_remote_engine(remote_engine) as remote_schema:
            downloaded_objects = []
            pbar = tqdm(objects, unit="objs", ascii=SG_CMD_ASCII)
            for object_id in pbar:
                pbar.set_postfix(object=object_id[:10] + "...")
                if not self.table_exists(remote_schema, object_id):
                    logging.error("%s not found on the remote engine!", object_id)
                    continue

                # Create the CStore table on the engine and copy the contents of the object into it.
                schema_spec = remote_engine.get_full_table_schema(SPLITGRAPH_META_SCHEMA, object_id)
                self.mount_object(object_id, schema_spec=schema_spec)
                self.copy_table(
                    remote_schema,
                    object_id,
                    SPLITGRAPH_META_SCHEMA,
                    object_id,
                    with_pk_constraints=False,
                )
                self._set_object_schema(object_id, schema_spec=schema_spec)
                downloaded_objects.append(object_id)
        if len(downloaded_objects) < len(objects):
            raise IncompleteObjectDownloadError(reason=None, successful_objects=downloaded_objects)
        return downloaded_objects

    def get_change_key(self, schema: str, table: str) -> List[Tuple[str, str]]:
        return get_change_key(self.get_full_table_schema(schema, table))


def get_change_key(schema_spec: TableSchema) -> List[Tuple[str, str]]:
    pk = [(c.name, c.pg_type) for c in schema_spec if c.is_pk]
    if pk:
        return pk

    # Only return columns that can be compared (since we'll be using them
    # for chunking)
    return [(c.name, c.pg_type) for c in schema_spec if c.pg_type in PG_INDEXABLE_TYPES]


def _split_ri_cols(
    action: str,
    row_data: Dict[str, Any],
    changed_fields: Optional[Dict[str, str]],
    ri_cols: Tuple[str, ...],
) -> Any:
    """
    :return: `(ri_data, non_ri_data)`: a tuple of 2 dictionaries:
        * `ri_data`: maps column names in `ri_cols` to values identifying the replica identity (RI) of a given tuple
        * `non_ri_data`: map of column names and values not in the RI that have been changed/updated
    """
    non_ri_data = {}
    ri_data = {}

    if action == "I":
        for column, value in row_data.items():
            if column in ri_cols:
                ri_data[column] = value
            else:
                non_ri_data[column] = value
    elif action == "D":
        for column, value in row_data.items():
            if column in ri_cols:
                ri_data[column] = value
    else:
        assert action == "U"
        for column, value in row_data.items():
            if column in ri_cols:
                ri_data[column] = value
        if changed_fields:
            for column, value in changed_fields.items():
                non_ri_data[column] = value

    return ri_data, non_ri_data


def _recalculate_disjoint_ri_cols(
    ri_cols: Tuple[str, ...],
    ri_data: Dict[str, Any],
    non_ri_data: Dict[str, Any],
    row_data: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    # If part of the PK has been updated (is in the non_ri_cols/vals), we have to instead
    # apply the update to the PK (ri_cols/vals) and recalculate the new full new tuple
    # (by applying the update to row_data).
    new_non_ri_data = {}
    row_data = row_data.copy()

    for nrc, nrv in non_ri_data.items():
        if nrc in ri_cols:
            ri_data[nrc] = nrv
        else:
            row_data[nrc] = nrv

    for col, val in row_data.items():
        if col not in ri_cols:
            new_non_ri_data[col] = val

    return ri_data, new_non_ri_data


def _convert_audit_change(
    action: str,
    row_data: Dict[str, Any],
    changed_fields: Optional[Dict[str, str]],
    ri_cols: Tuple[str, ...],
) -> List[Tuple[Tuple, bool, Dict[str, str], Dict[str, str]]]:
    """
    Converts the audit log entry into Splitgraph's internal format.

    :returns: [(pk, (True for upserted, False for deleted), (old row value if updated/deleted),
        (new row value if inserted/updated))].
        More than 1 change might be emitted from a single audit entry.
    """
    ri_data, non_ri_data = _split_ri_cols(action, row_data, changed_fields, ri_cols)
    pk_changed = any(c in ri_cols for c in non_ri_data)

    if changed_fields:
        new_row = row_data.copy()
        for key, value in changed_fields.items():
            if key not in ri_cols:
                new_row[key] = value
    else:
        new_row = row_data
    if pk_changed:
        assert action == "U"
        # If it's an update that changed the PK (e.g. the table has no replica identity so we treat the whole
        # tuple as a primary key), then we turn it into (old PK, DELETE, old row data); (new PK, INSERT, {})

        # Recalculate the new PK to be inserted + the new (full) tuple, otherwise if the whole
        # tuple hasn't been updated, we'll lose parts of the old row (see test_diff_conflation_on_commit[test_case2]).

        result = [(tuple(ri_data[c] for c in ri_cols), False, row_data, new_row)]

        ri_data, non_ri_data = _recalculate_disjoint_ri_cols(
            ri_cols, ri_data, non_ri_data, row_data
        )
        result.append((tuple(ri_data[c] for c in ri_cols), True, {}, new_row))
        return result
    if action == "U" and not non_ri_data:
        # Nothing was actually updated -- don't emit an action
        return []
    return [
        (
            tuple(ri_data[c] for c in ri_cols),
            action in ("I", "U"),
            row_data if action in ("U", "D") else {},
            new_row if action in ("I", "U") else {},
        )
    ]


_KIND = {"I": 0, "D": 1, "U": 2}


def _generate_where_clause(table: str, cols: List[str], table_2: str) -> Composed:
    return SQL(" AND ").join(
        SQL("{}.{} = {}.{}").format(
            Identifier(table), Identifier(c), Identifier(table_2), Identifier(c)
        )
        for c in cols
    )


def add_ud_flag_column(table_schema: TableSchema) -> TableSchema:
    return table_schema + [TableColumn(table_schema[-1].ordinal + 1, SG_UD_FLAG, "boolean", False)]
