"""Module imported by Multicorn on the Splitgraph engine server: a foreign data wrapper that implements
layered querying (read-only queries to Splitgraph tables without materialization)."""
import contextlib
import logging
from typing import Callable, Dict, NamedTuple, Optional, Tuple, cast

import splitgraph.config
from psycopg2.sql import SQL, Identifier
from splitgraph.config import SPLITGRAPH_META_SCHEMA, get_singleton
from splitgraph.core.object_manager import ObjectManager
from splitgraph.core.output import pretty_size
from splitgraph.core.repository import Repository, get_engine
from splitgraph.core.table import Table

with contextlib.suppress(ImportError):
    # Multicorn not installed (OK if we're not on the engine machine).
    from multicorn import ANY, ForeignDataWrapper

_PG_LOGLEVEL = logging.INFO

# This is shared between all FDW instances on this connection
class TableCacheKey(NamedTuple):
    namespace: str
    repository: str
    image_hash: str
    table: str
    engine: Optional[str]
    object_engine: Optional[str]


_TABLE_CACHE: Dict[TableCacheKey, Table] = {}


def get_table(key: TableCacheKey) -> Table:
    splitgraph.config.IN_FDW = True
    global _TABLE_CACHE
    if key not in _TABLE_CACHE:
        engine_obj = get_engine(key.engine, use_fdw_params=True)
        object_engine_obj = (
            get_engine(key.object_engine, use_fdw_params=True) if key.object_engine else engine_obj
        )
        repository = Repository(
            key.namespace,
            key.repository,
            engine=engine_obj,
            object_engine=object_engine_obj,
            object_manager=ObjectManager(
                object_engine=object_engine_obj, metadata_engine=engine_obj
            ),
        )
        table = repository.images[key.image_hash].get_table(key.table)
        _TABLE_CACHE[key] = table
    return _TABLE_CACHE[key]


class ObjectWrapperForeignDataWrapper(ForeignDataWrapper):
    _startup_cost = 0

    def __init__(self, fdw_options, fdw_columns):
        splitgraph.config.IN_FDW = True
        # Initialize the logger that will log to the engine's stderr: log timestamp and PID.
        logging.basicConfig(
            format="%(asctime)s [%(process)d] %(levelname)s %(message)s",
            level=get_singleton(splitgraph.config.CONFIG, "SG_LOGLEVEL"),
        )

        # Dict of connection parameters as well as the table, repository and image hash to query.
        self.fdw_options = fdw_options

        # The foreign datawrapper columns (name -> ColumnDefinition).
        self.fdw_columns = fdw_columns

        self.table: Table = get_table(
            key=TableCacheKey(
                self.fdw_options["namespace"],
                self.fdw_options["repository"],
                self.fdw_options["image_hash"],
                self.fdw_options["table"],
                self.fdw_options.get("engine"),
                self.fdw_options.get("object_engine"),
            )
        )

        self.object_id = self.fdw_options["object_id"]
        self.end_scan_callback: Optional[Callable] = None

    @staticmethod
    def _quals_to_cnf(quals):
        def _qual_to_cnf(qual):
            if qual.is_list_operator:
                if any(v is None for v in qual.value):
                    # We don't keep track of NULLs so if we get a clause of type
                    # col IN (1,2,3,NULL) we have to look at all objects to see if they contain a NULL.
                    return [[]]
                if qual.list_any_or_all == ANY:
                    # Convert col op ANY(ARRAY[a,b,c...]) into (col op a) OR (col op b)...
                    # which is one single AND clause of multiple ORs
                    return [[(qual.field_name, qual.operator[0], v) for v in qual.value]]
                # Convert col op ALL(ARRAY[a,b,c...]) into (cop op a) AND (col op b)...
                # which is multiple AND clauses of one OR each
                return [[(qual.field_name, qual.operator[0], v)] for v in qual.value]

            if qual.value is None:
                return [[]]
            return [[(qual.field_name, qual.operator, qual.value)]]

        return [q for qual in quals for q in _qual_to_cnf(qual) if q != []]

    def get_rel_size(self, quals, columns):
        cnf_quals = self._quals_to_cnf(quals)
        plan = self.table.get_query_plan(cnf_quals, columns)

        if self.object_id not in plan.filtered_objects:
            return 0, 0

        my_meta = plan.object_meta[self.object_id]
        return my_meta.rows_inserted - my_meta.rows_deleted, int(
            plan.size_per_row / len(self.table.table_schema) * len(columns)
        )

    def explain(self, quals, columns, sortkeys=None, verbose=False):
        cnf_quals = self._quals_to_cnf(quals)
        plan = self.table.get_query_plan(cnf_quals, columns)

        if self.object_id not in plan.filtered_objects:
            return ["Skipped"]
        else:
            return [
                "Scan through CStore object (%s)"
                % pretty_size(plan.object_meta[self.object_id].size)
            ]

    def get_path_keys(self):
        # Return the PK of the table (unique path something)
        pks = [k[1] for k in self.table.table_schema if k[3]]
        if not pks:
            pks = [k[1] for k in self.table.table_schema]
        return [(tuple(pks), 1)]

    def execute(self, quals, columns, sortkeys=None):
        """Main Multicorn entry point."""
        if self.end_scan_callback:
            self.end_scan_callback(from_fdw=True)
            self.end_scan_callback = None

        cnf_quals = self._quals_to_cnf(quals)
        plan = self.table.get_query_plan(cnf_quals, columns)

        if self.object_id not in plan.filtered_objects:
            return

        object_manager = plan.object_manager
        with object_manager.ensure_objects(
            self.table, [self.object_id], defer_release=True
        ) as eo_result:
            _, release_callback = cast(Tuple, eo_result)
            self.end_scan_callback = release_callback
            query = (
                SQL("{}.{}")
                .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(self.object_id))
                .as_string(self.table.repository.object_engine.connection)
            ).encode()
            logging.info(query)
            return [query]

    def end_scan(self):
        if self.end_scan_callback:
            # Call the scan-end callback making sure to use
            # the special hack to avoid deadlocks with Multicorn.
            self.end_scan_callback(from_fdw=True)
            self.end_scan_callback = None
