"""Table metadata-related classes."""
import itertools
import logging
import threading
from contextlib import contextmanager
from math import ceil
from typing import (
    Any,
    Callable,
    Iterator,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Dict,
    Sequence,
    cast,
)

from psycopg2.sql import SQL, Identifier, Composable
from tqdm import tqdm

from splitgraph.config import SPLITGRAPH_META_SCHEMA, SPLITGRAPH_API_SCHEMA, SG_CMD_ASCII
from splitgraph.core.common import Tracer, get_temporary_table_id
from splitgraph.core.fragment_manager import (
    get_chunk_groups,
    ExtraIndexInfo,
)
from splitgraph.core.indexing.range import quals_to_sql
from splitgraph.core.output import pluralise, truncate_list
from splitgraph.core.sql import select
from splitgraph.core.types import TableSchema, Quals
from splitgraph.engine import ResultShape
from splitgraph.engine.postgres.engine import get_change_key
from splitgraph.exceptions import ObjectIndexingError

if TYPE_CHECKING:
    from splitgraph.core.image import Image
    from splitgraph.core.repository import Repository
    from splitgraph.engine.postgres.engine import PostgresEngine

# Output checkout progress every 5MB to trade off between the latency
# of initializing a batch of object applications and not reporting anything at all
_PROGRESS_EVERY = 5 * 1024 * 1024


def _generate_select_query(
    engine: "PostgresEngine",
    table: bytes,
    columns: Sequence[str],
    qual_sql: Optional[Composable] = None,
    qual_args: Optional[Tuple] = None,
) -> bytes:
    cur = engine.connection.cursor()

    query = (
        SQL("SELECT ")
        + SQL(",").join(Identifier(c) for c in columns)
        + SQL(" FROM " + table.decode("utf-8"))
        + (SQL(" WHERE ") + qual_sql if qual_args else SQL(""))
    )
    query = cur.mogrify(query, qual_args)

    cur.close()
    return cast(bytes, query)


def _generate_table_names(engine: "PostgresEngine", schema: str, tables: List[str]) -> List[bytes]:
    result = []
    cur = engine.connection.cursor()
    for table in tables:
        result.append(cur.mogrify(SQL("{}.{}").format(Identifier(schema), Identifier(table))))
    cur.close()
    logging.debug("Returning tables %r", tables)
    return result


def _delete_temporary_table(engine: "PostgresEngine", schema: str, table: str) -> None:
    """
    Workaround for Multicorn deadlocks at end_scan time, supposed to be run in a separate thread.
    """
    # Delete the temporary table in an autocommitting connection with a small lock
    # timeout: if we fail to delete it because Multicorn is still holding a lock on it etc,
    # we'd rather leave the temporary table in splitgraph_meta than lock up.
    conn = engine.connection
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout TO '5s';")
            cur.execute(
                SQL("DROP TABLE IF EXISTS {}.{}").format(Identifier(schema), Identifier(table))
            )
        logging.info("Dropped temporary table %s", table)
    finally:
        conn.autocommit = False
        conn.close()


def _empty_callback(**kwargs) -> None:
    pass


class QueryPlan:
    """
    Represents the initial query plan (fragments to query) for given columns and
    qualifiers.
    """

    def __init__(self, table: "Table", quals: Optional[Quals], columns: Sequence[str]) -> None:
        self.table = table
        self.quals = quals
        self.columns = columns
        self.tracer = Tracer()

        self.object_manager = table.repository.objects

        self.required_objects = table.objects
        self.tracer.log("resolve_objects")
        self.filtered_objects = self.object_manager.filter_fragments(
            self.required_objects, table, quals
        )
        # Estimate the number of rows in the filtered objects
        self.estimated_rows = sum(
            [
                o.rows_inserted - o.rows_deleted
                for o in self.object_manager.get_object_meta(self.filtered_objects).values()
            ]
        )
        self.tracer.log("filter_objects")

        # Prepare a list of objects to query

        # Special fast case: single-chunk groups can all be batched together
        # and queried directly without having to copy them to a staging table.
        # We also grab all fragments from multiple-fragment groups and batch them together
        # for future application.
        #
        # Technically, we could do multiple batches of application for these groups
        # (apply first batch to the staging table, extract the result, clean the table,
        # apply next batch etc): in the middle of it we could also talk back to the object
        # manager and release the objects that we don't need so that they can be garbage
        # collected. The tradeoff is that we perform more calls to apply_fragments (hence
        # more roundtrips).
        self.non_singletons, self.singletons = self._extract_singleton_fragments()

        logging.info(
            "Fragment grouping: %d singletons, %d non-singletons",
            len(self.singletons),
            len(self.non_singletons),
        )
        self.tracer.log("group_fragments")

        self.sql_quals, self.sql_qual_vals = quals_to_sql(
            quals, column_types={c.name: c.pg_type for c in self.table.table_schema}
        )

        if self.singletons:
            self.singleton_queries = _generate_table_names(
                self.object_manager.object_engine, SPLITGRAPH_META_SCHEMA, self.singletons
            )
        else:
            self.singleton_queries = []
        self.tracer.log("generate_singleton_queries")

    def _extract_singleton_fragments(self) -> Tuple[List[str], List[str]]:
        # Get fragment boundaries (min-max PKs of every fragment).
        table_pk = get_change_key(self.table.table_schema)
        object_pks = self.object_manager.get_min_max_pks(self.filtered_objects, table_pk)
        # Group fragments into non-overlapping groups: those can be applied independently of each other.
        object_groups = get_chunk_groups(
            [
                (object_id, min_max[0], min_max[1])
                for object_id, min_max in zip(self.filtered_objects, object_pks)
            ]
        )
        singletons: List[str] = []
        non_singletons: List[str] = []
        for group in object_groups:
            if len(group) == 1:
                singletons.append(group[0][0])
            else:
                non_singletons.extend(object_id for object_id, _, _ in group)
        return non_singletons, singletons


QueryPlanCacheKey = Tuple[Optional[Tuple[Tuple[Tuple[str, str, Any]]]], Tuple[str]]


def _get_plan_cache_key(quals: Optional[Quals], columns: Sequence[str]) -> QueryPlanCacheKey:
    quals = (
        cast(
            Tuple[Tuple[Tuple[str, str, Any]]],
            tuple(tuple(tuple(q) for q in qual) for qual in quals),
        )
        if quals
        else None
    )
    columns = cast(Tuple[str], tuple(columns))
    return quals, columns


def merge_index_data(current_index: Dict[str, Any], new_index: Dict[str, Any]):
    for index_type, index_data in new_index.items():
        for col_name, col_index_data in index_data.items():
            if index_type not in current_index:
                current_index[index_type] = {}
            if col_name not in current_index[index_type]:
                current_index[index_type][col_name] = {}

            current_index[index_type][col_name] = col_index_data


class Table:
    """Represents a Splitgraph table in a given image. Shouldn't be created directly, use Table-loading
    methods in the :class:`splitgraph.core.image.Image` class instead."""

    def __init__(
        self,
        repository: "Repository",
        image: "Image",
        table_name: str,
        table_schema: TableSchema,
        objects: List[str],
    ) -> None:
        self.repository = repository
        self.image = image
        self.table_name = table_name
        self.table_schema = table_schema

        # List of fragments this table is composed of
        self.objects = objects

        self._query_plans: Dict[Tuple[Optional[Quals], Tuple[str]], QueryPlan] = {}

    def __repr__(self) -> str:
        return "Table %s in %s" % (self.table_name, str(self.image))

    def get_query_plan(
        self, quals: Optional[Quals], columns: Sequence[str], use_cache: bool = True
    ) -> QueryPlan:
        """
        Start planning a query (preliminary steps before object downloading,
        like qualifier filtering).

        :param quals: Qualifiers in CNF form
        :param columns: List of columns
        :param use_cache: If True, will fetch the plan from the cache for the same qualifiers and columns.
        :return: QueryPlan
        """
        key = _get_plan_cache_key(quals, columns)

        if use_cache and key in self._query_plans:
            plan = self._query_plans[key]
            # Reset the tracer in the plan: if this instance
            # persisted, the resolve/filter times in it will be
            # way in the past.
            plan.tracer = Tracer()
            plan.tracer.log("resolve_objects")
            plan.tracer.log("filter_objects")
            plan.tracer.log("group_fragments")
            plan.tracer.log("generate_singleton_queries")
            return plan

        plan = QueryPlan(self, quals, columns)
        self._query_plans[key] = plan
        return plan

    def materialize(
        self,
        destination: str,
        destination_schema: Optional[str] = None,
        lq_server: Optional[str] = None,
        temporary: bool = False,
    ) -> None:
        """
        Materializes a Splitgraph table in the target schema as a normal Postgres table, potentially downloading all
        required objects and using them to reconstruct the table.

        :param destination: Name of the destination table.
        :param destination_schema: Name of the destination schema.
        :param lq_server: If set, sets up a layered querying FDW for the table instead using this foreign server.
        """
        # Circular import
        from splitgraph.hooks.data_source.fdw import create_foreign_table

        destination_schema = destination_schema or self.repository.to_schema()
        engine = self.repository.object_engine
        object_manager = self.repository.objects
        engine.delete_table(destination_schema, destination)

        if not lq_server:
            # Materialize by applying fragments to one another in their dependency order.
            with object_manager.ensure_objects(
                table=self, objects=self.objects
            ) as required_objects:
                engine.create_table(
                    schema=destination_schema,
                    table=destination,
                    schema_spec=self.table_schema,
                    include_comments=True,
                    unlogged=True,
                    temporary=temporary,
                )
                if required_objects:
                    logging.debug("Applying %s...", pluralise("fragment", len(required_objects)))

                    table_size = self.get_size()

                    progress_every: Optional[int]
                    if table_size > _PROGRESS_EVERY:
                        progress_every = int(
                            ceil(len(required_objects) * _PROGRESS_EVERY / float(table_size))
                        )
                    else:
                        progress_every = None

                    engine.apply_fragments(
                        [(SPLITGRAPH_META_SCHEMA, d) for d in cast(List[str], required_objects)],
                        destination_schema,
                        destination,
                        progress_every=progress_every,
                    )
        else:
            query, args = create_foreign_table(
                destination_schema,
                lq_server,
                self.table_name,
                self.table_schema,
                extra_options={"table": self.table_name},
            )

            engine.run_sql(query, args)

    def query_indirect(
        self, columns: List[str], quals: Optional[Quals]
    ) -> Tuple[Iterator[bytes], Callable, QueryPlan]:
        """
        Run a read-only query against this table without materializing it. Instead of
        actual results, this returns a generator of SQL queries that the caller can use
        to get the results as well as a callback that the caller has to run after they're
        done consuming the results.

        In particular, the query generator will prefer returning direct queries to
        Splitgraph objects and only when those are exhausted will it start materializing
        delta-compressed fragments.

        This is an advanced method: you probably want to call table.query().

        :param columns: List of columns from this table to fetch
        :param quals: List of qualifiers in conjunctive normal form. See the documentation for
            FragmentManager.filter_fragments for the actual format.
        :return: Generator of queries (bytes), a callback and a query plan object (containing stats
            that are fully populated after the callback has been called to end the query).
        """
        plan = self.get_query_plan(quals, columns)
        required_objects = plan.filtered_objects
        logging.info(
            "Using %d fragments (%s) to satisfy the query",
            len(required_objects),
            truncate_list(required_objects),
        )
        if not required_objects:
            return cast(Iterator[bytes], []), cast(Callable, _empty_callback), plan

        object_manager = self.repository.objects
        if not plan.non_singletons:
            with object_manager.ensure_objects(
                self, objects=required_objects, defer_release=True, tracer=plan.tracer
            ) as eo_result:
                _, release_callback = cast(Tuple, eo_result)
                return iter(plan.singleton_queries), cast(Callable, release_callback), plan

        def _generate_nonsingleton_query():
            # If we have fragments that need applying to a staging area, we don't want to
            # do it immediately: the caller might be satisfied with the data they got from
            # the queries to singleton fragments. So here we have a callback that, when called,
            # actually materializes the chunks into a temporary table and then changes
            # the table's release callback to also delete that temporary table.

            # There's a slight issue: we can't use temporary tables if we're returning
            # pointers to tables since the caller might be in a different session.
            staging_table = self._create_staging_table()
            engine = self.repository.object_engine

            def _f(from_fdw=False):
                # This is very horrible but the way we share responsibilities between ourselves
                # and Multicorn leaves us no other choice. In LQ, this is supposed to be called
                # during EndForeignScan (at which point we are done with this staging table).
                # However, EndForeignScan doesn't actually release locks on tables that Multicorn
                # was reading (that are acquired outside of our control if the foreign scan happened in a
                # transaction). In that case we can't delete this table until the transaction actually finishes
                # -- which can't happen until we've deleted this table.

                # Other options are: generating the materialization query and running it on
                # Multicorn side (issues with Portals not being able to perform DDL and us having
                # to rework our interface), adding a DROP table to the end of the query (then
                # it doesn't return results, Portals still can't do DDL and the query is no longer
                # idempotent).

                # Instead, we pretend that we've successfully cleaned up but actually spawn
                # a thread whose single job will be running DROP table and waiting until it actually
                # returns.

                if from_fdw:
                    thread = threading.Thread(
                        target=_delete_temporary_table,
                        args=(engine, SPLITGRAPH_META_SCHEMA, staging_table),
                    )
                    thread.start()
                else:
                    engine.delete_table(SPLITGRAPH_META_SCHEMA, staging_table)

            nonlocal release_callback
            release_callback.append(_f)

            # Apply the fragments (just the parts that match the qualifiers) to the staging area
            if quals:
                engine.apply_fragments(
                    [(SPLITGRAPH_META_SCHEMA, o) for o in plan.non_singletons],
                    SPLITGRAPH_META_SCHEMA,
                    staging_table,
                    extra_quals=plan.sql_quals,
                    extra_qual_args=plan.sql_qual_vals,
                    schema_spec=self.table_schema,
                )
            else:
                engine.apply_fragments(
                    [(SPLITGRAPH_META_SCHEMA, o) for o in plan.non_singletons],
                    SPLITGRAPH_META_SCHEMA,
                    staging_table,
                    schema_spec=self.table_schema,
                )
            engine.commit()
            table_name = _generate_table_names(engine, SPLITGRAPH_META_SCHEMA, [staging_table])[0]
            yield table_name

        with object_manager.ensure_objects(
            self, objects=required_objects, defer_release=True, tracer=plan.tracer
        ) as eo_result:
            _, release_callback = cast(Tuple, eo_result)
            return (
                itertools.chain(plan.singleton_queries, _generate_nonsingleton_query()),
                cast(Callable, release_callback),
                plan,
            )

    @contextmanager
    def query_lazy(self, columns: List[str], quals: Quals) -> Iterator[Iterator[Dict[str, Any]]]:
        """
        Run a read-only query against this table without materializing it.

        :param columns: List of columns from this table to fetch
        :param quals: List of qualifiers in conjunctive normal form. See the documentation for
            FragmentManager.filter_fragments for the actual format.
        :return: Generator of dictionaries of results.
        """

        table_gen, release_callback, plan = self.query_indirect(columns, quals)
        engine = self.repository.object_engine

        def _generate_results():
            for table in table_gen:
                result = engine.run_sql(
                    _generate_select_query(
                        engine, table, plan.columns, plan.sql_quals, plan.sql_qual_vals
                    )
                )
                for row in result:
                    yield {c: v for c, v in zip(columns, row)}

        try:
            yield _generate_results()
        finally:
            release_callback()

    def query(self, columns: List[str], quals: Quals):
        """
        Run a read-only query against this table without materializing it.

        This is a wrapper around query_lazy() that force evaluates the results which
        might mean more fragments being materialized that aren't needed.

        :param columns: List of columns from this table to fetch
        :param quals: List of qualifiers in conjunctive normal form. See the documentation for
            FragmentManager.filter_fragments for the actual format.
        :return: List of dictionaries of results
        """
        with self.query_lazy(columns, quals) as result:
            return list(result)

    def get_size(self) -> int:
        """
        Get the physical size used by the table's objects (including those shared with other tables).

        This is calculated from the metadata, the on-disk footprint might be smaller if not all of table's
        objects have been downloaded.

        :return: Size of the table in bytes.
        """
        return cast(
            int,
            self.repository.engine.run_sql(
                select("get_table_size", table_args="(%s,%s,%s,%s)", schema=SPLITGRAPH_API_SCHEMA),
                (
                    self.repository.namespace,
                    self.repository.repository,
                    self.image.image_hash,
                    self.table_name,
                ),
                return_shape=ResultShape.ONE_ONE,
            )
            or 0,
        )

    def get_length(self) -> int:
        """
        Get the number of rows in this table.

        This might be smaller than the total number of rows in all objects belonging to this
        table as some objects might overwrite each other.

        :return: Number of rows in table
        """
        return cast(
            int,
            self.repository.engine.run_sql(
                select(
                    "get_table_length", table_args="(%s,%s,%s,%s)", schema=SPLITGRAPH_API_SCHEMA
                ),
                (
                    self.repository.namespace,
                    self.repository.repository,
                    self.image.image_hash,
                    self.table_name,
                ),
                return_shape=ResultShape.ONE_ONE,
            )
            or 0,
        )

    def reindex(self, extra_indexes: ExtraIndexInfo, raise_on_patch_objects=True) -> List[str]:
        """
        Run extra indexes on all objects in this table and update their metadata.
        This only works on objects that don't have any deletions or upserts (have a deletion hash of 000000...).

        :param extra_indexes: Dictionary of {index_type: column: index_specific_kwargs}.
        :param raise_on_patch_objects: If True, will raise an exception if any objects in the table
            overwrite any other objects. If False, will log a warning but will reindex all non-patch objects.

        :returns List of objects that were reindexed.
        """
        object_manager = self.repository.objects

        object_meta = object_manager.get_object_meta(self.objects)

        # Because the index includes both inserted and deleted entries (so that we know
        # to grab the object if it deletes something we're interested in), we can't easily
        # reindex objects that include deletions since we'd have to find the object with
        # the old value for whatever we're deleting and also download it.

        valid_objects = {o: om for o, om in object_meta.items() if om.deletion_hash == "0" * 64}
        if len(valid_objects) < len(object_meta):
            message = "%d object(s) in %s have deletions or upserts, reindexing unsupported" % (
                len(object_meta) - len(valid_objects),
                self,
            )
            if raise_on_patch_objects:
                raise ObjectIndexingError(message)
            else:
                logging.warning(message)

        # Download the objects to reindex them
        with object_manager.ensure_objects(self, objects=list(valid_objects)):
            for object_id in tqdm(valid_objects, unit="objs", ascii=SG_CMD_ASCII):
                current_index = valid_objects[object_id].object_index

                index_struct = object_manager.generate_object_index(
                    object_id, self.table_schema, changeset=None, extra_indexes=extra_indexes
                )

                # Update the index in-place and overwrite it
                merge_index_data(current_index, index_struct)

        object_manager.register_objects(list(valid_objects.values()))
        return list(valid_objects)

    def _create_staging_table(self) -> str:
        staging_table = get_temporary_table_id()

        logging.debug("Using staging table %s", staging_table)
        self.repository.object_engine.create_table(
            schema=SPLITGRAPH_META_SCHEMA,
            table=staging_table,
            schema_spec=self.table_schema,
            unlogged=True,
        )
        return staging_table
