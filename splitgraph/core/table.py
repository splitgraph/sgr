"""Table metadata-related classes."""
import itertools
import logging
import threading
from contextlib import contextmanager

from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core._common import Tracer
from splitgraph.core.fragment_manager import get_random_object_id, quals_to_sql, get_chunk_groups
from splitgraph.engine import ResultShape


def _delete_temporary_table(engine, schema, table):
    engine.delete_table(schema, table)
    engine.commit()


def _empty_callback(from_fdw=False):
    pass


class QueryPlan:
    """
    Represents the initial query plan (fragments to query) for given columns and
    qualifiers.
    """

    def __init__(self, table, quals, columns):
        self.table = table
        self.quals = quals
        self.columns = columns
        self.tracer = Tracer()

        self.object_manager = table.repository.objects

        logging.info(
            "Resolving objects for table %s:%s:%s",
            table.repository,
            table.image.image_hash,
            table.table_name,
        )

        self.required_objects = table.objects
        self.tracer.log("resolve_objects")
        self.filtered_objects = self.object_manager.filter_objects(
            self.required_objects, table, quals
        )
        self.tracer.log("filter_objects")

    def get_rel_size(self):
        """
        Return the expected number of rows returned by this query.
        :return: Tuple of (amount of rows, average row width in bytes)
        """
        # local_objects = self.object_manager.get_downloaded_objects(limit_to=self.filtered_objects)
        local_objects = []
        # eeh
        rows = sum(
            (
                self.table.repository.object_engine.run_sql(
                    SQL("SELECT COUNT (*) FROM splitgraph_meta.{0}").format(Identifier(o)),
                    return_shape=ResultShape.ONE_ONE,
                )
            )
            if o in local_objects
            else 10000
            for o in self.filtered_objects
        )

        total_size = sum(
            o.size
            for o in self.table.repository.objects.get_object_meta(self.filtered_objects).values()
        )

        if rows:
            return rows, total_size / rows
        else:
            return 0, 0


class Table:
    """Represents a Splitgraph table in a given image. Shouldn't be created directly, use Table-loading
    methods in the :class:`splitgraph.core.image.Image` class instead."""

    def __init__(self, repository, image, table_name, table_schema, objects):
        self.repository = repository
        self.image = image
        self.table_name = table_name
        self.table_schema = [tuple(entry) for entry in table_schema]

        # List of fragments this table is composed of
        self.objects = objects

        self._query_plans = {}

    def get_query_plan(self, quals, columns, use_cache=True):
        quals = tuple(tuple(tuple(t) for t in qual) for qual in quals) if quals else None
        columns = tuple(columns)
        key = (quals, columns)

        if use_cache and key in self._query_plans:
            plan = self._query_plans[key]
            # Reset the tracer in the plan: if this instance
            # persisted, the resolve/filter times in it will be
            # way in the past.
            plan.tracer = Tracer()
            plan.tracer.log("resolve_objects")
            plan.tracer.log("filter_objects")

        plan = QueryPlan(self, quals, columns)
        self._query_plans[key] = plan
        return plan

    def materialize(self, destination, destination_schema=None, lq_server=None):
        """
        Materializes a Splitgraph table in the target schema as a normal Postgres table, potentially downloading all
        required objects and using them to reconstruct the table.

        :param destination: Name of the destination table.
        :param destination_schema: Name of the destination schema.
        :param lq_server: If set, sets up a layered querying FDW for the table instead using this foreign server.
        """
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
                    schema=destination_schema, table=destination, schema_spec=self.table_schema
                )
                if required_objects:
                    logging.info("Applying %d fragment(s)...", (len(required_objects)))
                    engine.apply_fragments(
                        [(SPLITGRAPH_META_SCHEMA, d) for d in required_objects],
                        destination_schema,
                        destination,
                    )
        else:
            query = SQL("CREATE FOREIGN TABLE {}.{} (").format(
                Identifier(destination_schema), Identifier(self.table_name)
            )
            query += SQL(",".join("{} %s " % ctype for _, _, ctype, _ in self.table_schema)).format(
                *(Identifier(cname) for _, cname, _, _ in self.table_schema)
            )
            query += SQL(") SERVER {} OPTIONS (table %s)").format(Identifier(lq_server))
            engine.run_sql(query, (self.table_name,))

    def query_indirect(self, columns, quals):
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
        :return: Generator of queries (bytes) a callback and a query plan object (containing stats
            that are fully populated after the callback has been called to end the query).
        """

        sql_quals, sql_qual_vals = quals_to_sql(
            quals, column_types={c[1]: c[2] for c in self.table_schema}
        )

        plan = self.get_query_plan(quals, columns)
        required_objects = plan.filtered_objects

        object_manager = self.repository.objects
        logging.info("Using fragments %r to satisfy the query", required_objects)
        if not required_objects:
            return [], _empty_callback, plan

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
        non_singletons, singletons = self._extract_singleton_fragments(
            object_manager, required_objects
        )

        logging.info(
            "Fragment grouping: %d singletons, %d non-singletons",
            len(singletons),
            len(non_singletons),
        )
        plan.tracer.log("group_fragments")

        if singletons:
            queries = self._generate_select_queries(
                SPLITGRAPH_META_SCHEMA,
                singletons,
                columns,
                qual_sql=sql_quals,
                qual_args=sql_qual_vals,
            )
        else:
            queries = []
        plan.tracer.log("generate_singleton_queries")

        if not non_singletons:
            with object_manager.ensure_objects(
                self, objects=required_objects, defer_release=True, tracer=plan.tracer
            ) as (required_objects, release_callback):
                return queries, release_callback, plan

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
                    _delete_temporary_table(engine, SPLITGRAPH_META_SCHEMA, staging_table)

            nonlocal release_callback
            release_callback.append(_f)

            # Apply the fragments (just the parts that match the qualifiers) to the staging area
            if quals:
                engine.apply_fragments(
                    [(SPLITGRAPH_META_SCHEMA, o) for o in non_singletons],
                    SPLITGRAPH_META_SCHEMA,
                    staging_table,
                    extra_quals=sql_quals,
                    extra_qual_args=sql_qual_vals,
                    schema_spec=self.table_schema,
                )
            else:
                engine.apply_fragments(
                    [(SPLITGRAPH_META_SCHEMA, o) for o in non_singletons],
                    SPLITGRAPH_META_SCHEMA,
                    staging_table,
                    schema_spec=self.table_schema,
                )
            engine.commit()
            query = self._generate_select_queries(SPLITGRAPH_META_SCHEMA, [staging_table], columns)[
                0
            ]
            yield query

        with object_manager.ensure_objects(
            self, objects=required_objects, defer_release=True, tracer=plan.tracer
        ) as (required_objects, release_callback):
            return itertools.chain(queries, _generate_nonsingleton_query()), release_callback, plan

    @contextmanager
    def query_lazy(self, columns, quals):
        """
        Run a read-only query against this table without materializing it.

        :param columns: List of columns from this table to fetch
        :param quals: List of qualifiers in conjunctive normal form. See the documentation for
            FragmentManager.filter_fragments for the actual format.
        :return: Generator of dictionaries of results.
        """

        query_gen, release_callback, _ = self.query_indirect(columns, quals)
        engine = self.repository.object_engine

        def _generate_results():
            for query in query_gen:
                result = engine.run_sql(query)
                for row in result:
                    yield {c: v for c, v in zip(columns, row)}

        try:
            yield _generate_results()
        finally:
            release_callback()

    def query(self, columns, quals):
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

    def _extract_singleton_fragments(self, object_manager, required_objects):
        # Get fragment boundaries (min-max PKs of every fragment).
        table_pk = [(t[1], t[2]) for t in self.table_schema if t[3]]
        if not table_pk:
            table_pk = [(t[1], t[2]) for t in self.table_schema]
        object_pks = object_manager.get_min_max_pks(required_objects, table_pk)
        # Group fragments into non-overlapping groups: those can be applied independently of each other.
        object_groups = get_chunk_groups(
            [
                (object_id, min_max[0], min_max[1])
                for object_id, min_max in zip(required_objects, object_pks)
            ]
        )
        singletons = []
        non_singletons = []
        for group in object_groups:
            if len(group) == 1:
                singletons.append(group[0][0])
            else:
                non_singletons.extend(object_id for object_id, _, _ in group)
        return non_singletons, singletons

    def _create_staging_table(self):
        staging_table = get_random_object_id()

        logging.info("Using staging table %s", staging_table)
        self.repository.object_engine.create_table(
            schema=SPLITGRAPH_META_SCHEMA,
            table=staging_table,
            schema_spec=self.table_schema,
            unlogged=True,
        )
        return staging_table

    def _generate_select_queries(self, schema, tables, columns, qual_sql=None, qual_args=None):
        engine = self.repository.object_engine
        cur = engine.connection.cursor()

        queries = []
        for table in tables:
            query = (
                SQL("SELECT ")
                + SQL(",").join(Identifier(c) for c in columns)
                + SQL(" FROM {}.{}").format(Identifier(schema), Identifier(table))
                + (SQL(" WHERE ") + qual_sql if qual_args else SQL(""))
            )
            query = cur.mogrify(query, qual_args)
            queries.append(query)

        cur.close()
        logging.info("Returning queries %r", queries)
        return queries
