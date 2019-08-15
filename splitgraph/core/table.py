"""Table metadata-related classes."""

import logging

from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core.fragment_manager import get_random_object_id, quals_to_sql, get_chunk_groups


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
            with object_manager.ensure_objects(self) as required_objects:
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

    def query(self, columns, quals):
        """
        Run a read-only query against this table without materializing it.

        :param columns: List of columns from this table to fetch
        :param quals: List of qualifiers in conjunctive normal form. See the documentation for
            FragmentManager.filter_fragments for the actual format.
        :return: List of tuples of results
        """

        sql_quals, sql_qual_vals = quals_to_sql(
            quals, column_types={c[1]: c[2] for c in self.table_schema}
        )

        object_manager = self.repository.objects
        with object_manager.ensure_objects(self, quals=quals) as required_objects:
            logging.info("Using fragments %r to satisfy the query", required_objects)
            if not required_objects:
                return []

            # Get fragment boundaries (min-max PKs of every fragment).
            table_pk = [t[1] for t in self.table_schema if t[3]]
            if not table_pk:
                table_pk = [t[1] for t in self.table_schema]
            object_pks = object_manager.extract_min_max_pks(required_objects, table_pk)

            # Group fragments into non-overlapping groups: those can be applied independently of each other.
            object_groups = get_chunk_groups(
                [
                    (object_id, min_max[0], min_max[1])
                    for object_id, min_max in zip(required_objects, object_pks)
                ]
            )

            # Special fast case: single-chunk groups can all be batched together
            # and queried directly (with a SELECT + UNION) without having to copy them to a staging table.
            # We also grab all fragments from multiple-fragment groups and batch them together
            # for future application.
            #
            # Technically, we could do multiple batches of application for these groups
            # (apply first batch to the staging table, extract the result, clean the table,
            # apply next batch etc): in the middle of it we could also talk back to the object
            # manager and release the objects that we don't need so that they can be garbage
            # collected. The tradeoff is that we perform more calls to apply_fragments (hence
            # more roundtrips).
            singletons = []
            non_singletons = []
            for group in object_groups:
                if len(group) == 1:
                    singletons.append(group[0][0])
                else:
                    non_singletons.extend(object_id for object_id, _, _ in group)

            logging.info(
                "Fragment grouping: %d singletons, %d non-singletons",
                len(singletons),
                len(non_singletons),
            )

            # Query singletons directly with SELECT ... FROM o1 UNION SELECT ... FROM o2 ...
            if singletons:
                yield from self._run_select_from_staging(
                    SPLITGRAPH_META_SCHEMA,
                    singletons,
                    columns,
                    drop_table=False,
                    qual_sql=sql_quals,
                    qual_args=sql_qual_vals,
                )

            # If the table consists of disjoint chunks, we're done.
            if not non_singletons:
                return

            # Create a temporary table for objects that need applying to each other
            staging_table = self._create_staging_table()

            # Apply the fragments (just the parts that match the qualifiers) to the staging area
            engine = self.repository.object_engine
            if quals:
                engine.apply_fragments(
                    [(SPLITGRAPH_META_SCHEMA, o) for o in non_singletons],
                    "pg_temp",
                    staging_table,
                    extra_quals=sql_quals,
                    extra_qual_args=sql_qual_vals,
                    schema_spec=self.table_schema,
                )
            else:
                engine.apply_fragments(
                    [(SPLITGRAPH_META_SCHEMA, o) for o in non_singletons],
                    "pg_temp",
                    staging_table,
                    schema_spec=self.table_schema,
                )
        yield from self._run_select_from_staging(
            "pg_temp", [staging_table], columns, drop_table=True
        )

    def _create_staging_table(self):
        staging_table = get_random_object_id()

        logging.info("Using staging table %s", staging_table)
        self.repository.object_engine.create_table(
            schema=None, table=staging_table, schema_spec=self.table_schema, temporary=True
        )
        return staging_table

    def _run_select_from_staging(
        self, schema, tables, columns, drop_table=False, qual_sql=None, qual_args=None
    ):
        """Runs the actual select query against the partially materialized table(s).
        If qual_sql is passed, this will include it in the SELECT query. Despite that Postgres
        will check our results again, this is still useful so that we don't pass all the rows
        in the fragment(s) through the Python runtime.

        If multiple tables are passed, the query will run the same SELECT with a UNION
        between all of them."""
        engine = self.repository.object_engine

        cur = engine.connection.cursor("sg_layered_query_cursor")

        query = SQL(" UNION ").join(
            SQL("SELECT ")
            + SQL(",").join(Identifier(c) for c in columns)
            + SQL(" FROM {}.{}").format(Identifier(schema), Identifier(table))
            + (SQL(" WHERE ") + qual_sql if qual_args else SQL(""))
            for table in tables
        )

        if qual_args:
            query = cur.mogrify(query, qual_args * len(tables))
        cur.execute(query)

        while True:
            try:
                yield {c: v for c, v in zip(columns, next(cur))}
            except StopIteration:
                # When the cursor has been consumed, delete the staging tables and close it.
                cur.close()
                if drop_table:
                    for table in tables:
                        engine.delete_table(schema, table)

                # End the transaction so that nothing else deadlocks (at this point we've returned
                # all the data we needed to the runtime so nothing will be lost).
                self.repository.commit_engines()
                return
