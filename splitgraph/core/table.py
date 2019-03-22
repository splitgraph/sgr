"""Table metadata-related classes."""

import logging

from psycopg2.sql import SQL, Identifier
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core.fragment_manager import get_random_object_id, quals_to_sql


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
        engine = self.repository.engine
        object_manager = self.repository.objects
        engine.delete_table(destination_schema, destination)

        if not lq_server:
            # Copy the given snap id over to "staging" and apply the DIFFS
            with object_manager.ensure_objects(self) as required_objects:
                engine.copy_table(SPLITGRAPH_META_SCHEMA, required_objects[0], destination_schema, destination,
                                  with_pk_constraints=True)
                if len(required_objects) > 1:
                    logging.info("Applying %d fragment(s)...", (len(required_objects) - 1))
                    engine.apply_fragments([(SPLITGRAPH_META_SCHEMA, d) for d in required_objects[1:]],
                                           destination_schema, destination)
        else:
            query = SQL("CREATE FOREIGN TABLE {}.{} (") \
                .format(Identifier(destination_schema), Identifier(self.table_name))
            query += SQL(','.join(
                "{} %s " % ctype for _, _, ctype, _ in self.table_schema)).format(
                *(Identifier(cname) for _, cname, _, _ in self.table_schema))
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

        sql_quals, sql_qual_vals = quals_to_sql(quals, column_types={c[1]: c[2] for c in self.table_schema})

        object_manager = self.repository.objects
        engine = self.repository.engine
        with object_manager.ensure_objects(self, quals=quals) as required_objects:
            logging.info("Using fragments %r to satisfy the query", required_objects)
            if not required_objects:
                return []
            if len(required_objects) == 1:
                # If one object has our answer, we can send queries directly to it
                return self._run_select_from_staging(SPLITGRAPH_META_SCHEMA, required_objects[0], columns,
                                                     drop_table=False, qual_sql=sql_quals, qual_args=sql_qual_vals)

            # Accumulate the query result in a temporary table.
            staging_table = self._create_staging_table(required_objects[0])

            # Apply the fragments (just the parts that match the qualifiers) to the staging area
            if quals:
                engine.apply_fragments([(SPLITGRAPH_META_SCHEMA, o) for o in required_objects],
                                       SPLITGRAPH_META_SCHEMA, staging_table,
                                       extra_quals=sql_quals,
                                       extra_qual_args=sql_qual_vals)
            else:
                engine.apply_fragments([(SPLITGRAPH_META_SCHEMA, o) for o in required_objects],
                                       SPLITGRAPH_META_SCHEMA, staging_table)
        return self._run_select_from_staging(SPLITGRAPH_META_SCHEMA, staging_table, columns,
                                             drop_table=True)

    def _create_staging_table(self, snap):
        staging_table = get_random_object_id()
        engine = self.repository.engine

        logging.info("Using staging table %s", staging_table)
        engine.run_sql(SQL("CREATE TABLE {0}.{1} AS SELECT * FROM {0}.{2} LIMIT 1 WITH NO DATA").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(staging_table), Identifier(snap)))
        pks = engine.get_primary_keys(SPLITGRAPH_META_SCHEMA, snap)
        if pks:
            engine.run_sql(SQL("ALTER TABLE {}.{} ADD PRIMARY KEY (").format(
                Identifier(SPLITGRAPH_META_SCHEMA), Identifier(staging_table)) + SQL(',').join(
                SQL("{}").format(Identifier(c)) for c, _ in pks) + SQL(")"))
        return staging_table

    def _run_select_from_staging(self, schema, table, columns, drop_table=False, qual_sql=None, qual_args=None):
        """Runs the actual select query against the partially materialized table.
        If qual_sql is passed, this will include it in the SELECT query. Despite that Postgres
        will check our results again, this is still useful so that we don't pass all the rows
        in the SNAP through the Python runtime."""
        engine = self.repository.engine

        cur = engine.connection.cursor('sg_layered_query_cursor')
        query = SQL("SELECT ") + SQL(',').join(Identifier(c) for c in columns) \
                + SQL(" FROM {}.{}").format(Identifier(schema),
                                            Identifier(table))
        if qual_args:
            query += SQL(" WHERE ") + qual_sql
            query = cur.mogrify(query, qual_args)
        cur.execute(query)

        while True:
            try:
                yield {c: v for c, v in zip(columns, next(cur))}
            except StopIteration:
                # When the cursor has been consumed, delete the staging table and close it.
                cur.close()
                if drop_table:
                    engine.delete_table(schema, table)

                # End the transaction so that nothing else deadlocks (at this point we've returned
                # all the data we needed to the runtime so nothing will be lost).
                engine.commit()
                return
