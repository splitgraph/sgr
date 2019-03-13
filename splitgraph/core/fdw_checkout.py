"""Module imported by Multicorn on the Splitgraph engine server: a foreign data wrapper that implements
layered querying (read-only queries to Splitgraph tables without materialization)."""

import logging

from psycopg2.sql import Identifier, SQL
from splitgraph import SPLITGRAPH_META_SCHEMA, Repository, get_engine
from splitgraph.core._common import adapt
from splitgraph.core.object_manager import get_random_object_id, ObjectManager

try:
    from multicorn import ForeignDataWrapper, ANY, ALL
    from multicorn.utils import log_to_postgres
except ImportError:
    # Multicorn not installed (OK if we're not on the engine machine).
    pass

_PG_LOGLEVEL = logging.WARNING


class QueryingForeignDataWrapper(ForeignDataWrapper):
    """The actual Multicorn LQ FDW class"""

    def _quals_to_postgres(self, quals):
        """Converts a list of Multicorn Quals to Postgres clauses (joined with AND)."""

        def _qual_to_pg(qual):
            # Returns a SQL object + a list of args to be mogrified into it.
            if qual.is_list_operator:
                value = [adapt(v, self.column_types[qual.field_name]) for v in qual.value]
                operator = qual.operator[0] + ' ' + ('ANY' if qual.list_any_or_all == ANY else 'ALL')
                operator += '(ARRAY[' + ','.join('%s' for _ in range(len(value))) + '])'
            else:
                operator = qual.operator + ' %s'
                value = [qual.value]
            return Identifier(qual.field_name) + SQL(" " + operator), value

        sql_objs = []
        vals = []
        for qual in quals:
            sql, value = _qual_to_pg(qual)
            sql_objs.append(sql)
            vals.extend(value)

        return SQL(" AND ").join(s for s in sql_objs), vals

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
                else:
                    # Convert col op ALL(ARRAY[a,b,c...]) into (cop op a) AND (col op b)...
                    # which is multiple AND clauses of one OR each
                    return [[(qual.field_name, qual.operator[0], v)] for v in qual.value]
            else:
                if qual.value is None:
                    return [[]]
                return [[(qual.field_name, qual.operator, qual.value)]]

        return [q for qual in quals for q in _qual_to_cnf(qual) if q != []]

    def _run_select_from_staging(self, schema, table, columns, drop_table=False):
        """Runs the actual select query against the partially materialized table.
        There's no point in applying the quals since Postgres doesn't trust the FDW and will reapply them
        once again"""
        cur = self.engine.connection.cursor('sg_layered_query_cursor')
        query = SQL("SELECT ") + SQL(',').join(Identifier(c) for c in columns) \
                + SQL(" FROM {}.{}").format(Identifier(schema),
                                            Identifier(table))
        log_to_postgres("SELECT FROM STAGING: " + query.as_string(self.engine.connection), _PG_LOGLEVEL)
        cur.execute(query)

        while True:
            try:
                yield {c: v for c, v in zip(columns, next(cur))}
            except StopIteration:
                # When the cursor has been consumed, delete the staging table and close it.
                cur.close()
                if drop_table:
                    self.engine.delete_table(schema, table)

                # End the transaction so that nothing else deadlocks (at this point we've returned
                # all the data we needed to the runtime so nothing will be lost).
                self.engine.commit()
                return

    def execute(self, quals, columns, sortkeys=None):
        """Main Multicorn entry point."""

        # Multicorn passes a _set_ of columns to us instead of a list, so the order of iteration through
        # it can randomly change and the order in which we return the tuples might not be the one it expects.
        columns = list(columns)
        # For quals, the more elaborate ones (like table.id = table.name or similar) actually aren't passed here
        # at all and PG filters them out later on.
        qual_sql, qual_vals = self._quals_to_postgres(quals)
        cnf_quals = self._quals_to_cnf(quals)
        log_to_postgres("quals: %r" % (quals,), _PG_LOGLEVEL)
        log_to_postgres("CNF quals: %r" % (cnf_quals,), _PG_LOGLEVEL)

        with self.object_manager.ensure_objects(self.table, quals=cnf_quals) as required_objects:
            log_to_postgres("Using fragments %r to satisfy the query" % (required_objects,), _PG_LOGLEVEL)
            if len(required_objects) == 1:
                # If one object has our answer, we can send queries directly to it
                return self._run_select_from_staging(SPLITGRAPH_META_SCHEMA, required_objects[0], columns,
                                                     drop_table=False)

            # Accumulate the query result in a temporary table.
            staging_table = self._create_staging_table(required_objects[0])

            # Apply the fragments to the staging area, discarding rows that don't match the qualifiers any more.
            if quals:
                discard_query = SQL("DELETE FROM {}.{} WHERE ").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                       Identifier(staging_table)) \
                                + SQL("NOT (") + qual_sql + SQL(")")

                # self.engine.batch_apply_diff_objects([(SPLITGRAPH_META_SCHEMA, o) for o in diffs],
                #                                      SPLITGRAPH_META_SCHEMA, staging_table,
                #                                      run_after_every=discard_query,
                #                                      run_after_every_args=qual_vals,
                #                                      ignore_cols=['sg_meta_keep_pk'])
                for fragment in required_objects:
                    self.engine.apply_fragment(SPLITGRAPH_META_SCHEMA, fragment, SPLITGRAPH_META_SCHEMA, staging_table)
                    self.engine.run_sql(discard_query, qual_vals)

            else:
                for fragment in required_objects:
                    self.engine.apply_fragment(SPLITGRAPH_META_SCHEMA, fragment, SPLITGRAPH_META_SCHEMA, staging_table)
                # self.engine.batch_apply_diff_objects([(SPLITGRAPH_META_SCHEMA, o) for o in diffs],
                #                                      SPLITGRAPH_META_SCHEMA, staging_table,
                #                                      ignore_cols=['sg_meta_keep_pk'])
        return self._run_select_from_staging(SPLITGRAPH_META_SCHEMA, staging_table, columns,
                                             drop_table=True)

    def _create_staging_table(self, snap):
        staging_table = get_random_object_id()
        log_to_postgres("Using staging table %s" % staging_table, _PG_LOGLEVEL)
        self.engine.run_sql(SQL("CREATE TABLE {0}.{1} AS SELECT * FROM {0}.{2} LIMIT 1 WITH NO DATA").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(staging_table), Identifier(snap)))
        pks = self.engine.get_primary_keys(SPLITGRAPH_META_SCHEMA, snap)
        if pks:
            self.engine.run_sql(SQL("ALTER TABLE {}.{} ADD PRIMARY KEY (").format(
                Identifier(SPLITGRAPH_META_SCHEMA), Identifier(staging_table)) + SQL(',').join(
                SQL("{}").format(Identifier(c)) for c, _ in pks) + SQL(")"))
        return staging_table

    def __init__(self, fdw_options, fdw_columns):
        """The foreign data wrapper is initialized on the first query.
        Args:
            fdw_options (dict): The foreign data wrapper options. It is a dictionary
                mapping keys from the sql "CREATE FOREIGN TABLE"
                statement options. It is left to the implementor
                to decide what should be put in those options, and what
                to do with them.

        """
        # Dict of connection parameters as well as the table, repository and image hash to query.
        self.fdw_options = fdw_options

        # The foreign datawrapper columns (name -> ColumnDefinition).
        self.fdw_columns = fdw_columns

        # Try using a UNIX socket if the engine is local to us
        self.engine = get_engine(self.fdw_options['engine'], bool(self.fdw_options.get('use_socket', False)))

        self.repository = Repository(fdw_options['namespace'], self.fdw_options['repository'], self.engine)
        self.table = self.repository.images[self.fdw_options['image_hash']].get_table(self.fdw_options['table'])
        self.column_types = {c[1]: c[2] for c in self.table.table_schema}

        self.object_manager = ObjectManager(self.engine)
