"""Module imported by Multicorn on the Splitgraph engine server: a foreign data wrapper that implements
layered querying (read-only queries to Splitgraph tables without materialization)."""

import logging

from psycopg2.sql import Identifier, SQL

from splitgraph import SPLITGRAPH_META_SCHEMA, Repository, get_engine
from splitgraph.core.object_manager import get_random_object_id, ObjectManager
from splitgraph.engine.postgres.engine import _generate_where_clause

try:
    from multicorn import ForeignDataWrapper, ANY, ALL
    from multicorn.utils import log_to_postgres
except ImportError:
    # Multicorn not installed (OK if we're not on the engine machine).
    pass

_PG_LOGLEVEL = logging.INFO


class QueryingForeignDataWrapper(ForeignDataWrapper):
    """The actual Multicorn LQ FDW class"""

    @staticmethod
    def _quals_to_postgres(quals):
        """Converts a list of Multicorn Quals to Postgres clauses (joined with AND)."""

        def _qual_to_pg(qual):
            # Returns a SQL object + a list of args to be mogrified into it.
            if qual.is_list_operator:
                value = qual.value
                operator = qual.operator[0] + ' ' + '%s' % ('ANY' if qual.list_any_or_all == ANY else 'ALL')
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

    def _run_select_from_staging(self, schema, table, columns, drop_table=False):
        """Runs the actual select query against the partially materialized table.
        There's no point in applying the quals since Postgres doesn't trust the FDW and will reapply them
        once again"""
        cur = self.engine.connection.cursor('sg_layered_query_cursor')
        query = SQL("SELECT ") + SQL(','.join('{}' for _ in columns)).format(*[Identifier(c) for c in columns]) \
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

        with self.object_manager.ensure_objects(self.table) as (snap, diffs):
            log_to_postgres("Using SNAP %s, DIFFs %r to satisfy the query" % (snap, diffs), _PG_LOGLEVEL)
            if not diffs:
                # If we only have the SNAP, we can just send SELECTs directly to it.
                return self._run_select_from_staging(SPLITGRAPH_META_SCHEMA, snap, columns, drop_table=False)

            # Accumulate the query result in a temporary table.
            staging_table = self._create_staging_table(snap)

            # 1) First, insert all rows in the SNAP where the PK will be updated, marking them as sg_meta_keep_pk=True
            #    (meaning we won't check them against the qualifiers until the very end).
            ri_cols, _ = zip(*self.engine.get_change_key(SPLITGRAPH_META_SCHEMA, snap))
            all_cols = self.engine.get_column_names(SPLITGRAPH_META_SCHEMA, snap)
            all_cols_sql = SQL(','.join('{}' for _ in all_cols)).format(*[Identifier(c) for c in all_cols])

            # Faster route here: if all quals only touch the PK, we don't need to hold on to tuples that will
            # eventually get updated (to see if they start satisfying the qualifiers again) since UPDATEs
            # can't change PK by definition.
            pk_only_quals = all(q.field_name in ri_cols for q in quals)

            if not pk_only_quals:
                for object_id in diffs:
                    query = SQL("INSERT INTO {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                            Identifier(staging_table))
                    query += SQL(" (") + all_cols_sql + SQL(",sg_meta_keep_pk)")
                    # SELECT <snap_id>.col1, <snap_id>.col2, TRUE FROM <snap_id> join <object_id> on [pk_cols]
                    query += SQL(" (SELECT ") + SQL(','.join('{}.{}' for _ in all_cols)).format(
                        *[f for c in all_cols for f in (Identifier(snap), Identifier(c))]) \
                             + SQL(",TRUE")
                    query += SQL(" FROM {0}.{1} JOIN {0}.{2} ON ").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier(snap), Identifier(object_id))
                    query += _generate_where_clause(schema=SPLITGRAPH_META_SCHEMA, table=snap, cols=ri_cols,
                                                    table_2=object_id, schema_2=SPLITGRAPH_META_SCHEMA)
                    query += SQL(" WHERE {}.sg_action_kind=2) ON CONFLICT DO NOTHING").format(Identifier(object_id))
                    log_to_postgres(query.as_string(self.engine.connection), _PG_LOGLEVEL)
                    self.engine.run_sql(query)

            # 2) Add all rows from the SNAP satisfying the query (if they already exist in staging, skip them).
            #    This time, set sg_meta_keep_pk to False (if they stop satisfying the qualifiers, they will be deleted).
            query = SQL("INSERT INTO {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(staging_table))
            query += SQL(" (") + all_cols_sql + SQL(",sg_meta_keep_pk)")
            query += SQL(" (SELECT ") + all_cols_sql + SQL(",FALSE")
            query += SQL(" FROM {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(snap))
            if quals:
                query += SQL(" WHERE ") + qual_sql
            query += SQL(") ON CONFLICT DO NOTHING")
            log_to_postgres(query.as_string(self.engine.connection), _PG_LOGLEVEL)
            self.engine.run_sql(query, qual_vals)

            # 3) Apply the diffs to the partially materialized table, making sure to discard rows that don't match
            #    the qualifiers any more
            if quals:
                discard_query = SQL("DELETE FROM {}.{} WHERE ").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                       Identifier(staging_table)) \
                                + SQL("sg_meta_keep_pk = FALSE AND NOT (") + qual_sql + SQL(")")

                # For the final diff, we don't need to apply any quals to the staging table since Postgres doesn't trust
                # us and will apply them/do projections anyway.
                self.engine.batch_apply_diff_objects([(SPLITGRAPH_META_SCHEMA, o) for o in diffs],
                                                     SPLITGRAPH_META_SCHEMA, staging_table,
                                                     run_after_every=discard_query,
                                                     run_after_every_args=qual_vals,
                                                     ignore_cols=['sg_meta_keep_pk'])
            else:
                self.engine.batch_apply_diff_objects([(SPLITGRAPH_META_SCHEMA, o) for o in diffs],
                                                     SPLITGRAPH_META_SCHEMA, staging_table,
                                                     ignore_cols=['sg_meta_keep_pk'])
        return self._run_select_from_staging(SPLITGRAPH_META_SCHEMA, staging_table, columns,
                                             drop_table=True)

    def _create_staging_table(self, snap):
        staging_table = get_random_object_id()
        log_to_postgres("Using staging table %s" % staging_table, _PG_LOGLEVEL)
        self.engine.run_sql(SQL("CREATE TABLE {0}.{1} AS SELECT * FROM {0}.{2} LIMIT 1 WITH NO DATA").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(staging_table), Identifier(snap)))
        self.engine.run_sql(SQL("ALTER TABLE {}.{} ADD COLUMN sg_meta_keep_pk BOOLEAN DEFAULT TRUE").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(staging_table)))
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

        self.object_manager = ObjectManager(self.engine)
        # Since the actual base objects required to resolve a table aren't likely to change (we don't allow overwriting
        # history), there's no point in reusing it (though how do we find out the current occupied cache size?)
        # self.object_tree = self.object_manager.get_full_object_tree()
