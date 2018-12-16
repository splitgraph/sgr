"""
Internal functions for materializing Splitgraph objects into tables
"""

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.connection import get_connection
from splitgraph.engine import get_engine
from splitgraph.exceptions import SplitGraphException


def apply_record_to_staging(object_id, dest_schema, dest_table):
    """
    Applies a DIFF table stored in `object_id` to destination.

    :param object_id: Object ID of the DIFF table.
    :param dest_schema: Schema where the destination table is located.
    :param dest_table: Target table.
    """
    queries = []
    repl_id = get_engine().get_change_key(SPLITGRAPH_META_SCHEMA, object_id)
    ri_cols, _ = zip(*repl_id)

    # Minor hack alert: here we assume that the PK of the object is the PK of the table it refers to, which means
    # that we are expected to have the PKs applied to the object table no matter how it originated.
    if sorted(ri_cols) == sorted(get_engine().get_column_names(SPLITGRAPH_META_SCHEMA, object_id)):
        raise SplitGraphException("Error determining the replica identity of %s. " % object_id +
                                  "Have primary key constraints been applied?")

    with get_connection().cursor() as cur:
        # Apply deletes
        cur.execute(SQL("DELETE FROM {0}.{2} USING {1}.{3} WHERE {1}.{3}.sg_action_kind = 1").format(
            Identifier(dest_schema), Identifier(SPLITGRAPH_META_SCHEMA), Identifier(dest_table),
            Identifier(object_id)) + SQL(" AND ") + _generate_where_clause(dest_schema, dest_table, ri_cols, object_id,
                                                                           SPLITGRAPH_META_SCHEMA))

        # Generate queries for inserts
        cur.execute(
            SQL("SELECT * FROM {}.{} WHERE sg_action_kind = 0").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                       Identifier(object_id)))
        for row in cur:
            # Not sure if we can rely on ordering here.
            # Also for the future: if all column names are the same, we can do a big INSERT.
            action_data = row[-1]
            cols_to_insert = list(ri_cols) + action_data['c']
            vals_to_insert = _convert_vals(list(row[:-2]) + action_data['v'])

            query = SQL("INSERT INTO {}.{} (").format(Identifier(dest_schema), Identifier(dest_table))
            query += SQL(','.join('{}' for _ in cols_to_insert)).format(*[Identifier(c) for c in cols_to_insert])
            query += SQL(") VALUES (" + ','.join('%s' for _ in vals_to_insert) + ')')
            query = cur.mogrify(query, vals_to_insert)
            queries.append(query)
        # ...and updates
        cur.execute(
            SQL("SELECT * FROM {}.{} WHERE sg_action_kind = 2").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                       Identifier(object_id)))
        for row in cur:
            action_data = row[-1]
            ri_vals = list(row[:-2])
            cols_to_insert = action_data['c']
            vals_to_insert = action_data['v']

            query = SQL("UPDATE {}.{} SET ").format(Identifier(dest_schema), Identifier(dest_table))
            query += SQL(', '.join("{} = %s" for _ in cols_to_insert)).format(*(Identifier(i) for i in cols_to_insert))
            query += SQL(" WHERE ") + _generate_where_clause(dest_schema, dest_table, ri_cols)
            queries.append(cur.mogrify(query, vals_to_insert + ri_vals))
        # Apply the insert/update queries (might not exist if the diff was all deletes)
        if queries:
            cur.execute(b';'.join(queries))  # maybe some pagination needed here.


def _convert_vals(vals):
    """Psycopg returns jsonb objects as dicts but doesn't actually accept them directly
    as a query param. Hence, we have to wrap them in the Json datatype when applying a DIFF
    to a table."""
    # This might become a bottleneck since we call this for every row in the diff + function
    # calls are expensive in Python -- maybe there's a better way (e.g. tell psycopg to not convert
    # things to dicts or apply diffs in-driver).
    return [v if not isinstance(v, dict) else Json(v) for v in vals]


def _generate_where_clause(schema, table, cols, table_2=None, schema_2=None):
    if not table_2:
        return SQL(" AND ").join(SQL("{}.{}.{} = %s").format(
            Identifier(schema), Identifier(table), Identifier(c)) for c in cols)
    return SQL(" AND ").join(SQL("{}.{}.{} = {}.{}.{}").format(
        Identifier(schema), Identifier(table), Identifier(c),
        Identifier(schema_2), Identifier(table_2), Identifier(c)) for c in cols)
