import json

from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SPLITGRAPH_META_SCHEMA, SplitGraphException
from splitgraph.objects.creation import _convert_vals
from splitgraph.objects.utils import get_replica_identity, _generate_where_clause
from splitgraph.pg_utils import _get_column_names


def apply_record_to_staging(conn, object_id, mountpoint, destination):
    """
    Applies a DIFF table stored in `object_id` to destination.

    :param conn: psycopg connection object.
    :param object_id: Object ID of the DIFF table.
    :param mountpoint: Schema where the destination table is located.
    :param destination: Target table.
    """
    queries = []
    repl_id = get_replica_identity(conn, SPLITGRAPH_META_SCHEMA, object_id)
    ri_cols, _ = zip(*repl_id)

    # Minor hack alert: here we assume that the PK of the object is the PK of the table it refers to, which means
    # that we are expected to have the PKs applied to the object table no matter how it originated.
    if sorted(ri_cols) == sorted(_get_column_names(conn, SPLITGRAPH_META_SCHEMA, object_id)):
        raise SplitGraphException("Error determining the replica identity of %s. " % object_id +
                                  "Have primary key constraints been applied?")

    with conn.cursor() as cur:
        # Apply deletes
        cur.execute(SQL("DELETE FROM {0}.{2} USING {1}.{3} WHERE {1}.{3}.sg_action_kind = 1").format(
            Identifier(mountpoint), Identifier(SPLITGRAPH_META_SCHEMA), Identifier(destination),
            Identifier(object_id)) + SQL(" AND ") + _generate_where_clause(mountpoint, destination, ri_cols, object_id,
                                                                           SPLITGRAPH_META_SCHEMA))

        # Generate queries for inserts
        cur.execute(
            SQL("SELECT * FROM {}.{} WHERE sg_action_kind = 0").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                       Identifier(object_id)))
        for row in cur:
            # Not sure if we can rely on ordering here.
            # Also for the future: if all column names are the same, we can do a big INSERT.
            action_data = json.loads(row[-1])
            cols_to_insert = list(ri_cols) + action_data['c']
            vals_to_insert = _convert_vals(list(row[:-2]) + action_data['v'])

            query = SQL("INSERT INTO {}.{} (").format(Identifier(mountpoint), Identifier(destination))
            query += SQL(','.join('{}' for _ in cols_to_insert)).format(*[Identifier(c) for c in cols_to_insert])
            query += SQL(") VALUES (" + ','.join('%s' for _ in vals_to_insert) + ')')
            query = cur.mogrify(query, vals_to_insert)
            queries.append(query)
        # ...and updates
        cur.execute(
            SQL("SELECT * FROM {}.{} WHERE sg_action_kind = 2").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                       Identifier(object_id)))
        for row in cur:
            action_data = json.loads(row[-1])
            ri_vals = list(row[:-2])
            cols_to_insert = action_data['c']
            vals_to_insert = action_data['v']

            query = SQL("UPDATE {}.{} SET ").format(Identifier(mountpoint), Identifier(destination))
            query += SQL(', '.join("{} = %s" for _ in cols_to_insert)).format(*(Identifier(i) for i in cols_to_insert))
            query += SQL(" WHERE ") + _generate_where_clause(mountpoint, destination, ri_cols)
            queries.append(cur.mogrify(query, vals_to_insert + ri_vals))
        # Apply the insert/update queries (might not exist if the diff was all deletes)
        if queries:
            cur.execute(b';'.join(queries))  # maybe some pagination needed here.
