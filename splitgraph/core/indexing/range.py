# PG types we can run max/min on
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from psycopg2.sql import Composed, SQL
from psycopg2.sql import Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA, SPLITGRAPH_API_SCHEMA
from splitgraph.core._common import adapt, coerce_val_to_json, ResultShape, select
from splitgraph.engine.postgres.engine import PostgresEngine

_PG_INDEXABLE_TYPES = [
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


# Custom min/max functions that ignore Nones
def _min(left: Any, right: Union[int, date, str, float]) -> Union[int, date, str, float, Decimal]:
    return right if left is None else (left if right is None else min(left, right))


def _max(left: Any, right: Union[int, date, str, float]) -> Union[int, date, str, float, Decimal]:
    return right if left is None else (left if right is None else max(left, right))


def _qual_to_index_clause(
    qual: Union[Tuple[str, str, datetime], Tuple[str, str, int], Tuple[str, str, str]], ctype: str
) -> Union[
    Tuple[Composed, Tuple[str, int]],
    Tuple[Composed, Tuple[str, datetime]],
    Tuple[SQL, Tuple],
    Tuple[Composed, Tuple[str, str]],
]:
    """Convert our internal qual format into a WHERE clause that runs against an object's index entry.
    Returns a Postgres clause (as a Composable) and a tuple of arguments to be mogrified into it."""
    column_name, qual_op, value = qual

    # Our index is essentially a bloom filter: it returns True if an object _might_ have rows
    # that affect the result of a query with a given qual and False if it definitely doesn't.
    # Hence, we can combine qualifiers in a similar Boolean way (proof?)

    # If there's no index information for a given column, we have to assume it might match the qual.
    query = SQL("NOT (index -> 'range') ? %s OR ")
    args = [column_name]

    # If the column has to be greater than (or equal to) X, it only might exist in objects
    # whose maximum value is greater than (or equal to) X.
    if qual_op in (">", ">="):
        query += SQL("(index #>> '{{range,{},1}}')::" + ctype + "  " + qual_op + " %s").format(
            (Identifier(column_name))
        )
        args.append(value)
    # Similar for smaller than, but here we check that the minimum value is smaller than X.
    elif qual_op in ("<", "<="):
        query += SQL("(index #>> '{{range,{},0}}')::" + ctype + " " + qual_op + " %s").format(
            (Identifier(column_name))
        )
        args.append(value)
    elif qual_op == "=":
        query += SQL(
            "%s BETWEEN (index #>> '{{range,{0},0}}')::"
            + ctype
            + " AND (index #>> '{{range,{0},1}}')::"
            + ctype
        ).format((Identifier(column_name)))
        args.append(value)
    # Currently, we ignore the LIKE (~~) qualifier since we can only make a judgement when the % pattern is at
    # the end of a string.
    # For inequality, we can't really say when an object is definitely not pertinent to a qual:
    #   * if a <> X and X is included in an object's range, the object still might have values that aren't X.
    #   * if X isn't included in an object's range, the object definitely has values that aren't X so we have
    #     to fetch it.
    else:
        # For all other operators, we don't know if they will match so we assume that they will.
        return SQL("TRUE"), ()
    return query, tuple(args)


def _qual_to_sql_clause(qual: Tuple[str, str, str], ctype: str) -> Tuple[Composed, Tuple[str]]:
    """Convert a qual to a normal SQL clause that can be run against the actual object rather than the index."""
    column_name, qual_op, value = qual
    return SQL("{}::" + ctype + " " + qual_op + " %s").format(Identifier(column_name)), (value,)


def _quals_to_clause(
    quals: Any, column_types: Dict[str, str], qual_to_clause: Callable = _qual_to_index_clause
) -> Any:
    if not quals:
        return SQL(""), ()

    def _internal_quals_to_clause(or_quals):
        clauses, args = zip(*[qual_to_clause(q, column_types[q[0]]) for q in or_quals])
        return (
            SQL(" OR ").join(SQL("(") + c + SQL(")") for c in clauses),
            tuple([a for arg in args for a in arg]),
        )

    clauses, args = zip(*[_internal_quals_to_clause(q) for q in quals])
    return (
        SQL(" AND ").join(SQL("(") + c + SQL(")") for c in clauses),
        tuple([a for arg in args for a in arg]),
    )


def quals_to_sql(
    quals: Optional[List[List[Tuple[str, str, str]]]], column_types: Dict[str, str]
) -> Union[Tuple[Composed, Tuple[str]], Tuple[SQL, Tuple], Tuple[Composed, Tuple[str, str]]]:
    """
    Convert a list of qualifiers in CNF to a fragment of a Postgres query
    :param quals: Qualifiers in CNF
    :param column_types: Dictionary of column names and their types
    :return: SQL Composable object and a tuple of arguments to be mogrified into it.
    """

    return _quals_to_clause(quals, column_types, qual_to_clause=_qual_to_sql_clause)


def extract_min_max_pks(engine: PostgresEngine, fragments: List[str], table_pks: List[str]) -> Any:
    """
    Extract minimum/maximum PK values for given fragments.

    :param engine: Engine the objects live on
    :param fragments: IDs of objects
    :param table_pks: List of columns forming the table primary key
    :return: List of min/max primary key for every object.
    """

    # Why can't we use the index for individual columns here? If the PK is composite, consider this example:
    #
    # (1, 1)   CHUNK 1
    # (1, 2)   <- pk1: min 1, max 2; pk2: min1, max2 but (pk1, pk2): min (1, 1), max (2, 1)
    # (2, 1)
    # ------   CHUNK 2
    # (2, 2)   <- pk1: min 2, max 2; pk2: min 2, max 2;
    #
    # Say we have a changeset doing UPDATE pk=(2,2). If we use the index by each part of the key separately,
    # it fits both the first and the second chunk. This essentially means that chunks now overlap:
    # we now have to apply chunk 2 (and everything inheriting from it) after chunk 1 (and everything that
    # inherits from it) and make sure to attach the new fragment to chunk 2.
    # This could be solved by including composite PKs in the index as well, not just individual columns.
    # Currently, we assume the objects are local so doing this is mostly OK but that's a strong assumption
    # (there isn't much preventing us from evicting objects once they've been used to materialize a table,
    # so if we do that, we won't want to redownload them again just to find their boundaries).

    min_max = []
    pk_sql = SQL(",").join(Identifier(p) for p in table_pks)
    for fragment in fragments:
        query = (
            SQL("SELECT ")
            + pk_sql
            + SQL(" FROM {}.{} ORDER BY ").format(
                Identifier(SPLITGRAPH_META_SCHEMA), Identifier(fragment)
            )
        )
        frag_min = engine.run_sql(
            query + pk_sql + SQL(" LIMIT 1"), return_shape=ResultShape.ONE_MANY
        )
        frag_max = engine.run_sql(
            query
            + SQL(",").join(Identifier(p) + SQL(" DESC") for p in table_pks)
            + SQL(" LIMIT 1"),
            return_shape=ResultShape.ONE_MANY,
        )

        min_max.append((frag_min, frag_max))
    return min_max


def generate_range_index(
    object_engine: PostgresEngine,
    object_id: str,
    table_schema: List[Tuple[int, str, str, bool]],
    changeset: Any,
) -> Dict[str, Any]:
    """
    Calculate the minimim/maximum values of every column in the object (including deleted values).

    :param object_id: ID of the object.
    :param table_schema: Schema of the table
    :param changeset: Changeset (old values will be included in the index
    :return: Dictionary of {column: [min, max]}
    """
    object_pk = [c[1] for c in table_schema if c[3]]
    if not object_pk:
        object_pk = [c[1] for c in table_schema]
    column_types = {c[1]: c[2] for c in table_schema}
    columns_to_index = [c[1] for c in table_schema if c[2] in _PG_INDEXABLE_TYPES]
    query = SQL("SELECT ") + SQL(",").join(
        SQL("MIN({0}), MAX({0})").format(Identifier(c)) for c in columns_to_index
    )
    query += SQL(" FROM {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id))
    result = object_engine.run_sql(query, return_shape=ResultShape.ONE_MANY)
    index = {
        col: (cmin, cmax) for col, cmin, cmax in zip(columns_to_index, result[0::2], result[1::2])
    }
    # Also explicitly store the ranges of composite PKs (since they won't be included
    # in the columns list) to be used for faster chunking/querying.
    if len(object_pk) > 1:
        # Add the PK to the same index dict but prefix it with a dollar sign so that
        # it explicitly doesn't clash with any other columns.
        index["$pk"] = extract_min_max_pks(object_engine, [object_id], object_pk)[0]
    if changeset:
        # Expand the index ranges to include the old row values in this chunk.
        # Why is this necessary? Say we have a table of (key (PK), value) and a
        # query "value = 42". Say we have 2 objects:
        #
        #   key | value
        #   1   | 42
        #
        #   key | value
        #   1   | 43   (UPDATED)
        #
        # If we don't include the old value that object 2 overwrote in the index, we'll disregard object 2
        # when inspecting the index for that query (since there, "value" only spans [43, 43]) and give the
        # wrong answer (1, 42) even though we should give (1, 43). Similarly with deletes: if the index for
        # an object doesn't say "some of the values spanning this range are deleted in this chunk",
        # we won't fetch the object.
        #
        # See test_lq_qual_filtering for these test cases.

        # We don't need to do this for the PK since the PK is always specified in deletes.

        # For DELETEs, we put NULLs in the non-PK columns; make sure we ignore them here.
        for _, old_row in changeset.values():
            for col, val in old_row.items():
                # Ignore columns that we aren't indexing because they have unsupported types.
                # Also ignore NULL values.
                if col not in columns_to_index or val is None:
                    continue
                # The audit trigger stores the old row values as JSON so only supports strings and floats/ints.
                # Hence, we have to coerce them into the values returned by the index.
                val = adapt(val, column_types[col])
                index[col] = (_min(index[col][0], val), _max(index[col][1], val))
    range_index = {
        k: (coerce_val_to_json(v[0]), coerce_val_to_json(v[1])) for k, v in index.items()
    }
    return range_index


def filter_range_index(
    metadata_engine: PostgresEngine, object_ids: List[str], quals: Any, column_types: Dict[str, str]
) -> List[str]:
    clause, args = _quals_to_clause(quals, column_types)
    query = (
        select("get_object_meta", "object_id", table_args="(%s)", schema=SPLITGRAPH_API_SCHEMA)
        + SQL(" WHERE ")
        + clause
    )

    return metadata_engine.run_sql(
        query, [object_ids] + list(args), return_shape=ResultShape.MANY_ONE
    )
