import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, cast, TYPE_CHECKING

from psycopg2.sql import Composed, SQL, Composable
from psycopg2.sql import Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA, SPLITGRAPH_API_SCHEMA
from splitgraph.core.common import adapt, coerce_val_to_json
from splitgraph.core.sql import select
from splitgraph.core.types import Quals, Changeset, TableSchema, Comparable
from splitgraph.engine import ResultShape
from splitgraph.engine.postgres.engine import PG_INDEXABLE_TYPES

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PsycopgEngine

T = TypeVar("T", bound=Comparable)


# Custom min/max functions that ignore Nones
def _min(left: Optional[T], right: Optional[T]) -> Optional[T]:
    return right if left is None else (left if right is None else min(left, right))


def _max(left: Optional[T], right: Optional[T]) -> Optional[T]:
    return right if left is None else (left if right is None else max(left, right))


def _inject_collation(qual: str, ctype: str):
    # On engines that have LC_COLLATE set to something like en_US, we still
    # want string comparison to mimic C/Python since we rely on that order
    # in range indexing.
    if ctype in ("text", "varchar", "character varying", "char"):
        return qual + ' COLLATE "C"'
    return qual


def _strip_type_mod(ctype: str) -> str:
    """Convert e.g. numeric(5,3) into numeric for the purpose of
    checking if we can run comparisons on this type and add it to the range index.
    """
    if "(" in ctype:
        ctype = ctype[: ctype.index("(")]
    if "[" in ctype:
        ctype = ctype[: ctype.index("[")]
    return ctype


def _qual_to_index_clause(qual: Tuple[str, str, Any], ctype: str) -> Tuple[SQL, Tuple]:
    """Convert our internal qual format into a WHERE clause that runs against an object's index entry.
    Returns a Postgres clause (as a Composable) and a tuple of arguments to be mogrified into it."""
    column_name, qual_op, value = qual

    # Our index is essentially a bloom filter: it returns True if an object _might_ have rows
    # that affect the result of a query with a given qual and False if it definitely doesn't.
    # Hence, we can combine qualifiers in a similar Boolean way (proof?)

    # If there's no index information for a given column, we have to assume it might match the qual.
    query = SQL("NOT (index -> 'range') ? %s OR ")
    args: List[Any] = [column_name]

    # If the column has to be greater than (or equal to) X, it only might exist in objects
    # whose maximum value is greater than (or equal to) X.
    if qual_op in (">", ">="):
        query += SQL(
            _inject_collation("(index #>> '{{range,{},1}}'", ctype)
            + ")::"
            + ctype
            + "  "
            + qual_op
            + " %s"
        ).format((Identifier(column_name)))
        args.append(value)
    # Similar for smaller than, but here we check that the minimum value is smaller than X.
    elif qual_op in ("<", "<="):
        query += SQL(
            _inject_collation("(index #>> '{{range,{},0}}'", ctype)
            + ")::"
            + ctype
            + " "
            + qual_op
            + " %s"
        ).format((Identifier(column_name)))
        args.append(value)
    elif qual_op == "=":
        query += SQL(
            _inject_collation(
                "%s BETWEEN (index #>> '{{range,{0},0}}')::"
                + ctype
                + " AND (index #>> '{{range,{0},1}}')::"
                + ctype,
                ctype,
            )
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
    quals: Optional[Quals],
    column_types: Dict[str, str],
    qual_to_clause: Callable = _qual_to_index_clause,
) -> Tuple[Composable, Tuple]:
    if not quals:
        return SQL(""), ()

    def _internal_quals_to_clause(or_quals):
        clauses, args = zip(
            *[qual_to_clause(q, _strip_type_mod(column_types[q[0]])) for q in or_quals]
        )
        return (
            SQL(" OR ").join(SQL("(") + c + SQL(")") for c in clauses),
            tuple([a for arg in args for a in arg]),
        )

    clauses, args = zip(*[_internal_quals_to_clause(q) for q in quals])
    return (
        SQL(" AND ").join(SQL("(") + c + SQL(")") for c in clauses),
        tuple([a for arg in args for a in arg]),
    )


def quals_to_sql(quals: Optional[Quals], column_types: Dict[str, str]) -> Tuple[Composable, Tuple]:
    """
    Convert a list of qualifiers in CNF to a fragment of a Postgres query
    :param quals: Qualifiers in CNF
    :param column_types: Dictionary of column names and their types
    :return: SQL Composable object and a tuple of arguments to be mogrified into it.
    """

    return _quals_to_clause(quals, column_types, qual_to_clause=_qual_to_sql_clause)


def extract_min_max_pks(
    engine: "PsycopgEngine", fragments: List[str], table_pks: List[str], table_pk_types: List[str]
) -> Any:
    """
    Extract minimum/maximum PK values for given fragments.

    :param engine: Engine the objects live on
    :param fragments: IDs of objects
    :param table_pks: List of columns forming the table primary key
    :param table_pk_types: List of types for table PK columns
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
    # it fits both the first and the second chunk. This essentially means that chunks now overlap,
    # so we'll be fetching/scanning through them when it might not be necessary.

    min_max = []
    pk_sql = SQL(",").join(
        Identifier(p) + SQL(_inject_collation("", t)) for p, t in zip(table_pks, table_pk_types)
    )
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
            + SQL(",").join(
                Identifier(p) + SQL(_inject_collation("", t) + " DESC")
                for p, t in zip(table_pks, table_pk_types)
            )
            + SQL(" LIMIT 1"),
            return_shape=ResultShape.ONE_MANY,
        )

        min_max.append((frag_min, frag_max))
    return min_max


def generate_range_index(
    object_engine: "PsycopgEngine",
    object_id: str,
    table_schema: "TableSchema",
    changeset: Optional[Changeset],
    columns: Optional[List[str]] = None,
) -> Dict[str, Tuple[T, T]]:
    """
    Calculate the minimum/maximum values of every column in the object (including deleted values).

    :param object_engine: Engine the object is located on
    :param object_id: ID of the object.
    :param table_schema: Schema of the table
    :param changeset: Changeset (old values will be included in the index)
    :param columns: Columns to run the index on (default all)
    :return: Dictionary of {column: [min, max]}
    """
    columns = columns if columns is not None else [c.name for c in table_schema]

    object_pk = [c.name for c in table_schema if c.is_pk]
    if not object_pk:
        object_pk = [c.name for c in table_schema if c.pg_type in PG_INDEXABLE_TYPES]
    column_types = {c.name: _strip_type_mod(c.pg_type) for c in table_schema}
    columns_to_index = [
        c.name
        for c in table_schema
        if _strip_type_mod(c.pg_type) in PG_INDEXABLE_TYPES and (c.is_pk or c.name in columns)
    ]

    logging.debug("Running range index on columns %s", columns_to_index)
    query = SQL("SELECT ") + SQL(",").join(
        SQL(
            _inject_collation("MIN({0}", column_types[c])
            + "), "
            + _inject_collation("MAX({0}", column_types[c])
            + ")"
        ).format(Identifier(c))
        for c in columns_to_index
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
        index["$pk"] = extract_min_max_pks(
            object_engine, [object_id], object_pk, [column_types[c] for c in object_pk]
        )[0]
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
        for _, old_row, _ in changeset.values():
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
    metadata_engine: "PsycopgEngine",
    object_ids: List[str],
    quals: Any,
    column_types: Dict[str, str],
) -> List[str]:
    clause, args = _quals_to_clause(quals, column_types)
    query = (
        select("get_object_meta", "object_id", table_args="(%s)", schema=SPLITGRAPH_API_SCHEMA)
        + SQL(" WHERE ")
        + clause
    )

    return cast(
        List[str],
        metadata_engine.run_chunked_sql(
            query, [object_ids] + list(args), return_shape=ResultShape.MANY_ONE, chunk_position=0
        ),
    )
