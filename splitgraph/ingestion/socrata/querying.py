from typing import Any, Dict, List, Optional, Tuple

from splitgraph.core.types import TableColumn, TableSchema
from splitgraph.ingestion.common import dedupe_sg_schema

try:
    from multicorn import ANY
except ImportError:
    # Don't fail the import if we're running this directly.
    ANY = object()


SUPPORTED_OPERATORS = ["=", ">", ">=", "<", "<=", "<>", "!=", "~~"]

SUPPORTED_AGG_FUNCS = {
    "avg": "avg",
    "min": "min",
    "max": "max",
    "sum": "sum",
    "count": "count",
    "count.*": "count",
}


_socrata_types = {
    "checkbox": "boolean",
    "double": "double",
    "floating timestamp": "timestamp without time zone",
    "calendar date": "date",
    "money": "money",
    "number": "numeric",
    "text": "text",
    "url": "text",
}


def _socrata_to_pg_type(socrata_type):
    socrata_type = socrata_type.lower()
    if socrata_type in [
        "line",
        "location",
        "multiline",
        "multipoint",
        "multipolygon",
        "point",
    ]:
        return "json"
    if socrata_type in _socrata_types:
        return _socrata_types[socrata_type]
    else:
        return "text"


def socrata_to_sg_schema(metadata: Dict[str, Any]) -> Tuple[TableSchema, Dict[str, str]]:
    try:
        col_names = metadata["resource"]["columns_field_name"]
        col_types = metadata["resource"]["columns_datatype"]
    except KeyError:
        raise ValueError("Invalid Socrata metadata!")

    col_desc = metadata["resource"].get("columns_description") or [None] * len(col_names)

    # Prepend the Socrata :id column that we can order on and use as PK.
    col_names = [":id"] + col_names
    col_types = ["text"] + col_types
    col_desc = ["Socrata column ID"] + col_desc

    result = [
        TableColumn(i, n, _socrata_to_pg_type(t), False, d)
        for i, (n, t, d) in enumerate(zip(col_names, col_types, col_desc))
    ]

    # Truncate Socrata column names to 63 characters and calculate
    # a map of Splitgraph columns to Socrata columns.
    result_deduped = dedupe_sg_schema(result)

    sg_to_socrata_cols = {
        d.name: r.name for r, d in zip(result, result_deduped) if d.name != r.name
    }

    return result_deduped, sg_to_socrata_cols


def estimate_socrata_rows_width(columns, metadata, column_map=None):
    """Estimate number of rows required for a query and each row's width
    from the table metadata."""
    column_map = column_map or {}

    # We currently don't use qualifiers for this, they get passed to Socrata
    # directly. Socrata's metadata does contain some statistics for each column
    # (min, max, sum, most popular values etc) that can be used to help the planner though.

    contents = metadata["columns"][0]["cachedContents"]
    cardinality = int(contents["non_null"]) + int(contents["null"])
    column_widths = {c["fieldName"]: c.get("width", 100) for c in metadata["columns"]}

    # Socrata doesn't return the Socrata Row ID width in its metadata but we know
    # how big it usually is (row-XXXX.XXXX_XXXX).
    row_width = sum(column_widths[column_map.get(c, c)] if c != ":id" else 18 for c in columns)

    return cardinality, row_width


def _emit_col(col: str, column_map: Optional[Dict[str, str]] = None) -> str:
    column_map = column_map or {}
    return f"`{column_map.get(col, col)}`"


def _emit_val(val: object) -> str:
    if val is None:
        return "NULL"
    if not isinstance(val, (int, float)):
        val = str(val).replace("'", "''")
        return f"'{val}'"
    return str(val)


def _convert_op(op: str) -> Optional[str]:
    if op in ["=", ">", ">=", "<", "<=", "<>", "!="]:
        return op
    if op == "~~":
        return "LIKE"
    return None


def _base_qual_to_socrata(
    col: str, op: str, value, column_map: Optional[Dict[str, str]] = None
) -> str:
    soql_op = _convert_op(op)
    if not soql_op:
        return "TRUE"
    if value is None:
        if soql_op == "=":
            soql_op = "IS"
        elif soql_op in ("<>", "!="):
            soql_op = "IS NOT"
        else:
            return "TRUE"

    return f"{_emit_col(col, column_map)} {soql_op} {_emit_val(value)}"


def _qual_to_socrata(qual: Any, column_map: Optional[Dict[str, str]] = None) -> str:
    if qual.is_list_operator:
        if qual.list_any_or_all == ANY:
            # Convert col op ANY([a,b,c]) into (cop op a) OR (col op b)...
            return " OR ".join(
                f"({_base_qual_to_socrata(qual.field_name, qual.operator[0], v, column_map)})"
                for v in qual.value
            )
        # Convert col op ALL(ARRAY[a,b,c...]) into (cop op a) AND (col op b)...
        return " AND ".join(
            f"({_base_qual_to_socrata(qual.field_name, qual.operator[0], v, column_map)})"
            for v in qual.value
        )
    else:
        return f"{_base_qual_to_socrata(qual.field_name, qual.operator, qual.value, column_map)}"


def quals_to_socrata(quals: List[Any], column_map: Optional[Dict[str, str]] = None) -> str:
    """Convert a list of Multicorn quals to a SoQL query"""
    return " AND ".join(f"({_qual_to_socrata(q, column_map)})" for q in quals)


def group_to_socrata(
    group_clauses: Optional[List[str]] = None,
    column_map: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    if not group_clauses:
        return None

    return ",".join(_emit_col(c, column_map) for c in group_clauses)


def cols_to_socrata(
    cols: List[str],
    group_clauses: Optional[List[str]] = None,
    aggs: Optional[Dict[str, Dict[str, str]]] = None,
    column_map: Optional[Dict[str, str]] = None,
) -> Tuple[str, Dict[str, str]]:

    # Map from (Socrata agg output -> PostgreSQL agg name)
    agg_output_map: Dict[str, str] = {}

    if group_clauses or aggs:
        target_list: List[str] = []
        if group_clauses:
            target_list.extend(_emit_col(c, column_map) for c in group_clauses)
        if aggs:
            for agg_name, agg_props in aggs.items():
                agg_func = SUPPORTED_AGG_FUNCS[agg_props["function"]]
                emitted_col = (
                    _emit_col(agg_props["column"], column_map)
                    if agg_props["column"] != "*"
                    else "*"
                )

                # Give the agg result a custom alias because Socrata doesn't support
                # aliases with dots / stars (even if they're quoted).
                # This means we return a map to help the caller figure out which Socrata
                # result columns correspond to which results
                agg_alias = (
                    f"{agg_func}_{agg_props['column']}"
                    if agg_func != "count" and agg_props["column"] != "*"
                    else "count"
                )
                agg_target = f"{agg_func}({emitted_col}) AS `{agg_alias}`"
                agg_output_map[agg_alias] = agg_name
                target_list.append(agg_target)
    else:
        # Don't add ":id" to the request since we're getting it anyway
        # through exclude_system_fields=false
        target_list = [_emit_col(c, column_map) for c in cols if c != ":id"]

    return ",".join(target_list), agg_output_map


def sortkeys_to_socrata(
    sortkeys: List[Any],
    group_clauses: Optional[List[str]] = None,
    aggs: Optional[Dict[str, Dict[str, str]]] = None,
    column_map: Optional[Dict[str, str]] = None,
) -> str:
    if not sortkeys:
        if not group_clauses and not aggs:
            # Always sort on ID for stable paging
            return ":id"
        # In an aggregation, we don't have access to the ":id" column (we're
        # not aggregating on it)
        return ""

    clauses = []
    for key in sortkeys:
        if key.nulls_first != key.is_reversed:
            raise ValueError("Unsupported SortKey %s" % str(key))
        order = "DESC" if key.is_reversed else "ASC"
        clauses.append(f"{_emit_col(key.attname, column_map)} {order}")
    return ",".join(clauses)
