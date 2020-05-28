from typing import Dict, Any

from splitgraph.core.types import TableSchema, TableColumn

try:
    from multicorn import ANY
except ImportError:
    # Don't fail the import if we're running this directly.
    ANY = object()


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


def socrata_to_sg_schema(metadata: Dict[str, Any]) -> TableSchema:
    try:
        col_names = metadata["resource"]["columns_field_name"]
        col_types = metadata["resource"]["columns_datatype"]
    except KeyError:
        raise ValueError("Invalid Socrata metadata!")

    col_desc = metadata["resource"].get("columns_description") or [None] * len(col_names)

    return [
        TableColumn(i, n, _socrata_to_pg_type(t), False, d)
        for i, (n, t, d) in enumerate(zip(col_names, col_types, col_desc))
    ]


def estimate_socrata_rows_width(columns, metadata):
    """Estimate number of rows required for a query and each row's width
    from the table metadata."""

    # We currently don't use qualifiers for this, they get passed to Socrata
    # directly. Socrata's metadata does contain some statistics for each column
    # (min, max, sum, most popular values etc) that can be used to help the planner though.

    contents = metadata["columns"][0]["cachedContents"]
    cardinality = int(contents["non_null"]) + int(contents["null"])
    column_widths = {c["fieldName"]: c["width"] for c in metadata["columns"]}

    row_width = sum(column_widths[c] for c in columns)

    return cardinality, row_width


def _emit_col(col):
    return col


def _emit_val(val):
    if val is None:
        return "NULL"
    if isinstance(val, str):
        val = val.replace("'", "''")
        return f"'{val}'"
    return str(val)


def _convert_op(op):
    if op in ["=", ">", ">=", "<", "<=", "<>", "!="]:
        return op
    if op == "~~":
        return "LIKE"
    return None


def _base_qual_to_socrata(col, op, value):
    soql_op = _convert_op(op)
    if not soql_op:
        return "TRUE"
    else:
        return f"{_emit_col(col)} {soql_op} {_emit_val(value)}"


def _qual_to_socrata(qual):
    if qual.is_list_operator:
        if qual.list_any_or_all == ANY:
            # Convert col op ANY([a,b,c])
            if qual.operator[0] == "=":
                return f"{_emit_col(qual.field_name)} IN (" + ",".join(
                    f"{_emit_val(v)}" for v in qual.value
                )
            else:
                return "TRUE"
        else:
            # Convert col op ALL(ARRAY[a,b,c...]) into (cop op a) AND (col op b)...
            return " AND ".join(
                f"({_base_qual_to_socrata(qual.field_name, qual.operator[0], v)})"
                for v in qual.value
            )
    else:
        return f"{_base_qual_to_socrata(qual.field_name, qual.operator[0], qual.value)}"


def quals_to_socrata(quals):
    """Convert a list of Multicorn quals to a SoQL query"""
    return " AND ".join(f"({_qual_to_socrata(q)})" for q in quals)


def cols_to_socrata(cols):
    return ",".join(f"{_emit_col(c)}" for c in cols)
