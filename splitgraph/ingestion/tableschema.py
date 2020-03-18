"""Contains functions related to inputting/outputting data according to the TableSchema
 (http://specs.frictionlessdata.io/table-schema/) standard."""

# Currently this is not fully implemented, we only use tableschema for csv
# ingestion and inference.
from typing import Dict, Any, Optional, List

from splitgraph.core.types import TableSchema, TableColumn

# https://specs.frictionlessdata.io/table-schema/#types-and-formats

_TS_TYPE_MAP: Dict[str, str] = {
    "string": "character varying",
    "number": "numeric",
    "integer": "integer",
    "boolean": "boolean",
    "object": "json",
    "array": "json",
    "date": "date",
    "time": "time",
    "datetime": "timestamp",
}


def _ts_to_pg_type(ts_type: str) -> str:
    return _TS_TYPE_MAP.get(ts_type, "character varying")


def tableschema_to_sg(
    ts_schema: Dict[str, Any],
    override_types: Optional[Dict[str, Any]],
    primary_keys: Optional[List[str]] = None,
) -> TableSchema:
    """
    Convert a tableschema infer result to SG-internal table schema format
    """
    override_types = override_types or {}
    primary_keys = primary_keys or []
    result: TableSchema = []

    for i, field in enumerate(ts_schema["fields"]):
        pg_type = override_types.get(field["name"], _ts_to_pg_type(field["type"]))

        result.append(
            TableColumn(
                ordinal=i + 1,
                name=field["name"],
                pg_type=pg_type,
                is_pk=(field["name"] in primary_keys),
                comment=field.get("description"),
            )
        )

    return result
