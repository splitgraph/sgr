import json
from typing import Dict, Any, Optional, List, Tuple, Sequence, Callable

from splitgraph.core.output import parse_dt, parse_date, parse_time
from splitgraph.core.types import TableSchema, TableColumn


def parse_boolean(boolean: str):
    if boolean.lower() in ["t", "true"]:
        return True
    elif boolean.lower() in ["f", "false"]:
        return False
    raise ValueError("Invalid boolean!")


def parse_int(integer: str):
    result = int(integer)
    if result > 2147483647 or result < -2147483648:
        raise ValueError("Integer value %s out of range!" % integer)
    return result


def parse_bigint(integer: str):
    result = int(integer)
    if result > 9223372036854775807 or result < -9223372036854775808:
        raise ValueError("Bigint value %s out of range!" % integer)
    return result


_CONVERTERS: List[Tuple[str, Callable]] = [
    ("timestamp", parse_dt),
    ("date", parse_date),
    ("time", parse_time),
    ("integer", parse_int),
    ("bigint", parse_bigint),
    ("numeric", float),
    ("boolean", parse_boolean),
    ("json", json.loads),
]


def _infer_column_schema(column_sample: Sequence[str]) -> str:
    for candidate, converter in _CONVERTERS:
        try:
            seen_value = False
            for c in column_sample:
                if c == "":
                    continue

                seen_value = True
                converter(c)
            # Don't let empty strings or Nones break the parsers but don't accept
            # columns that are just empty strings (they'll be a string).
            if seen_value:
                return candidate
        except ValueError:
            continue

    # No suitable conversion, fall back to varchar
    return "character varying"


def infer_sg_schema(
    sample: List[Tuple[str, ...]],
    override_types: Optional[Dict[str, Any]],
    primary_keys: Optional[List[str]] = None,
):
    override_types = override_types or {}
    primary_keys = primary_keys or []
    result: TableSchema = []

    header = sample[0]
    columns = list(zip(*sample[1:]))
    if len(columns) != len(header):
        raise ValueError("Malformed CSV!")

    for i, (c_name, c_sample) in enumerate(zip(header, columns)):
        pg_type = override_types.get(c_name, _infer_column_schema(c_sample))

        result.append(
            TableColumn(
                ordinal=i + 1, name=c_name, pg_type=pg_type, is_pk=(c_name in primary_keys),
            )
        )

    return result
