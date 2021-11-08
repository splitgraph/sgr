import json
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from splitgraph.core.output import parse_date, parse_dt, parse_time
from splitgraph.core.types import TableColumn, TableSchema
from splitgraph.ingestion.csv.common import pad_csv_row


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


def parse_json(json_s: str):
    # Avoid false positives for strings like "  123" that json.loads can
    # parse as JSON but PostgreSQL can't (fall back to strings).
    if "{" not in json_s:
        raise ValueError("Not a JSON object")
    json.loads(json_s)


_CONVERTERS: List[Tuple[str, Callable]] = [
    ("timestamp", parse_dt),
    ("date", parse_date),
    ("time", parse_time),
    ("integer", parse_int),
    ("bigint", parse_bigint),
    ("numeric", float),
    ("boolean", parse_boolean),
    ("json", parse_json),
]


def _infer_column_schema(column_sample: Sequence[str]) -> str:
    for candidate, converter in _CONVERTERS:
        try:
            seen_value = False
            for c in column_sample:
                if c == "" or c is None:
                    continue

                seen_value = True
                converter(c)
            # Don't let empty strings or Nones break the parsers but don't accept
            # columns that are just empty strings (they'll be a string).
            if seen_value:
                return candidate
        except (ValueError, TypeError):
            continue

    # No suitable conversion, fall back to varchar
    return "character varying"


def infer_sg_schema(
    sample: Sequence[List[str]],
    override_types: Optional[Dict[str, str]] = None,
    primary_keys: Optional[List[str]] = None,
):
    override_types = override_types or {}
    primary_keys = primary_keys or []
    result: TableSchema = []

    header = sample[0]

    sample = [
        pad_csv_row(row, num_cols=len(sample[0]), row_number=i) for i, row in enumerate(sample[1:])
    ]

    columns = list(zip(*sample))
    if len(columns) != len(header):
        raise ValueError(
            "Malformed CSV: header has %d columns, rows have %d columns"
            % (len(header), len(columns))
        )

    for i, (c_name, c_sample) in enumerate(zip(header, columns)):
        pg_type = override_types.get(c_name, _infer_column_schema(c_sample))

        result.append(
            TableColumn(
                ordinal=i + 1,
                name=c_name,
                pg_type=pg_type,
                is_pk=(c_name in primary_keys),
            )
        )

    return result
