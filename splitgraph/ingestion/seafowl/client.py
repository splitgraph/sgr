import json
from datetime import datetime
from numbers import Number
from typing import Any, Dict, List, Optional, Tuple

import requests


def emit_value(value: Any) -> str:
    if value is None:
        return "NULL"

    if isinstance(value, float):
        return f"{value:.20f}"

    if isinstance(value, Number) and not isinstance(value, bool):
        return str(value)

    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"

    quoted = str(value).replace("'", "''")
    return f"'{quoted}'"


def get_pg_type(df_type: str) -> str:
    if "Utf" in df_type:
        return "TEXT"

    if "Int64" in df_type:
        return "BIGINT"

    if "Int" in df_type:
        return "INT"

    if "Float" in df_type:
        return "DOUBLE"

    if "Boolean" in df_type:
        return "BOOLEAN"

    if "Date" in df_type:
        return "TIMESTAMP"

    return "TEXT"


def quote_ident(ident: str) -> str:
    return '"%s"' % ident.replace('"', '""')


class SeafowlClient:
    def __init__(self, url: str) -> None:
        self.url = url

    @classmethod
    def _prepare_query(cls, query: str, args: Optional[Tuple[Any]] = None) -> str:
        args = args or ()
        for i, arg in enumerate(args):
            if f"${i+1}" not in query:
                raise ValueError(f"Argument ${i+1} not found!")
            query = query.replace(f"${i+1}", emit_value(arg))

        return query

    def sql(self, query: str, args: Optional[Tuple[Any]] = None) -> List[Dict[str, Any]]:
        query = self._prepare_query(query, args)

        response = requests.post(f"{self.url}/q", json={"query": query})
        response.raise_for_status()

        result = response.text.strip()
        if not result:
            return []
        return [json.loads(r) for r in result.splitlines(keepends=False)]
