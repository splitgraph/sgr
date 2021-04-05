from abc import ABCMeta, abstractmethod
from typing import Dict, Tuple, Any, NamedTuple, Optional, List, Sequence, Union

Changeset = Dict[Tuple[str, ...], Tuple[bool, Dict[str, Any], Dict[str, Any]]]


class TableColumn(NamedTuple):
    ordinal: int
    name: str
    pg_type: str
    is_pk: bool
    comment: Optional[str] = None


TableSchema = List[TableColumn]
Quals = Sequence[Sequence[Tuple[str, str, Any]]]

SourcesList = List[Dict[str, str]]
ProvenanceLine = Dict[str, Union[str, List[str], List[bool], SourcesList]]

# Ingestion-related params
Credentials = Dict[str, Any]
Params = Dict[str, Any]
TableParams = Dict[str, Any]
TableInfo = Union[List[str], Dict[str, Tuple[TableSchema, TableParams]]]
SyncState = Dict[str, Any]
PreviewResult = Dict[str, Union[str, List[Dict[str, Any]]]]


class Comparable(metaclass=ABCMeta):
    @abstractmethod
    def __lt__(self, other: Any) -> bool:
        ...


def dict_to_table_schema_params(
    tables: Dict[str, Dict[str, Any]]
) -> Dict[str, Tuple[TableSchema, TableParams]]:
    return {
        t: (
            [
                TableColumn(i + 1, cname, ctype, False, None)
                for (i, (cname, ctype)) in enumerate(tsp["schema"].items())
            ],
            tsp.get("options", {}),
        )
        for t, tsp in tables.items()
    }


def table_schema_params_to_dict(
    tables: Dict[str, Tuple[TableSchema, TableParams]]
) -> Dict[str, Dict[str, Dict[str, str]]]:
    return {
        t: {
            "schema": {c.name: c.pg_type for c in ts},
            "options": {tpk: str(tpv) for tpk, tpv in tp.items()},
        }
        for t, (ts, tp) in tables.items()
    }
