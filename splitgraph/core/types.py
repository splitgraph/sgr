from abc import ABCMeta, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    NamedTuple,
    NewType,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from pydantic import BaseModel

if TYPE_CHECKING:
    from splitgraph.cloud.models import ExternalTableRequest

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
Credentials = NewType("Credentials", Dict[str, Any])
Params = NewType("Params", Dict[str, Any])
TableParams = NewType("TableParams", Dict[str, Any])
TableInfo = Union[List[str], Dict[str, Tuple[TableSchema, TableParams]]]
SyncState = NewType("SyncState", Dict[str, Any])


class MountError(BaseModel):
    table_name: str
    error: str
    error_text: str


PreviewResult = NewType("PreviewResult", Dict[str, Union[MountError, List[Dict[str, Any]]]])
IntrospectionResult = NewType(
    "IntrospectionResult", Dict[str, Union[Tuple[TableSchema, TableParams], MountError]]
)

T = TypeVar("T")


def unwrap(
    result: Dict[str, Union[MountError, T]],
) -> Tuple[Dict[str, T], Dict[str, MountError]]:
    good = {}
    bad = {}
    for k, v in result.items():
        if isinstance(v, MountError):
            bad[k] = v
        else:
            good[k] = v
    return good, bad


def get_table_params(table_info: TableInfo, table_name: str) -> TableParams:
    if isinstance(table_info, dict) and table_name in table_info:
        return table_info[table_name][1]
    return TableParams({})


def get_table_list(table_info: TableInfo) -> List[str]:
    if isinstance(table_info, list):
        return table_info
    return list(table_info.keys())


class Comparable(metaclass=ABCMeta):
    @abstractmethod
    def __lt__(self, other: Any) -> bool:
        ...


def dict_to_table_schema_params(
    tables: Dict[str, "ExternalTableRequest"]
) -> Dict[str, Tuple[TableSchema, TableParams]]:
    return {
        t: (
            [
                TableColumn(i + 1, cname, ctype, False, None)
                for (i, (cname, ctype)) in enumerate((tsp.schema_ or {}).items())
            ],
            TableParams(tsp.options),
        )
        for t, tsp in tables.items()
    }


def table_schema_params_to_dict(
    tables: Dict[str, Tuple[TableSchema, TableParams]]
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    return {
        t: {
            "schema": {c.name: c.pg_type for c in ts},
            "options": tp,
        }
        for t, (ts, tp) in tables.items()
    }
