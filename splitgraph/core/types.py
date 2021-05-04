from abc import ABCMeta, abstractmethod
from typing import (
    Dict,
    Tuple,
    Any,
    NamedTuple,
    Optional,
    List,
    Sequence,
    Union,
    TypeVar,
    TYPE_CHECKING,
    NewType,
)

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


class MountError(NamedTuple):
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
) -> Dict[str, Dict[str, Dict[str, str]]]:
    return {
        t: {
            "schema": {c.name: c.pg_type for c in ts},
            "options": {tpk: str(tpv) for tpk, tpv in tp.items()},
        }
        for t, (ts, tp) in tables.items()
    }
