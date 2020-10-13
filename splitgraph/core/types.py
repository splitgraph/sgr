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


class Comparable(metaclass=ABCMeta):
    @abstractmethod
    def __lt__(self, other: Any) -> bool:
        ...


def dict_to_tableschema(tables: Dict[str, Dict[str, Any]]) -> Dict[str, TableSchema]:
    return {
        t: [
            TableColumn(i, cname, ctype, False, None)
            for (i, (cname, ctype)) in enumerate(ts.items())
        ]
        for t, ts in tables.items()
    }


def tableschema_to_dict(tables: Dict[str, TableSchema]) -> Dict[str, Dict[str, str]]:
    return {t: {c.name: c.pg_type for c in ts} for t, ts in tables.items()}
