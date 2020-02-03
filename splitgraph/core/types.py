from typing import Dict, Tuple, Any, NamedTuple, Optional, List, Sequence, Union

Changeset = Dict[Tuple[str, ...], Tuple[bool, Dict[str, Any]]]


class TableColumn(NamedTuple):
    ordinal: int
    name: str
    pg_type: str
    is_pk: bool
    comment: Optional[str] = None


TableSchema = List[TableColumn]
Quals = Sequence[Sequence[Tuple[str, str, Any]]]


ProvenanceData = Dict[str, Union[str, List[str], List[bool]]]
