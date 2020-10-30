from abc import ABC, abstractmethod
from typing import Dict, Any, Union, List, Optional, TYPE_CHECKING, cast

from splitgraph.core.repository import Repository
from splitgraph.core.types import TableSchema
if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine
    from splitgraph.core.repository import Repository

Credentials = Dict[str, Any]
Params = Dict[str, Any]
TableInfo = Union[List[str], Dict[str, TableSchema]]
SyncState = Dict[str, Any]


class DataSource(ABC):
    params_schema: Dict[str, Any]
    credentials_schema: Dict[str, Any]

    supports_mount = False
    supports_sync = False
    supports_load = False

    def __init__(self, engine: "PostgresEngine", credentials: Credentials, params: Params):
        import jsonschema

        self.engine = engine

        jsonschema.validate(instance=credentials, schema=self.credentials_schema)
        jsonschema.validate(instance=params, schema=self.params_schema)

        self.credentials = credentials
        self.params = params

    @abstractmethod
    def introspect(self) -> Dict[str, TableSchema]:
        pass

    def mount(
        self,
        schema: str,
        tables: Optional[TableInfo] = None,
        overwrite: bool = True,
    ):
        """Instantiate the data source as foreign tables in a schema"""
        raise NotImplemented

    def _sync(
        self, schema: str, state: Optional[SyncState] = None, tables: Optional[TableInfo] = None
    ) -> SyncState:
        """Incremental load"""
        raise NotImplemented

    def sync(self, repository: Repository, image_hash: Optional[str]):
        # load "state" from the image
        # checkout the repo
        # pass state to sync
        # commit the repo
        # override with singer
        pass

    def load(self, schema: str, tables: Optional[TableInfo] = None):
        if self.supports_sync:
            self._sync(schema, tables=tables)

        raise NotImplemented