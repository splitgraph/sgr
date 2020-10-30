from abc import ABC, abstractmethod
from typing import Dict, Any, Union, List, Optional, TYPE_CHECKING, cast

from psycopg2._json import Json
from psycopg2.sql import SQL, Identifier

from splitgraph.core.engine import repository_exists
from splitgraph.core.sql import insert
from splitgraph.core.types import TableSchema, TableColumn
from splitgraph.engine import ResultShape
from splitgraph.ingestion.singer import prepare_new_image

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine
    from splitgraph.core.repository import Repository

Credentials = Dict[str, Any]
Params = Dict[str, Any]
TableInfo = Union[List[str], Dict[str, TableSchema]]
SyncState = Dict[str, Any]


INGESTION_STATE_TABLE = "_sg_ingestion_state"
INGESTION_STATE_SCHEMA = [
    TableColumn(1, "timestamp", "timestamp", True, None),
    TableColumn(2, "state", "json", False, None),
]


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
        self, schema: str, tables: Optional[TableInfo] = None, overwrite: bool = True,
    ):
        """Instantiate the data source as foreign tables in a schema"""
        raise NotImplemented

    def _sync(
        self, schema: str, state: Optional[SyncState] = None, tables: Optional[TableInfo] = None
    ) -> SyncState:
        """Incremental load"""
        raise NotImplemented

    def sync(
        self,
        repository: "Repository",
        image_hash: Optional[str],
        tables: Optional[TableInfo] = None,
    ):
        if not repository_exists(repository):
            repository.init()
            image_hash = repository.images["latest"].image_hash

        state = get_ingestion_state(repository, image_hash)
        base_image, new_image_hash = prepare_new_image(repository, image_hash)
        repository.images[new_image_hash].checkout()

        try:
            new_state = self._sync(schema=repository.to_schema(), state=state, tables=tables)

            # Write the new state to the table
            if not repository.object_engine.table_exists(
                repository.to_schema(), INGESTION_STATE_TABLE
            ):
                repository.object_engine.create_table(
                    repository.to_schema(), INGESTION_STATE_TABLE, INGESTION_STATE_SCHEMA
                )

            repository.object_engine.run_sql(
                SQL("INSERT INTO pg_temp.{} (timestamp, state) VALUES(now(), %s)").format(
                    Identifier(INGESTION_STATE_TABLE)
                ),
                (Json(new_state),),
            )

            repository.commit()
        finally:
            repository.uncheckout()
            repository.commit_engines()

    def load(self, schema: str, tables: Optional[TableInfo] = None):
        if self.supports_sync:
            self._sync(schema, tables=tables)

        raise NotImplemented


def get_ingestion_state(repository, image_hash) -> Optional[SyncState]:
    state = None

    if image_hash:
        image = repository.images[image_hash]
        if INGESTION_STATE_TABLE in image.get_tables():
            with image.query_schema() as s:
                state = repository.object_engine.run_sql(
                    SQL("SELECT state FROM {}.{} LIMIT 1").format(
                        Identifier(s), Identifier(INGESTION_STATE_TABLE)
                    ),
                    return_shape=ResultShape.ONE_ONE,
                )
    return cast(SyncState, state)
