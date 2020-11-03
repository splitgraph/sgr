import json
from abc import ABC, abstractmethod
from random import getrandbits
from typing import Dict, Any, Union, List, Optional, TYPE_CHECKING, cast, Tuple

from psycopg2._json import Json
from psycopg2.sql import SQL, Identifier

from splitgraph.core.engine import repository_exists
from splitgraph.core.image import Image
from splitgraph.core.types import TableSchema, TableColumn
from splitgraph.engine import ResultShape

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine
    from splitgraph.core.repository import Repository

Credentials = Dict[str, Any]
Params = Dict[str, Any]
TableInfo = Union[List[str], Dict[str, TableSchema]]
SyncState = Dict[str, Any]
PreviewResult = Dict[str, Union[str, List[Dict[str, Any]]]]


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

    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_description(cls) -> str:
        raise NotImplementedError

    def __init__(self, engine: "PostgresEngine", credentials: Credentials, params: Params):
        import jsonschema

        self.engine = engine

        jsonschema.validate(instance=credentials, schema=self.credentials_schema)
        jsonschema.validate(instance=params, schema=self.params_schema)

        self.credentials = credentials
        self.params = params

    @abstractmethod
    def introspect(self) -> Dict[str, TableSchema]:
        raise NotImplementedError


class MountableDataSource(DataSource, ABC):
    supports_mount = True

    @abstractmethod
    def mount(
        self, schema: str, tables: Optional[TableInfo] = None, overwrite: bool = True,
    ):
        """Instantiate the data source as foreign tables in a schema"""
        raise NotImplementedError


class LoadableDataSource(DataSource, ABC):
    supports_load = True

    @abstractmethod
    def load(self, schema: str, tables: Optional[TableInfo] = None):
        raise NotImplementedError


class SyncableDataSource(LoadableDataSource, DataSource, ABC):
    supports_load = True
    supports_sync = True

    @abstractmethod
    def _sync(
        self, schema: str, state: Optional[SyncState] = None, tables: Optional[TableInfo] = None
    ) -> Optional[SyncState]:
        """Incremental load"""
        raise NotImplementedError

    def sync(
        self,
        repository: "Repository",
        image_hash: Optional[str],
        tables: Optional[TableInfo] = None,
    ) -> str:
        if not repository_exists(repository):
            repository.init()

        state = get_ingestion_state(repository, image_hash)
        image_hash = image_hash or "0" * 64
        repository.images[image_hash].checkout()

        try:
            new_state = self._sync(schema=repository.to_schema(), state=state, tables=tables)

            if new_state:
                # Write the new state to the table
                if not repository.object_engine.table_exists(
                    repository.to_schema(), INGESTION_STATE_TABLE
                ):
                    repository.object_engine.create_table(
                        repository.to_schema(), INGESTION_STATE_TABLE, INGESTION_STATE_SCHEMA
                    )

                repository.run_sql(
                    SQL("INSERT INTO {} (timestamp, state) VALUES(now(), %s)").format(
                        Identifier(INGESTION_STATE_TABLE)
                    ),
                    (Json(new_state),),
                )

            new_image = repository.commit()
        finally:
            repository.uncheckout()
            repository.commit_engines()

        return new_image.image_hash

    def load(self, schema: str, tables: Optional[TableInfo] = None):
        self._sync(schema, tables=tables, state=None)


def get_ingestion_state(repository: "Repository", image_hash: Optional[str]) -> Optional[SyncState]:
    state = None

    if image_hash:
        image = repository.images[image_hash]
        if INGESTION_STATE_TABLE in image.get_tables():
            with image.query_schema() as s:
                state = repository.object_engine.run_sql(
                    SQL("SELECT state FROM {}.{} ORDER BY timestamp DESC LIMIT 1").format(
                        Identifier(s), Identifier(INGESTION_STATE_TABLE)
                    ),
                    return_shape=ResultShape.ONE_ONE,
                )
    return cast(Optional[SyncState], state)


def prepare_new_image(
    repository: "Repository", hash_or_tag: Optional[str]
) -> Tuple[Optional[Image], str]:
    new_image_hash = "{:064x}".format(getrandbits(256))
    if repository_exists(repository):
        # Clone the base image and delta compress against it
        base_image: Optional[Image] = repository.images[hash_or_tag] if hash_or_tag else None
        repository.images.add(parent_id=None, image=new_image_hash, comment="Singer tap ingestion")
        if base_image:
            repository.engine.run_sql(
                "INSERT INTO splitgraph_meta.tables "
                "(SELECT namespace, repository, %s, table_name, table_schema, object_ids "
                "FROM splitgraph_meta.tables "
                "WHERE namespace = %s AND repository = %s AND image_hash = %s)",
                (
                    new_image_hash,
                    repository.namespace,
                    repository.repository,
                    base_image.image_hash,
                ),
            )
    else:
        base_image = None
        repository.images.add(parent_id=None, image=new_image_hash, comment="Singer tap ingestion")
    return base_image, new_image_hash
