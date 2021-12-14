from abc import ABC, abstractmethod
from contextlib import contextmanager
from importlib.resources import read_binary
from random import getrandbits
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple, cast

from psycopg2._json import Json
from psycopg2.sql import SQL, Identifier
from splitgraph.config import DEFAULT_CHUNK_SIZE
from splitgraph.core.engine import repository_exists
from splitgraph.core.image import Image
from splitgraph.core.image_mounting import DefaultImageMounter, ImageMounter
from splitgraph.core.repository import Repository
from splitgraph.core.types import (
    Credentials,
    IntrospectionResult,
    MountError,
    Params,
    SyncState,
    TableColumn,
    TableInfo,
)
from splitgraph.engine import ResultShape
from splitgraph.ingestion.common import add_timestamp_tags
from splitgraph.resources import icons

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PostgresEngine

INGESTION_STATE_TABLE = "_sg_ingestion_state"
INGESTION_STATE_SCHEMA = [
    TableColumn(1, "timestamp", "timestamp", True, None),
    TableColumn(2, "state", "json", False, None),
]


class DataSource(ABC):
    params_schema: Dict[str, Any] = {"type": "object"}
    credentials_schema: Dict[str, Any] = {"type": "object"}
    table_params_schema: Dict[str, Any] = {"type": "object"}

    supports_mount = False
    supports_sync = False
    supports_load = False
    _icon_file: Optional[str] = None

    @classmethod
    def get_icon(cls) -> Optional[bytes]:
        if cls._icon_file:
            return read_binary(icons, cls._icon_file)
        return None

    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get_description(cls) -> str:
        raise NotImplementedError

    def __init__(
        self,
        engine: "PostgresEngine",
        credentials: Credentials,
        params: Params,
        tables: Optional[TableInfo] = None,
    ):
        import jsonschema

        self.engine = engine

        if "tables" in params:
            tables = params.pop("tables")

        jsonschema.validate(instance=credentials, schema=self.credentials_schema)
        jsonschema.validate(instance=params, schema=self.params_schema)

        self.credentials = credentials
        self.params = params

        self._validate_table_params(tables)
        self.tables = tables

    @classmethod
    def _validate_table_params(cls, tables: Optional[TableInfo]) -> None:
        import jsonschema

        if isinstance(tables, dict):
            for _, table_params in tables.values():
                jsonschema.validate(instance=table_params, schema=cls.table_params_schema)

    @abstractmethod
    def introspect(self) -> IntrospectionResult:
        raise NotImplementedError

    def get_raw_url(
        self, tables: Optional[TableInfo] = None, expiry: int = 3600
    ) -> Dict[str, List[Tuple[str, str]]]:
        """
        Get a list of public URLs for each table in this data source, e.g. to export the data
        as CSV. These may be temporary (e.g. pre-signed S3 URLs) but should be accessible without
        authentication.
        :param tables: A TableInfo object overriding the table params of the source
        :param expiry: The URL should be valid for at least this many seconds
        :return: Dict of table_name -> list of (mimetype, raw URL)
        """
        return {}


class MountableDataSource(DataSource, ABC):
    supports_mount = True

    @abstractmethod
    def mount(
        self,
        schema: str,
        tables: Optional[TableInfo] = None,
        overwrite: bool = True,
    ) -> Optional[List[MountError]]:
        """Instantiate the data source as foreign tables in a schema"""
        raise NotImplementedError


class LoadableDataSource(DataSource, ABC):
    supports_load = True

    @abstractmethod
    def _load(self, schema: str, tables: Optional[TableInfo] = None):
        raise NotImplementedError

    def load(self, repository: "Repository", tables: Optional[TableInfo] = None) -> str:
        self._validate_table_params(tables)
        if not repository_exists(repository):
            repository.init()

        image_hash = "{:064x}".format(getrandbits(256))
        tmp_schema = "{:064x}".format(getrandbits(256))
        repository.images.add(
            parent_id=None,
            image=image_hash,
        )
        repository.object_engine.create_schema(tmp_schema)

        try:
            self._load(schema=tmp_schema, tables=tables)

            repository._commit(
                head=None,
                image_hash=image_hash,
                snap_only=True,
                chunk_size=DEFAULT_CHUNK_SIZE,
                schema=tmp_schema,
            )
            add_timestamp_tags(repository, image_hash)
        finally:
            repository.object_engine.delete_schema(tmp_schema)
            repository.commit_engines()

        return image_hash


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
        self._validate_table_params(tables)
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

    def _load(self, schema: str, tables: Optional[TableInfo] = None):
        self._sync(schema, tables=tables, state=None)


class TransformingDataSource(DataSource, ABC):
    """
    Data source that runs transformations between Splitgraph images. Takes in an extra
    parameter, an ImageMounter instance to manage temporary image checkouts.
    """

    def __init__(
        self,
        engine: "PostgresEngine",
        credentials: Credentials,
        params: Params,
        tables: Optional[TableInfo] = None,
        image_mounter: Optional[ImageMounter] = None,
    ):
        super().__init__(engine, credentials, params, tables)
        self._mounter = image_mounter or DefaultImageMounter(engine)

    @abstractmethod
    def get_required_images(self) -> List[Tuple[str, str, str]]:
        """
        Get images required by this data source.
        :returns List of tuples (namespace, repository, hash_or_tag)
        """
        raise NotImplementedError

    @contextmanager
    def mount_required_images(self) -> Generator[Dict[Tuple[str, str, str], str], None, None]:
        """
        Mount all images required by this data source into temporary schemas. On exit from this
        context manager, unmounts them.
        :return: Map of (namespace, repository, hash_or_tag) -> schema where the image is mounted.
        """
        try:
            schema_map = self._mounter.mount(self.get_required_images())
            yield schema_map
        finally:
            self._mounter.unmount()


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
    repository: "Repository", hash_or_tag: Optional[str], comment: str = "Singer tap ingestion"
) -> Tuple[Optional[Image], str]:
    new_image_hash = "{:064x}".format(getrandbits(256))
    if repository_exists(repository):
        # Clone the base image and delta compress against it
        base_image: Optional[Image] = repository.images[hash_or_tag] if hash_or_tag else None
        repository.images.add(parent_id=None, image=new_image_hash, comment=comment)
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
        repository.images.add(parent_id=None, image=new_image_hash, comment=comment)
    return base_image, new_image_hash
