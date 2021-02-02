from typing import Optional, Dict, Any

from splitgraph.core.repository import Repository
from splitgraph.core.types import TableSchema, TableColumn
from splitgraph.engine import ResultShape
from splitgraph.hooks.data_source.base import SyncState, TableInfo, SyncableDataSource

SCHEMA = {
    "test_table": [
        TableColumn(1, "key", "integer", True),
        TableColumn(2, "value", "character varying", False),
    ]
}
TEST_REPO = "test/generic_sync"


class IngestionTestSource(SyncableDataSource):
    params_schema: Dict[str, Any] = {}
    credentials_schema: Dict[str, Any] = {}

    @classmethod
    def get_name(cls) -> str:
        return "Test ingestion"

    @classmethod
    def get_description(cls) -> str:
        return "Test ingestion"

    def introspect(self) -> Dict[str, TableSchema]:
        return SCHEMA

    def _sync(
        self, schema: str, state: Optional[SyncState] = None, tables: Optional[TableInfo] = None
    ) -> SyncState:
        if not self.engine.table_exists(schema, "test_table"):
            self.engine.create_table(schema, "test_table", SCHEMA["test_table"])

        if not state:
            self.engine.run_sql_in(
                schema, "INSERT INTO test_table (key, value) " "VALUES (1, 'one')"
            )
            return {"last_value": 1}
        else:
            last_value = state["last_value"]
            assert last_value == 1
            self.engine.run_sql_in(
                schema, "INSERT INTO test_table (key, value) " "VALUES (2, 'two')"
            )
            return {"last_value": 2}


def _get_state(repo):
    return repo.run_sql(
        "SELECT state FROM _sg_ingestion_state ORDER BY timestamp DESC LIMIT 1",
        return_shape=ResultShape.ONE_ONE,
    )


def test_syncable_data_source(local_engine_empty):
    source = IngestionTestSource(engine=local_engine_empty, credentials={}, params={})

    # Initial sync
    repo = Repository.from_schema(TEST_REPO)
    repo.init()

    image_hash_1 = source.sync(repo, "latest")

    assert len(repo.images()) == 2
    image = repo.images[image_hash_1]
    assert sorted(image.get_tables()) == ["_sg_ingestion_state", "test_table"]
    image.checkout()

    assert repo.run_sql("SELECT * FROM test_table") == [(1, "one")]
    assert _get_state(repo) == {"last_value": 1}

    # Load the data anew into a different image
    repo.images["0" * 64].checkout()
    source._load(repo.to_schema())
    repo.commit_engines()

    assert repo.run_sql("SELECT * FROM test_table") == [(1, "one")]

    repo.uncheckout(force=True)
    # Perform a sync based on the empty image
    image_hash_2 = source.sync(repo, "0" * 64)
    assert image_hash_2 != image_hash_1

    image = repo.images[image_hash_2]
    assert sorted(image.get_tables()) == ["_sg_ingestion_state", "test_table"]
    image.checkout()

    assert repo.run_sql("SELECT * FROM test_table") == [(1, "one")]
    assert _get_state(repo) == {"last_value": 1}

    # Perform a sync based on the ingested image
    image_hash_3 = source.sync(repo, image_hash_1)
    assert image_hash_3 != image_hash_1

    image = repo.images[image_hash_3]
    assert sorted(image.get_tables()) == ["_sg_ingestion_state", "test_table"]
    image.checkout()

    assert repo.run_sql("SELECT * FROM test_table ORDER BY key ASC") == [(1, "one"), (2, "two")]
    assert _get_state(repo) == {"last_value": 2}
