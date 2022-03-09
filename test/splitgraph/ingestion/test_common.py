from typing import Any, Dict, Optional, Tuple, cast

from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core.repository import Repository
from splitgraph.core.types import (
    IntrospectionResult,
    SyncState,
    TableColumn,
    TableInfo,
    TableParams,
    TableSchema,
)
from splitgraph.engine import ResultShape
from splitgraph.hooks.data_source import PostgreSQLDataSource
from splitgraph.hooks.data_source.base import SyncableDataSource

SCHEMA: Dict[str, Tuple[TableSchema, TableParams]] = {
    "test_table": (
        [
            TableColumn(1, "key", "integer", True),
            TableColumn(2, "value", "character varying", False),
        ],
        {},
    )
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

    def introspect(self) -> IntrospectionResult:
        return cast(IntrospectionResult, SCHEMA)

    def _sync(
        self, schema: str, state: Optional[SyncState] = None, tables: Optional[TableInfo] = None
    ) -> SyncState:
        if not self.engine.table_exists(schema, "test_table"):
            self.engine.create_table(schema, "test_table", SCHEMA["test_table"][0])

        if not state:
            self.engine.run_sql_in(schema, "INSERT INTO test_table (key, value) VALUES (1, 'one')")
            return {"last_value": 1}
        else:
            last_value = state["last_value"]
            assert last_value == 1
            self.engine.run_sql_in(schema, "INSERT INTO test_table (key, value) VALUES (2, 'two')")
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


def test_fdw_data_source_with_cursors(pg_repo_local):

    tables = {
        "fruits": (
            [
                TableColumn(
                    ordinal=1, name="fruit_id", pg_type="integer", is_pk=False, comment=None
                ),
                TableColumn(
                    ordinal=2,
                    name="name",
                    pg_type="character varying",
                    is_pk=False,
                    comment=None,
                ),
            ],
            {"table_name": "fruits", "cursor_fields": ["fruit_id"]},
        )
    }

    # Set up a data source pointing to the same engine so that we can test incremental loads.
    engine = pg_repo_local.object_engine
    handler = PostgreSQLDataSource(
        engine=engine,
        credentials={
            "username": engine.conn_params["SG_ENGINE_USER"],
            "password": engine.conn_params["SG_ENGINE_PWD"],
        },
        params={
            "host": engine.conn_params["SG_ENGINE_HOST"],
            "port": int(engine.conn_params["SG_ENGINE_PORT"]),
            "dbname": engine.conn_params["SG_ENGINE_DB_NAME"],
            "remote_schema": pg_repo_local.to_schema(),
        },
        tables=tables,
    )

    output = Repository("test", "fdw_sync")
    image_hash_1 = handler.load(output)

    assert len(output.images()) == 1
    image = output.images[image_hash_1]
    assert sorted(image.get_tables()) == ["_sg_ingestion_state", "fruits"]
    image.checkout()

    assert output.run_sql("SELECT COUNT(*) FROM fruits") == [(2,)]
    assert _get_state(output) == {
        "cursor_values": {
            "fruits": {
                "fruit_id": "2",
            },
        }
    }

    # Add a row to the table and sync again
    pg_repo_local.run_sql("INSERT INTO fruits (name) VALUES ('banana')")
    pg_repo_local.commit_engines()

    image_hash_2 = handler.sync(output, image_hash=image_hash_1)

    assert len(output.images()) == 2
    image = output.images[image_hash_2]
    image.checkout()

    assert output.run_sql("SELECT COUNT(*) FROM fruits") == [(3,)]
    assert _get_state(output) == {
        "cursor_values": {
            "fruits": {
                "fruit_id": "3",
            },
        }
    }

    # Check that we made an object with a single new row instead of overwriting the table
    table = image.get_table("fruits")
    assert len(table.objects) == 2
    assert engine.run_sql(
        SQL("SELECT * FROM {}.{}").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table.objects[1])
        )
    ) == [(3, "banana", True)]

    # Add a row to the existing table, add another table, sync again
    pg_repo_local.run_sql("INSERT INTO fruits (name) VALUES ('kumquat')")
    pg_repo_local.commit_engines()

    tables["vegetables"] = (
        [
            TableColumn(
                ordinal=1, name="vegetable_id", pg_type="integer", is_pk=False, comment=None
            ),
            TableColumn(
                ordinal=2,
                name="name",
                pg_type="character varying",
                is_pk=False,
                comment=None,
            ),
        ],
        {"table_name": "vegetables", "cursor_fields": ["vegetable_id"]},
    )
    image_hash_3 = handler.sync(output, image_hash=image_hash_2, tables=tables)

    assert len(output.images()) == 3
    image = output.images[image_hash_3]
    image.checkout()

    assert output.run_sql("SELECT COUNT(*) FROM fruits") == [(4,)]
    assert output.run_sql("SELECT COUNT(*) FROM vegetables") == [(2,)]
    assert _get_state(output) == {
        "cursor_values": {
            "fruits": {
                "fruit_id": "4",
            },
            "vegetables": {"vegetable_id": "2"},
        }
    }


def test_fdw_data_source_with_composite_cursors(pg_repo_local):
    # Same as previous test, but make sure it still works with composite cursors (more than one
    # column)

    tables = {
        "fruits": (
            [
                TableColumn(
                    ordinal=1, name="fruit_id", pg_type="integer", is_pk=False, comment=None
                ),
                TableColumn(
                    ordinal=2,
                    name="name",
                    pg_type="character varying",
                    is_pk=False,
                    comment=None,
                ),
            ],
            {"table_name": "fruits", "cursor_fields": ["fruit_id", "name"]},
        )
    }

    engine = pg_repo_local.object_engine
    handler = PostgreSQLDataSource(
        engine=engine,
        credentials={
            "username": engine.conn_params["SG_ENGINE_USER"],
            "password": engine.conn_params["SG_ENGINE_PWD"],
        },
        params={
            "host": engine.conn_params["SG_ENGINE_HOST"],
            "port": int(engine.conn_params["SG_ENGINE_PORT"]),
            "dbname": engine.conn_params["SG_ENGINE_DB_NAME"],
            "remote_schema": pg_repo_local.to_schema(),
        },
        tables=tables,
    )

    output = Repository("test", "fdw_sync")
    image_hash_1 = handler.load(output)
    output.images[image_hash_1].checkout()

    assert output.run_sql("SELECT COUNT(*) FROM fruits") == [(2,)]
    assert _get_state(output) == {
        "cursor_values": {
            "fruits": {
                "fruit_id": "2",
                "name": "orange",
            },
        }
    }

    # Add a row to the table (greatest is 2, orange but we only use the second field as a
    # tiebreaker, so its value doesn't need to be greater)
    pg_repo_local.run_sql("INSERT INTO fruits (name) VALUES ('kumquat')")
    pg_repo_local.commit_engines()

    image_hash_2 = handler.sync(output, image_hash=image_hash_1)

    assert len(output.images()) == 2
    output.images[image_hash_2].checkout()

    assert output.run_sql("SELECT COUNT(*) FROM fruits") == [(3,)]
    assert _get_state(output) == {
        "cursor_values": {
            "fruits": {
                "fruit_id": "3",
                "name": "kumquat",
            },
        }
    }
