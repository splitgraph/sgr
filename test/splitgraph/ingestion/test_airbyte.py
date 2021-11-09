import datetime
import os
import re
from distutils.dir_util import copy_tree
from test.splitgraph.commands.test_schema_changes import reassign_ordinals
from test.splitgraph.conftest import INGESTION_RESOURCES
from test.splitgraph.utils import reassign_ordinals
from unittest import mock

import pytest
from psycopg2.sql import SQL, Identifier
from splitgraph.core.repository import Repository
from splitgraph.core.types import TableColumn, TableParams
from splitgraph.engine import ResultShape
from splitgraph.hooks.data_source import merge_jsonschema
from splitgraph.ingestion.airbyte.data_source import AirbyteDataSource
from splitgraph.ingestion.airbyte.docker_utils import SubprocessError
from splitgraph.ingestion.airbyte.models import (
    AirbyteCatalog,
    AirbyteStream,
    DestinationSyncMode,
    SyncMode,
)
from splitgraph.ingestion.airbyte.utils import select_streams


class MySQLAirbyteDataSource(AirbyteDataSource):
    docker_image = "airbyte/source-mysql:latest"
    airbyte_name = "airbyte-mysql"

    credentials_schema = merge_jsonschema(
        AirbyteDataSource.credentials_schema,
        {"type": "object", "properties": {"password": {"type": "string"}}},
    )
    params_schema = merge_jsonschema(
        AirbyteDataSource.credentials_schema,
        {
            "type": "object",
            "properties": {
                "host": {"type": "string"},
                "port": {"type": "integer"},
                "database": {"type": "string"},
                "username": {"type": "string"},
                "replication_method": {"type": "string"},
            },
            "required": ["host", "port", "database", "username", "replication_method"],
        },
    )

    @classmethod
    def get_name(cls) -> str:
        return "MySQL (Airbyte)"

    @classmethod
    def get_description(cls) -> str:
        return "MySQL (Airbyte)"


def _source(local_engine_empty, table_params=None, extra_params=None, extra_credentials=None):
    extra_params = extra_params or {}
    extra_credentials = extra_credentials or {}
    return MySQLAirbyteDataSource(
        engine=local_engine_empty,
        params={
            "replication_method": "STANDARD",
            "host": "localhost",
            "port": 3306,
            "database": "mysqlschema",
            "username": "originuser",
            **extra_params,
        },
        credentials={"password": "originpass", **extra_credentials},
        tables=table_params,
    )


_EXPECTED_AIRBYTE_CATALOG = AirbyteCatalog(
    streams=[
        AirbyteStream(
            name="mushrooms",
            json_schema={
                "type": "object",
                "properties": {
                    "discovery": {"type": "string"},
                    "friendly": {"type": "boolean"},
                    "binary_data": {"type": "string"},
                    "name": {"type": "string"},
                    "mushroom_id": {"type": "number"},
                    "varbinary_data": {"type": "string"},
                },
            },
            supported_sync_modes=[SyncMode.full_refresh, SyncMode.incremental],
            source_defined_cursor=None,
            default_cursor_field=[],
            source_defined_primary_key=[["mushroom_id"]],
            namespace="mysqlschema",
        )
    ]
)
TEST_REPO = "test/airbyte"


@pytest.mark.mounting
def test_airbyte_mysql_source_introspection_harness(local_engine_empty):
    source = _source(local_engine_empty)

    airbyte_config = source.get_airbyte_config()
    assert airbyte_config == {
        "database": "mysqlschema",
        "host": "localhost",
        "password": "originpass",
        "port": 3306,
        "replication_method": "STANDARD",
        "username": "originuser",
    }

    airbyte_catalog = source._run_discovery(airbyte_config)
    assert airbyte_catalog == _EXPECTED_AIRBYTE_CATALOG


@pytest.mark.mounting
def test_airbyte_mysql_source_introspection_end_to_end(local_engine_empty):
    source = _source(local_engine_empty)

    introspection_result = source.introspect()

    assert introspection_result == {
        "mushrooms": (
            [
                TableColumn(
                    ordinal=0,
                    name="discovery",
                    pg_type="character varying",
                    is_pk=False,
                    comment=None,
                ),
                TableColumn(
                    ordinal=1, name="friendly", pg_type="boolean", is_pk=False, comment=None
                ),
                TableColumn(
                    ordinal=2,
                    name="binary_data",
                    pg_type="character varying",
                    is_pk=False,
                    comment=None,
                ),
                TableColumn(
                    ordinal=3, name="name", pg_type="character varying", is_pk=False, comment=None
                ),
                TableColumn(
                    ordinal=4,
                    name="mushroom_id",
                    pg_type="double precision",
                    is_pk=True,
                    comment=None,
                ),
                TableColumn(
                    ordinal=5,
                    name="varbinary_data",
                    pg_type="character varying",
                    is_pk=False,
                    comment=None,
                ),
            ],
            {"airbyte_cursor_field": [], "airbyte_primary_key": ["mushroom_id"]},
        )
    }

    # Introspect again but this time override the cursor field
    source = _source(
        local_engine_empty,
        table_params={"mushrooms": ([], {"airbyte_cursor_field": ["discovery"]})},
    )

    introspection_result = source.introspect()

    assert introspection_result["mushrooms"][1] == {
        "airbyte_cursor_field": ["discovery"],
        "airbyte_primary_key": ["mushroom_id"],
    }


def test_airbyte_mysql_source_catalog_selection_refresh():
    catalog = select_streams(_EXPECTED_AIRBYTE_CATALOG, tables=None, sync=False)
    assert len(catalog.streams) == 1
    assert catalog.streams[0].sync_mode == SyncMode.full_refresh
    assert catalog.streams[0].destination_sync_mode == DestinationSyncMode.overwrite


def test_airbyte_mysql_source_catalog_selection_incremental_no_cursor_fallback():
    catalog = select_streams(_EXPECTED_AIRBYTE_CATALOG, tables=None, sync=True)
    assert len(catalog.streams) == 1
    assert catalog.streams[0].sync_mode == SyncMode.full_refresh
    assert catalog.streams[0].destination_sync_mode == DestinationSyncMode.overwrite
    assert catalog.streams[0].cursor_field == []  # Default cursor field


def test_airbyte_mysql_source_catalog_selection_incremental_cursor_override():
    # Pretend mushroom_id can be used as an incremental cursor.
    catalog = select_streams(
        _EXPECTED_AIRBYTE_CATALOG,
        tables=None,
        sync=True,
        cursor_overrides={"mushrooms": ["mushroom_id"]},
    )
    assert len(catalog.streams) == 1
    assert catalog.streams[0].sync_mode == SyncMode.incremental
    assert catalog.streams[0].destination_sync_mode == DestinationSyncMode.append_dedup
    assert catalog.streams[0].primary_key == [["mushroom_id"]]
    assert catalog.streams[0].cursor_field == ["mushroom_id"]


def test_airbyte_mysql_source_catalog_selection_incremental_cursor_override_tables():
    catalog = select_streams(
        _EXPECTED_AIRBYTE_CATALOG,
        tables={"mushrooms": ([], TableParams({"airbyte_cursor_field": ["mushroom_id"]}))},
        sync=True,
    )
    assert len(catalog.streams) == 1
    assert catalog.streams[0].sync_mode == SyncMode.incremental
    assert catalog.streams[0].destination_sync_mode == DestinationSyncMode.append_dedup
    assert catalog.streams[0].primary_key == [["mushroom_id"]]
    assert catalog.streams[0].cursor_field == ["mushroom_id"]


def test_airbyte_mysql_source_catalog_selection_incremental_pk_override():
    catalog = select_streams(
        _EXPECTED_AIRBYTE_CATALOG,
        tables=None,
        sync=True,
        cursor_overrides={"mushrooms": ["discovery"]},
        primary_key_overrides={"mushrooms": ["discovery"]},
    )
    assert len(catalog.streams) == 1
    assert catalog.streams[0].sync_mode == SyncMode.incremental
    assert catalog.streams[0].destination_sync_mode == DestinationSyncMode.append_dedup
    assert catalog.streams[0].primary_key == [["discovery"]]
    assert catalog.streams[0].cursor_field == ["discovery"]


def test_airbyte_mysql_source_catalog_selection_incremental_pk_override_tables():
    catalog = select_streams(
        _EXPECTED_AIRBYTE_CATALOG,
        tables={
            "mushrooms": (
                [],
                TableParams(
                    {"airbyte_cursor_field": ["discovery"], "airbyte_primary_key": ["discovery"]}
                ),
            )
        },
        sync=True,
    )
    assert len(catalog.streams) == 1
    assert catalog.streams[0].sync_mode == SyncMode.incremental
    assert catalog.streams[0].destination_sync_mode == DestinationSyncMode.append_dedup
    assert catalog.streams[0].primary_key == [["discovery"]]
    assert catalog.streams[0].cursor_field == ["discovery"]


# Test in three modes:
# * Sync: two syncs one after another, make sure state is preserved and reinjected
# * Load: just a load into a fresh repo (not much difference since we still store emitted state)
# * Load after sync: make sure we delete data from raw tables between syncs.
@pytest.mark.mounting
@pytest.mark.parametrize("mode", ["sync", "load", "load_after_sync"])
def test_airbyte_mysql_source_end_to_end(local_engine_empty, mode):
    # Use the mushroom_id as the cursor for incremental replication.
    # Note we ignore the schema here (Airbyte does its own normalization so we can't predict it).
    repo = Repository.from_schema(TEST_REPO)

    if mode == "sync":
        source = _source(
            local_engine_empty,
            table_params={
                "mushrooms": ([], TableParams({"airbyte_cursor_field": ["mushroom_id"]}))
            },
        )
        source.sync(repo, "latest")
        expected_tables = [
            "_airbyte_raw_mushrooms",
            "_sg_ingestion_state",
            "mushrooms",
            # slowly changing dimension, used for incremental replication
            "mushrooms_scd",
        ]
    else:
        source = _source(local_engine_empty)
        source.load(repo)
        expected_tables = [
            "_airbyte_raw_mushrooms",
            "_sg_ingestion_state",
            "mushrooms",
        ]

    assert len(repo.images()) == 1
    image = repo.images["latest"]

    assert sorted(image.get_tables()) == expected_tables
    image.checkout()

    _assert_raw_data(repo)
    _assert_normalized_data(repo, unique_key=mode == "sync")

    if mode == "sync":
        _assert_state(repo)
        _assert_scd_data(repo)

        # Run another sync
        source.sync(repo, "latest")
        assert len(repo.images()) == 2
        image = repo.images["latest"]
        assert sorted(image.get_tables()) == [
            "_airbyte_raw_mushrooms",
            "_sg_ingestion_state",
            "mushrooms",
            "mushrooms_scd",
        ]
        image.checkout()

        # Check the empty object wasn't written
        assert len(image.get_table("_airbyte_raw_mushrooms").objects) == 1

        # Check the table lengths are all the same (including the raw tables, since we used the
        # ingestion state to make sure the source didn't output more raw data)
        for table in image.get_tables():
            expected_rows = 1 if table == "_sg_ingestion_state" else 2
            assert (
                repo.run_sql(
                    SQL("SELECT COUNT(1) FROM {}").format(Identifier(table)),
                    return_shape=ResultShape.ONE_ONE,
                )
                == expected_rows
            )
    elif mode == "load":
        _assert_state_empty(repo)
    elif mode == "load_after_sync":
        # Run a load after a sync to make sure the image gets cleared out properly.

        source.load(repo)

        assert len(repo.images()) == 2
        image = repo.images["latest"]

        # Check the SDC table went away
        assert sorted(image.get_tables()) == [
            "_airbyte_raw_mushrooms",
            "_sg_ingestion_state",
            "mushrooms",
        ]
        image.checkout()

        _assert_raw_data(repo)
        _assert_normalized_data(repo, unique_key=False)
        _assert_state_empty(repo)


@pytest.mark.mounting
def test_airbyte_mysql_source_pk_override(local_engine_empty):
    source = _source(local_engine_empty)
    repo = Repository.from_schema(TEST_REPO)
    source.cursor_overrides = {"mushrooms": ["discovery"]}
    source.primary_key_overrides = {"mushrooms": ["discovery"]}
    # Use sync since otherwise we don't get any effect in the destination (destination_sync_mode
    # has to be append_dedup)
    source.sync(repo, "latest")

    # Note we don't actually emit PKs here so we can't check they have changed (only influences
    # dedup). This is mostly to make sure it doesn't break.
    assert len(repo.images()) == 1
    repo.images["latest"].checkout()
    _assert_normalized_data(repo, unique_key=True)


@pytest.mark.mounting
def test_airbyte_mysql_source_no_normalization(local_engine_empty):
    repo = Repository.from_schema(TEST_REPO)

    source = _source(local_engine_empty, extra_params={"normalization_mode": "none"})
    source.load(repo)
    expected_tables = [
        "_airbyte_raw_mushrooms",
        "_sg_ingestion_state",
    ]

    assert len(repo.images()) == 1
    image = repo.images["latest"]
    assert sorted(image.get_tables()) == expected_tables


@pytest.mark.mounting
def test_airbyte_mysql_source_custom_normalization(local_engine_empty):
    # Pass in a Git repo with a dbt project that will do custom normalization for us.
    repo = Repository.from_schema(TEST_REPO)
    fixture_git_repo = os.path.join(INGESTION_RESOURCES, "dbt")

    source = _source(
        local_engine_empty,
        extra_params={"normalization_mode": "git", "normalization_git_branch": "some/branch"},
        extra_credentials={"normalization_git_url": "https://some-repo"},
    )

    # Use a mock so that we don't have to actually turn our fixture into a Git repo
    # and make the process pull it from a local file path.
    def _prepare_git_repo(url, target_path, ref):
        assert url == "https://some-repo"
        assert ref == "some/branch"
        # Use copy_tree from distutils because shutil breaks if target_path exists.
        copy_tree(fixture_git_repo, target_path)

    with mock.patch("splitgraph.ingestion.dbt.utils.prepare_git_repo", _prepare_git_repo):
        source.load(repo)

    expected_tables = [
        "_airbyte_raw_mushrooms",
        "_sg_ingestion_state",
        "dim_mushrooms",
    ]

    assert len(repo.images()) == 1
    image = repo.images["latest"]
    assert sorted(image.get_tables()) == expected_tables

    image.checkout()
    assert repo.run_sql(
        "SELECT mushroom_name, discovered_on FROM dim_mushrooms ORDER BY discovered_on ASC"
    ) == [
        ("portobello", datetime.datetime(2012, 11, 11, 8, 6, 26)),
        ("deathcap", datetime.datetime(2018, 3, 17, 8, 6, 26)),
    ]


def _assert_state(repo):
    assert repo.run_sql("SELECT state FROM _sg_ingestion_state")[0][0] == {
        "cdc": False,
        "streams": [
            {
                "stream_name": "mushrooms",
                "stream_namespace": "mysqlschema",
                "cursor_field": ["mushroom_id"],
                "cursor": "2",
            }
        ],
    }


def _assert_state_empty(repo):
    assert repo.run_sql("SELECT state FROM _sg_ingestion_state")[0][0] == {}


def _assert_scd_data(repo):
    assert repo.run_sql(
        "SELECT row_to_json(m) FROM mushrooms_scd m ORDER BY discovery ASC",
        return_shape=ResultShape.MANY_ONE,
    ) == [
        {
            "_airbyte_ab_id": mock.ANY,
            "_airbyte_active_row": 1,
            "_airbyte_emitted_at": mock.ANY,
            "_airbyte_end_at": None,
            "_airbyte_mushrooms_hashid": "e48f260f784baa48a5c4643ef36024af",
            "_airbyte_normalized_at": mock.ANY,
            "_airbyte_start_at": 1,
            "_airbyte_unique_key": "c4ca4238a0b923820dcc509a6f75849b",
            "_airbyte_unique_key_scd": mock.ANY,
            "binary_data": "YmludHN0AA==",
            "discovery": "2012-11-11T08:06:26Z",
            "friendly": True,
            "mushroom_id": 1,
            "name": "portobello",
            "varbinary_data": "fwAAAQ==",
        },
        {
            "_airbyte_ab_id": mock.ANY,
            "_airbyte_active_row": 1,
            "_airbyte_emitted_at": mock.ANY,
            "_airbyte_end_at": None,
            "_airbyte_mushrooms_hashid": "5257322455a690592e14baeb4d24069c",
            "_airbyte_normalized_at": mock.ANY,
            "_airbyte_start_at": 2,
            "_airbyte_unique_key": "c81e728d9d4c2f636f067f89cc14862c",
            "_airbyte_unique_key_scd": mock.ANY,
            "binary_data": "AAAxMjMAAA==",
            "discovery": "2018-03-17T08:06:26Z",
            "friendly": False,
            "mushroom_id": 2,
            "name": "deathcap",
            "varbinary_data": "fwAAAQ==",
        },
    ]


def _assert_normalized_data(repo, unique_key=False):
    expected = [
        {
            "discovery": "2012-11-11T08:06:26Z",
            "friendly": True,
            "binary_data": "YmludHN0AA==",
            "name": "portobello",
            "mushroom_id": 1,
            "varbinary_data": "fwAAAQ==",
            "_airbyte_ab_id": mock.ANY,
            "_airbyte_emitted_at": mock.ANY,
            "_airbyte_normalized_at": mock.ANY,
            "_airbyte_mushrooms_hashid": "e48f260f784baa48a5c4643ef36024af",
        },
        {
            "discovery": "2018-03-17T08:06:26Z",
            "friendly": False,
            "binary_data": "AAAxMjMAAA==",
            "name": "deathcap",
            "mushroom_id": 2,
            "varbinary_data": "fwAAAQ==",
            "_airbyte_ab_id": mock.ANY,
            "_airbyte_emitted_at": mock.ANY,
            "_airbyte_normalized_at": mock.ANY,
            "_airbyte_mushrooms_hashid": "5257322455a690592e14baeb4d24069c",
        },
    ]

    # Airbyte's normalization doesn't seem to emit PKs, so all is_pk will be False in any case.
    expected_schema = [
        TableColumn(
            ordinal=1,
            name="discovery",
            pg_type="character varying",
            is_pk=False,
            comment=None,
        ),
        TableColumn(ordinal=2, name="friendly", pg_type="boolean", is_pk=False, comment=None),
        TableColumn(
            ordinal=3,
            name="binary_data",
            pg_type="character varying",
            is_pk=False,
            comment=None,
        ),
        TableColumn(ordinal=4, name="name", pg_type="character varying", is_pk=False, comment=None),
        TableColumn(
            ordinal=5,
            name="mushroom_id",
            pg_type="double precision",
            is_pk=False,
            comment=None,
        ),
        TableColumn(
            ordinal=6,
            name="varbinary_data",
            pg_type="character varying",
            is_pk=False,
            comment=None,
        ),
        TableColumn(
            ordinal=7,
            name="_airbyte_ab_id",
            pg_type="character varying",
            is_pk=False,
            comment=None,
        ),
        TableColumn(
            ordinal=8,
            name="_airbyte_emitted_at",
            pg_type="timestamp with time zone",
            is_pk=False,
            comment=None,
        ),
        TableColumn(
            ordinal=9,
            name="_airbyte_normalized_at",
            pg_type="timestamp with time zone",
            is_pk=False,
            comment=None,
        ),
        TableColumn(
            ordinal=10,
            name="_airbyte_mushrooms_hashid",
            pg_type="text",
            is_pk=False,
            comment=None,
        ),
    ]

    if unique_key:
        expected[0]["_airbyte_unique_key"] = mock.ANY
        expected[1]["_airbyte_unique_key"] = mock.ANY
        expected_schema = reassign_ordinals(
            [
                TableColumn(
                    ordinal=1, name="_airbyte_unique_key", pg_type="text", is_pk=False, comment=None
                )
            ]
            + expected_schema
        )

    # Check the normalized data and the schema
    assert (
        repo.run_sql(
            "SELECT row_to_json(m) FROM mushrooms m ORDER BY discovery ASC",
            return_shape=ResultShape.MANY_ONE,
        )
        == expected
    )

    assert repo.images["latest"].get_table("mushrooms").table_schema == expected_schema


def _assert_raw_data(repo):
    # Check the raw data
    assert sorted(
        repo.run_sql(
            "SELECT row_to_json(m) FROM _airbyte_raw_mushrooms m", return_shape=ResultShape.MANY_ONE
        ),
        key=lambda r: r["_airbyte_data"]["mushroom_id"],
    ) == [
        {
            "_airbyte_ab_id": mock.ANY,
            "_airbyte_data": {
                "name": "portobello",
                "friendly": True,
                "discovery": "2012-11-11T08:06:26Z",
                "binary_data": "YmludHN0AA==",
                "mushroom_id": 1,
                "varbinary_data": "fwAAAQ==",
            },
            "_airbyte_emitted_at": mock.ANY,
        },
        {
            "_airbyte_ab_id": mock.ANY,
            "_airbyte_data": {
                "name": "deathcap",
                "friendly": False,
                "discovery": "2018-03-17T08:06:26Z",
                "binary_data": "AAAxMjMAAA==",
                "mushroom_id": 2,
                "varbinary_data": "fwAAAQ==",
            },
            "_airbyte_emitted_at": mock.ANY,
        },
    ]


@pytest.mark.mounting
def test_airbyte_mysql_source_failure(local_engine_empty):
    source = _source(local_engine_empty)
    source.credentials["password"] = "wrongpass"
    repo = Repository.from_schema(TEST_REPO)

    with pytest.raises(SubprocessError) as e:
        source.sync(repo, "latest")
    assert re.match(r"Container sg-ab-src-\S+ exited with 1", str(e.value))
    # Check we didn't create an empty image
    assert len(repo.images()) == 0
