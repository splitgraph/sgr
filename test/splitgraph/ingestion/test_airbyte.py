import pytest
from airbyte_cdk.models import AirbyteCatalog, AirbyteStream, SyncMode, DestinationSyncMode

from splitgraph.core.types import TableColumn
from splitgraph.ingestion.airbyte.utils import select_streams

try:
    from splitgraph.ingestion.airbyte.data_source import AirbyteDataSource
except ImportError:
    pytest.skip("airbyte-cdk (from the airbyte extra) not available", allow_module_level=True)


class MySQLAirbyteDataSource(AirbyteDataSource):
    docker_image = "airbyte/source-mysql:latest"
    airbyte_name = "airbyte-mysql"

    credentials_schema = {"type": "object", "properties": {"password": {"type": "string"}}}
    params_schema = {
        "type": "object",
        "properties": {
            "host": {"type": "string"},
            "port": {"type": "integer"},
            "database": {"type": "string"},
            "username": {"type": "string"},
            "replication_method": {"type": "string"},
        },
        "required": ["host", "port", "database", "username", "replication_method"],
    }

    @classmethod
    def get_name(cls) -> str:
        return "MySQL (Airbyte)"

    @classmethod
    def get_description(cls) -> str:
        return "MySQL (Airbyte)"


def _source(local_engine_empty):
    return MySQLAirbyteDataSource(
        engine=local_engine_empty,
        params={
            "replication_method": "STANDARD",
            "host": "localhost",
            "port": 3306,
            "database": "mysqlschema",
            "username": "originuser",
        },
        credentials={
            "password": "originpass",
        },
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

    assert source.introspect() == {
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
            {},
        )
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
