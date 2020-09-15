from datetime import datetime as dt

import pytest
from test.splitgraph.conftest import _mount_postgres, _mount_mysql, _mount_mongo

from splitgraph.core.repository import Repository
from splitgraph.core.types import TableColumn
from splitgraph.engine import get_engine
from splitgraph.hooks.mount_handlers import mount

PG_MNT = Repository.from_schema("test/pg_mount")
MG_MNT = Repository.from_schema("test_mg_mount")
MYSQL_MNT = Repository.from_schema("test/mysql_mount")


@pytest.mark.mounting
def test_mount_unmount(local_engine_empty):
    _mount_postgres(PG_MNT)
    assert (1, "apple") in get_engine().run_sql("""SELECT * FROM "test/pg_mount".fruits""")
    PG_MNT.delete()
    assert not get_engine().schema_exists(PG_MNT.to_schema())


@pytest.mark.mounting
def test_mount_partial(local_engine_empty):
    _mount_postgres(PG_MNT, tables=["fruits"])
    assert get_engine().table_exists(PG_MNT.to_schema(), "fruits")
    assert not get_engine().table_exists(PG_MNT.to_schema(), "vegetables")


@pytest.mark.mounting
def test_mount_force_schema(local_engine_empty):
    _mount_postgres(PG_MNT, tables={"fruits": {"fruit_id": "character varying"}})
    assert get_engine().table_exists(PG_MNT.to_schema(), "fruits")
    assert get_engine().get_full_table_schema(PG_MNT.to_schema(), "fruits") == [
        TableColumn(1, "fruit_id", "character varying", False, None)
    ]


@pytest.mark.mounting
def test_mount_mysql(local_engine_empty):
    try:
        _mount_mysql(MYSQL_MNT)
        # Gotchas: bool coerced to int
        assert (2, "deathcap", dt(2018, 3, 17, 8, 6, 26), 0) in get_engine().run_sql(
            """SELECT mushroom_id, name, discovery, friendly
                           FROM "test/mysql_mount".mushrooms
                           WHERE friendly = 0"""
        )
    finally:
        MYSQL_MNT.delete()


@pytest.mark.mounting
def test_cross_joins(local_engine_empty):
    _mount_postgres(PG_MNT)
    _mount_mongo(MG_MNT)
    assert (
        local_engine_empty.run_sql(
            """SELECT "test/pg_mount".fruits.fruit_id,
                 test_mg_mount.stuff.name,
                 "test/pg_mount".fruits.name as spirit_fruit
           FROM "test/pg_mount".fruits 
           JOIN test_mg_mount.stuff 
           ON "test/pg_mount".fruits.fruit_id = test_mg_mount.stuff.duration"""
        )
        == [(2, "James", "orange")]
    )


@pytest.mark.mounting
def test_mount_elasticsearch(local_engine_empty):
    # No ES running in this stack: this is just a test that we can instantiate the FDW.
    repo = Repository("test", "es_mount")
    try:
        mount(
            repo.to_schema(),
            "elasticsearch",
            dict(
                username=None,
                password=None,
                server="elasticsearch",
                port=9200,
                table_spec={
                    "table_1": {
                        "schema": {
                            "id": "text",
                            "@timestamp": "timestamp",
                            "query": "text",
                            "col_1": "text",
                            "col_2": "boolean",
                        },
                        "index": "index-pattern*",
                        "rowid_column": "id",
                        "query_column": "query",
                    }
                },
            ),
        )

        assert get_engine().get_full_table_schema(repo.to_schema(), "table_1") == [
            TableColumn(ordinal=1, name="id", pg_type="text", is_pk=False, comment=None),
            TableColumn(
                ordinal=2,
                name="@timestamp",
                pg_type="timestamp without time zone",
                is_pk=False,
                comment=None,
            ),
            TableColumn(ordinal=3, name="query", pg_type="text", is_pk=False, comment=None),
            TableColumn(ordinal=4, name="col_1", pg_type="text", is_pk=False, comment=None),
            TableColumn(ordinal=5, name="col_2", pg_type="boolean", is_pk=False, comment=None),
        ]

    finally:
        repo.delete()
