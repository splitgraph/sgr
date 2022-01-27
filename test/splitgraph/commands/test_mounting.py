from datetime import datetime as dt
from test.splitgraph.conftest import (
    _mount_elasticsearch,
    _mount_mongo,
    _mount_mysql,
    _mount_postgres,
)

import pytest

from splitgraph.core.repository import Repository
from splitgraph.core.types import TableColumn
from splitgraph.engine import get_engine
from splitgraph.hooks.data_source.fdw import PostgreSQLDataSource
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
    _mount_postgres(PG_MNT, tables={"fruits": {"schema": {"fruit_id": "character varying"}}})
    assert get_engine().table_exists(PG_MNT.to_schema(), "fruits")
    assert get_engine().get_full_table_schema(PG_MNT.to_schema(), "fruits") == [
        TableColumn(1, "fruit_id", "character varying", False, None)
    ]


@pytest.mark.mounting
def test_mount_mysql(local_engine_empty):
    try:
        # Mount MySQL with a set schema instead of letting the FDW detect it
        _mount_mysql(MYSQL_MNT)
        result = get_engine().run_sql(
            """SELECT mushroom_id, name, discovery, friendly, binary_data, varbinary_data
            FROM "test/mysql_mount".mushrooms"""
        )
        assert len(result) == 2
        assert any(r[1] == "deathcap" and r[2] == dt(2018, 3, 17, 8, 6, 26) for r in result)
        # Check binary -> bytea conversion works (the data is binary-encoded 127.0.0.1 IP)
        assert sorted(result, key=lambda r: r[0])[0][5].hex() == "7f000001"
    finally:
        MYSQL_MNT.delete()


@pytest.mark.mounting
def test_mount_esorigin(local_engine_empty):
    # Mount Elasticsearch with a set schema instead of letting the FDW detect it
    _mount_elasticsearch()
    result = get_engine().run_sql(
        """SELECT account_number, balance, firstname, lastname, age, gender,
        address, employer, email, city, state
        FROM es.account"""
    )
    assert len(result) == 1000

    # Assert random record matches
    assert result[28] == (
        140,
        26696.0,
        "Cotton",
        "Christensen",
        32,
        "M",
        "878 Schermerhorn Street",
        "Prowaste",
        "cottonchristensen@prowaste.com",
        "Mayfair",
        "LA",
    )


@pytest.mark.mounting
def test_mount_introspection_preview(local_engine_empty):
    handler = PostgreSQLDataSource(
        engine=local_engine_empty,
        credentials={"username": "originro", "password": "originpass"},
        params={"host": "pgorigin", "port": 5432, "dbname": "origindb", "remote_schema": "public"},
    )

    tables = handler.introspect()

    assert tables == {
        "fruits": (
            [
                TableColumn(
                    ordinal=1, name="fruit_id", pg_type="integer", is_pk=False, comment=None
                ),
                TableColumn(
                    ordinal=2, name="name", pg_type="character varying", is_pk=False, comment=None
                ),
            ],
            {"schema_name": "public", "table_name": "fruits"},
        ),
        "vegetables": (
            [
                TableColumn(
                    ordinal=1, name="vegetable_id", pg_type="integer", is_pk=False, comment=None
                ),
                TableColumn(
                    ordinal=2, name="name", pg_type="character varying", is_pk=False, comment=None
                ),
            ],
            {"schema_name": "public", "table_name": "vegetables"},
        ),
        "account": (
            [
                TableColumn(
                    ordinal=1, name="account_number", pg_type="integer", is_pk=False, comment=None
                ),
                TableColumn(
                    ordinal=2, name="balance", pg_type="integer", is_pk=False, comment=None
                ),
                TableColumn(
                    ordinal=3,
                    name="firstname",
                    pg_type="character varying(20)",
                    is_pk=False,
                    comment=None,
                ),
                TableColumn(
                    ordinal=4,
                    name="lastname",
                    pg_type="character varying(20)",
                    is_pk=False,
                    comment=None,
                ),
                TableColumn(ordinal=5, name="age", pg_type="integer", is_pk=False, comment=None),
                TableColumn(
                    ordinal=6,
                    name="gender",
                    pg_type="character varying(1)",
                    is_pk=False,
                    comment=None,
                ),
                TableColumn(ordinal=7, name="address", pg_type="text", is_pk=False, comment=None),
                TableColumn(
                    ordinal=8,
                    name="employer",
                    pg_type="character varying(20)",
                    is_pk=False,
                    comment=None,
                ),
                TableColumn(ordinal=9, name="email", pg_type="text", is_pk=False, comment=None),
                TableColumn(
                    ordinal=10,
                    name="city",
                    pg_type="character varying(20)",
                    is_pk=False,
                    comment=None,
                ),
                TableColumn(
                    ordinal=11,
                    name="state",
                    pg_type="character varying(5)",
                    is_pk=False,
                    comment=None,
                ),
            ],
            {"schema_name": "public", "table_name": "account"},
        ),
    }

    # Discard the account table due to its size
    tables.pop("account")

    preview = handler.preview(tables=tables)
    assert preview == {
        "fruits": [{"fruit_id": 1, "name": "apple"}, {"fruit_id": 2, "name": "orange"}],
        "vegetables": [
            {"name": "potato", "vegetable_id": 1},
            {"name": "carrot", "vegetable_id": 2},
        ],
    }


@pytest.mark.mounting
def test_mount_rename_table(local_engine_empty):
    tables = {
        "fruits_renamed": (
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
            {"table_name": "fruits"},
        )
    }
    handler = PostgreSQLDataSource(
        engine=local_engine_empty,
        credentials={"username": "originro", "password": "originpass"},
        params={"host": "pgorigin", "port": 5432, "dbname": "origindb", "remote_schema": "public"},
        tables=tables,
    )

    preview = handler.preview(tables)
    assert preview == {
        "fruits_renamed": [{"fruit_id": 1, "name": "apple"}, {"fruit_id": 2, "name": "orange"}],
    }


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
            {
                "host": "elasticsearch",
                "port": 9200,
                "tables": {
                    "table_1": {
                        "schema": {
                            "id": "text",
                            "@timestamp": "timestamp",
                            "query": "text",
                            "col_1": "text",
                            "col_2": "boolean",
                        },
                        "options": {
                            "index": "index-pattern*",
                            "rowid_column": "id",
                            "query_column": "query",
                        },
                    }
                },
            },
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


@pytest.mark.mounting
def test_mount_with_empty_credentials(local_engine_empty):
    class EmptyCredentialsSchemaDataSource(PostgreSQLDataSource):
        credentials_schema = {"type": "object"}

        def get_user_options(self):
            return {"user": "originro", "password": "originpass"}

    handler = EmptyCredentialsSchemaDataSource(
        engine=local_engine_empty,
        credentials={},
        params={"host": "pgorigin", "port": 5432, "dbname": "origindb", "remote_schema": "public"},
    )

    tables = handler.introspect()
    assert tables
    assert handler.from_commandline(
        handler.engine,
        {"host": "pgorigin", "port": 5432, "dbname": "origindb", "remote_schema": "public"},
    )
