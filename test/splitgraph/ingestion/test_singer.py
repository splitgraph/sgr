import json
import os
import shutil
from datetime import datetime
from decimal import Decimal
from test.splitgraph.conftest import INGESTION_RESOURCES
from unittest import mock

import psycopg2
import pytest
from click.testing import CliRunner

from splitgraph.core.repository import Repository
from splitgraph.core.types import TableColumn
from splitgraph.engine import ResultShape
from splitgraph.ingestion.singer.commandline import singer_target
from splitgraph.ingestion.singer.data_source import (
    GenericSingerDataSource,
    MySQLSingerDataSource,
)
from splitgraph.ingestion.singer.db_sync import select_breadcrumb

TEST_REPO = "test/singer"
TEST_TAP = os.path.join(INGESTION_RESOURCES, "singer/fake_tap.py")

_STARGAZERS_SCHEMA = [
    TableColumn(
        ordinal=0,
        name="_sdc_repository",
        pg_type="character varying",
        is_pk=False,
        comment=None,
    ),
    TableColumn(
        ordinal=1,
        name="starred_at",
        pg_type="timestamp without time zone",
        is_pk=False,
        comment=None,
    ),
    TableColumn(ordinal=2, name="user", pg_type="jsonb", is_pk=False, comment=None),
    TableColumn(ordinal=3, name="user_id", pg_type="numeric", is_pk=True, comment=None),
]


_RELEASES_SCHEMA = [
    TableColumn(
        ordinal=0,
        name="_sdc_repository",
        pg_type="character varying",
        is_pk=False,
        comment=None,
    ),
    TableColumn(ordinal=1, name="author", pg_type="jsonb", is_pk=False, comment=None),
    TableColumn(ordinal=2, name="body", pg_type="character varying", is_pk=False, comment=None),
    TableColumn(
        ordinal=3,
        name="created_at",
        pg_type="timestamp without time zone",
        is_pk=False,
        comment=None,
    ),
    TableColumn(ordinal=4, name="draft", pg_type="boolean", is_pk=False, comment=None),
    TableColumn(ordinal=5, name="html_url", pg_type="character varying", is_pk=False, comment=None),
    TableColumn(ordinal=6, name="id", pg_type="character varying", is_pk=True, comment=None),
    TableColumn(ordinal=7, name="name", pg_type="character varying", is_pk=False, comment=None),
    TableColumn(ordinal=8, name="prerelease", pg_type="boolean", is_pk=False, comment=None),
    TableColumn(
        ordinal=9,
        name="published_at",
        pg_type="timestamp without time zone",
        is_pk=False,
        comment=None,
    ),
    TableColumn(
        ordinal=10, name="tag_name", pg_type="character varying", is_pk=False, comment=None
    ),
    TableColumn(
        ordinal=11,
        name="target_commitish",
        pg_type="character varying",
        is_pk=False,
        comment=None,
    ),
    TableColumn(ordinal=12, name="url", pg_type="character varying", is_pk=False, comment=None),
]


def test_singer_ingestion_initial(local_engine_empty):
    # Initial ingestion: two tables (stargazers and releases) grabbed from the output of
    # tap-github, truncated (simulate table creation and insertion of some rows)

    runner = CliRunner(mix_stderr=False)

    with open(os.path.join(INGESTION_RESOURCES, "singer/initial.json"), "r") as f:
        result = runner.invoke(singer_target, [TEST_REPO], input=f, catch_exceptions=False)

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "bookmarks": {
            "splitgraph/splitgraph": {
                "stargazers": {"since": "2020-10-14T11:06:40.852311Z"},
                "releases": {"since": "2020-10-14T11:06:40.852311Z"},
            }
        }
    }
    repo = Repository.from_schema(TEST_REPO)

    assert len(repo.images()) == 1
    image = repo.images["latest"]
    assert sorted(image.get_tables()) == ["releases", "stargazers"]
    image.checkout()

    assert repo.run_sql("SELECT COUNT(1) FROM releases", return_shape=ResultShape.ONE_ONE) == 6
    assert repo.run_sql("SELECT COUNT(1) FROM stargazers", return_shape=ResultShape.ONE_ONE) == 5

    assert repo.run_sql("SELECT user_id, starred_at FROM stargazers ORDER BY user_id") == [
        (Decimal("100001"), datetime(2018, 10, 17, 22, 14, 12)),
        (Decimal("100002"), datetime(2018, 11, 6, 11, 26, 16)),
        (Decimal("100003"), datetime(2018, 12, 11, 16, 0, 42)),
        (Decimal("100004"), datetime(2019, 2, 18, 8, 14, 21)),
        (Decimal("100005"), datetime(2019, 4, 18, 2, 40, 47)),
    ]

    assert image.get_table("releases").table_schema == _RELEASES_SCHEMA
    assert image.get_table("releases").objects == [
        "oae029fe8e58f00dd4c7513dc8b85985fb8384b15d11fc348e90896ea9cdd20"
    ]

    assert image.get_table("stargazers").table_schema == _STARGAZERS_SCHEMA
    assert image.get_table("stargazers").objects == [
        "od68e932ebc99c1a337363c1b92056dcf7fc7c6c45494bc42e1e1ec4e0c88ac"
    ]


def test_singer_ingestion_update(local_engine_empty):
    # Run the initial ingestion and then a repeat ingestion with a few rows getting updated
    # (check that a record with the same PK but a different value gets picked up as a diff),
    # a few inserted and one inserted that hasn't changed (check it's not saved in the diff).
    runner = CliRunner(mix_stderr=False)

    with open(os.path.join(INGESTION_RESOURCES, "singer/initial.json"), "r") as f:
        result = runner.invoke(
            singer_target, [TEST_REPO + ":latest"], input=f, catch_exceptions=False
        )

    assert result.exit_code == 0

    with open(os.path.join(INGESTION_RESOURCES, "singer/update.json"), "r") as f:
        result = runner.invoke(
            singer_target, [TEST_REPO + ":latest"], input=f, catch_exceptions=False
        )

    assert result.exit_code == 0

    assert json.loads(result.stdout) == {
        "bookmarks": {
            "splitgraph/splitgraph": {
                "releases": {"since": "2020-10-14T11:06:42.786589Z"},
                "stargazers": {"since": "2020-10-14T11:06:42.565793Z"},
            }
        }
    }
    repo = Repository.from_schema(TEST_REPO)

    assert len(repo.images()) == 2
    image = repo.images["latest"]
    assert sorted(image.get_tables()) == ["releases", "stargazers"]
    image.checkout()

    assert repo.run_sql("SELECT COUNT(1) FROM releases", return_shape=ResultShape.ONE_ONE) == 9
    assert repo.run_sql("SELECT COUNT(1) FROM stargazers", return_shape=ResultShape.ONE_ONE) == 6

    assert repo.run_sql("SELECT user_id, starred_at FROM stargazers ORDER BY user_id") == [
        (Decimal("100001"), datetime(2018, 10, 17, 22, 14, 12)),
        (Decimal("100002"), datetime(2018, 11, 6, 11, 26, 16)),
        (Decimal("100003"), datetime(2018, 12, 11, 16, 0, 42)),
        (Decimal("100004"), datetime(2020, 10, 11, 21, 9, 30)),
        (Decimal("100005"), datetime(2019, 4, 18, 2, 40, 47)),
        (Decimal("100006"), datetime(2019, 6, 6, 20, 53)),
    ]

    assert image.get_table("releases").table_schema == _RELEASES_SCHEMA
    assert image.get_table("releases").objects == [
        "oae029fe8e58f00dd4c7513dc8b85985fb8384b15d11fc348e90896ea9cdd20",
        "od5d0eea0e4348657b9899e7813da20258d74862a671c5644820205f051568f",
    ]

    assert image.get_table("stargazers").table_schema == _STARGAZERS_SCHEMA

    # Extra DIFF gets split into two: one overwriting the old object, the other one
    # with new rows.
    assert image.get_table("stargazers").objects == [
        "od68e932ebc99c1a337363c1b92056dcf7fc7c6c45494bc42e1e1ec4e0c88ac",
        "o5e93d48967488548e02f86b39f1e2f7a881fafbaa8cf0d1451fcc4971748f8",
        "o28d65722b57d834e3284c8e2609b6bf5c4d61adf51681fc7523f00ac2e95aa",
    ]

    # Check the fragment that we wrote the overwriting rows into
    assert repo.run_sql(
        "SELECT sg_ud_flag, user_id, starred_at "
        "FROM splitgraph_meta.o5e93d48967488548e02f86b39f1e2f7a881fafbaa8cf0d1451fcc4971748f8 "
        "ORDER BY user_id"
    ) == [
        (True, Decimal("100004"), datetime(2020, 10, 11, 21, 9, 30)),
        # Even though this row is the same as in the previous fragment, we keep it here
        # as we don't compare its value with the previous fragment.
        (True, Decimal("100005"), datetime(2019, 4, 18, 2, 40, 47)),
    ]

    # Check the fragment that we wrote the new rows into
    assert repo.run_sql(
        "SELECT sg_ud_flag, user_id, starred_at "
        "FROM splitgraph_meta.o28d65722b57d834e3284c8e2609b6bf5c4d61adf51681fc7523f00ac2e95aa "
        "ORDER BY user_id"
    ) == [
        (True, Decimal("100006"), datetime(2019, 6, 6, 20, 53)),
    ]


def test_singer_ingestion_schema_change(local_engine_empty):
    # Run the initial ingestion and then another one where we've changed the user_id in
    # stargazers to be a string.

    runner = CliRunner(mix_stderr=False)

    with open(os.path.join(INGESTION_RESOURCES, "singer/initial.json"), "r") as f:
        result = runner.invoke(
            singer_target, [TEST_REPO + ":latest"], input=f, catch_exceptions=False
        )

    assert result.exit_code == 0

    with open(os.path.join(INGESTION_RESOURCES, "singer/schema_change.json"), "r") as f:
        result = runner.invoke(
            singer_target, [TEST_REPO + ":latest"], input=f, catch_exceptions=False
        )

    assert result.exit_code == 0

    assert json.loads(result.stdout) == {
        "bookmarks": {
            "splitgraph/splitgraph": {
                "stargazers": {"since": "2020-10-14T11:06:42.565793Z"},
            }
        }
    }
    repo = Repository.from_schema(TEST_REPO)

    assert len(repo.images()) == 2
    image = repo.images["latest"]
    assert sorted(image.get_tables()) == ["releases", "stargazers"]
    image.checkout()

    assert repo.run_sql("SELECT COUNT(1) FROM releases", return_shape=ResultShape.ONE_ONE) == 6
    assert repo.run_sql("SELECT COUNT(1) FROM stargazers", return_shape=ResultShape.ONE_ONE) == 6

    assert repo.run_sql("SELECT user_id, starred_at FROM stargazers ORDER BY user_id") == [
        ("100001", datetime(2018, 10, 17, 22, 14, 12)),
        ("100002", datetime(2018, 11, 6, 11, 26, 16)),
        ("100003", datetime(2018, 12, 11, 16, 0, 42)),
        ("100004", datetime(2020, 10, 11, 21, 9, 30)),
        ("100005", datetime(2019, 4, 18, 2, 40, 47)),
        ("string_user_id", datetime(2019, 4, 18, 2, 40, 47)),
    ]

    # Releases unchanged -- same table
    assert image.get_table("releases").table_schema == _RELEASES_SCHEMA
    assert image.get_table("releases").objects == [
        "oae029fe8e58f00dd4c7513dc8b85985fb8384b15d11fc348e90896ea9cdd20"
    ]

    assert image.get_table("stargazers").table_schema == [
        TableColumn(
            ordinal=0,
            name="_sdc_repository",
            pg_type="character varying",
            is_pk=False,
            comment=None,
        ),
        TableColumn(
            ordinal=1,
            name="starred_at",
            pg_type="timestamp without time zone",
            is_pk=False,
            comment=None,
        ),
        TableColumn(ordinal=2, name="user", pg_type="jsonb", is_pk=False, comment=None),
        TableColumn(
            ordinal=3, name="user_id", pg_type="character varying", is_pk=True, comment=None
        ),
    ]

    # Stargazers: had a migration, new object
    assert image.get_table("stargazers").objects == [
        "o9e54958076c86d854ad21da17239daecaec839e84daee8ff9ca5dcecd84cdd"
    ]


def test_singer_ingestion_delete_old_image(local_engine_empty):
    runner = CliRunner(mix_stderr=False)

    with open(os.path.join(INGESTION_RESOURCES, "singer/initial.json"), "r") as f:
        result = runner.invoke(
            singer_target, [TEST_REPO + ":latest"], input=f, catch_exceptions=False
        )

    assert result.exit_code == 0

    with open(os.path.join(INGESTION_RESOURCES, "singer/update.json"), "r") as f:
        result = runner.invoke(
            singer_target, [TEST_REPO + ":latest", "--delete-old"], input=f, catch_exceptions=False
        )

    assert result.exit_code == 0
    repo = Repository.from_schema(TEST_REPO)
    assert len(repo.images()) == 1


def test_singer_ingestion_errors(local_engine_empty):
    runner = CliRunner(mix_stderr=False)

    with open(os.path.join(INGESTION_RESOURCES, "singer/initial.json"), "r") as f:
        result = runner.invoke(
            singer_target, [TEST_REPO + ":latest"], input=f, catch_exceptions=False
        )

    assert result.exit_code == 0

    # Default strategy: delete image on failure
    with open(os.path.join(INGESTION_RESOURCES, "singer/wrong_schema.json"), "r") as f:
        result = runner.invoke(
            singer_target, [TEST_REPO + ":latest"], input=f, catch_exceptions=True
        )

    assert result.exit_code == 1
    assert isinstance(result.exception, psycopg2.errors.InvalidDatetimeFormat)
    repo = Repository.from_schema(TEST_REPO)
    assert len(repo.images()) == 1

    # Keep new image
    with open(os.path.join(INGESTION_RESOURCES, "singer/wrong_schema.json"), "r") as f:
        result = runner.invoke(
            singer_target,
            [TEST_REPO + ":latest", "--failure=keep-both"],
            input=f,
            catch_exceptions=True,
        )

    assert result.exit_code == 1
    assert isinstance(result.exception, psycopg2.errors.InvalidDatetimeFormat)
    repo = Repository.from_schema(TEST_REPO)
    assert len(repo.images()) == 2

    # The "stargazers" table is still the same but the "releases" table managed to get updated.
    image = repo.images["latest"]
    assert sorted(image.get_tables()) == ["releases", "stargazers"]
    image.checkout()

    assert repo.run_sql("SELECT COUNT(1) FROM releases", return_shape=ResultShape.ONE_ONE) == 7
    assert repo.run_sql("SELECT COUNT(1) FROM stargazers", return_shape=ResultShape.ONE_ONE) == 5


def test_singer_data_source_introspect(local_engine_empty):
    source = GenericSingerDataSource(
        local_engine_empty,
        credentials={"some": "credential"},
        params={"tap_path": TEST_TAP, "other": "param"},
    )

    schema = source.introspect()
    assert schema == {"releases": (_RELEASES_SCHEMA, {}), "stargazers": (_STARGAZERS_SCHEMA, {})}


def test_singer_data_source_sync(local_engine_empty):
    source = GenericSingerDataSource(
        local_engine_empty,
        credentials={"some": "credential"},
        params={"tap_path": TEST_TAP, "other": "param"},
    )

    repo = Repository.from_schema(TEST_REPO)
    source.sync(repo, "latest")

    assert len(repo.images()) == 1
    image = repo.images["latest"]
    assert sorted(image.get_tables()) == ["_sg_ingestion_state", "releases", "stargazers"]
    image.checkout()

    assert repo.run_sql("SELECT COUNT(1) FROM releases", return_shape=ResultShape.ONE_ONE) == 6
    assert repo.run_sql("SELECT COUNT(1) FROM stargazers", return_shape=ResultShape.ONE_ONE) == 5

    assert repo.run_sql("SELECT state FROM _sg_ingestion_state")[0][0] == {
        "bookmarks": {
            "splitgraph/splitgraph": {
                "stargazers": {"since": "2020-10-14T11:06:40.852311Z"},
                "releases": {"since": "2020-10-14T11:06:40.852311Z"},
            }
        }
    }

    # Second sync
    source.sync(repo, "latest")
    assert len(repo.images()) == 1
    image = repo.images["latest"]
    assert sorted(image.get_tables()) == ["_sg_ingestion_state", "releases", "stargazers"]
    image.checkout()

    assert repo.run_sql("SELECT COUNT(1) FROM releases", return_shape=ResultShape.ONE_ONE) == 9
    assert repo.run_sql("SELECT COUNT(1) FROM stargazers", return_shape=ResultShape.ONE_ONE) == 6

    assert repo.run_sql("SELECT state FROM _sg_ingestion_state")[0][0] == {
        "bookmarks": {
            "splitgraph/splitgraph": {
                "releases": {"since": "2020-10-14T11:06:42.786589Z"},
                "stargazers": {"since": "2020-10-14T11:06:42.565793Z"},
            }
        }
    }


def _source(local_engine_empty):
    return MySQLSingerDataSource(
        engine=local_engine_empty,
        params={
            "replication_method": "INCREMENTAL",
            "host": "localhost",
            "port": 3306,
        },
        credentials={
            "user": "originuser",
            "password": "originpass",
        },
    )


@pytest.mark.skipif(shutil.which("tap-mysql") is None, reason="tap-mysql is missing in PATH")
@pytest.mark.mounting
def test_singer_tap_mysql_introspection(local_engine_empty):
    source = _source(local_engine_empty)
    assert source.introspect() == {
        "mushrooms": (
            [
                TableColumn(
                    ordinal=0,
                    name="discovery",
                    pg_type="timestamp without time zone",
                    is_pk=False,
                    comment=None,
                ),
                TableColumn(
                    ordinal=1, name="friendly", pg_type="boolean", is_pk=False, comment=None
                ),
                TableColumn(
                    ordinal=2, name="mushroom_id", pg_type="integer", is_pk=True, comment=None
                ),
                TableColumn(
                    ordinal=3, name="name", pg_type="character varying", is_pk=False, comment=None
                ),
            ],
            {},
        )
    }

    singer_config = source.get_singer_config()
    assert singer_config == {
        "host": "localhost",
        "password": "originpass",
        "port": 3306,
        "replication_method": "INCREMENTAL",
        "user": "originuser",
    }

    # Binary datatypes aren't supported by tap-singer but we make sure it's aware of them
    # (shows that they're not supported).
    singer_catalog = source._run_singer_discovery(singer_config)
    assert singer_catalog == {
        "streams": [
            {
                "metadata": mock.ANY,
                "schema": {
                    "properties": {
                        "discovery": {
                            "format": "date-time",
                            "inclusion": "available",
                            "type": ["null", "string"],
                        },
                        "friendly": {"inclusion": "available", "type": ["null", "boolean"]},
                        "mushroom_id": {
                            "inclusion": "automatic",
                            "maximum": 2147483647,
                            "minimum": -2147483648,
                            "type": ["null", "integer"],
                        },
                        "name": {
                            "inclusion": "available",
                            "maxLength": 20,
                            "type": ["null", "string"],
                        },
                        "binary_data": {
                            "description": "Unsupported column type binary(7)",
                            "inclusion": "unsupported",
                        },
                        "varbinary_data": {
                            "description": "Unsupported column type varbinary(16)",
                            "inclusion": "unsupported",
                        },
                    },
                    "type": "object",
                },
                "stream": "mushrooms",
                "table_name": "mushrooms",
                "tap_stream_id": "mysqlschema-mushrooms",
            }
        ]
    }

    assert sorted(singer_catalog["streams"][0]["metadata"], key=lambda m: m["breadcrumb"]) == [
        {
            "breadcrumb": [],
            "metadata": {
                "database-name": "mysqlschema",
                "is-view": False,
                "row-count": 2,
                "selected-by-default": False,
                "table-key-properties": ["mushroom_id"],
            },
        },
        {
            "breadcrumb": ["properties", "binary_data"],
            "metadata": {"selected-by-default": False, "sql-datatype": "binary(7)"},
        },
        {
            "breadcrumb": ["properties", "discovery"],
            "metadata": {"selected-by-default": True, "sql-datatype": "datetime"},
        },
        {
            "breadcrumb": ["properties", "friendly"],
            "metadata": {"selected-by-default": True, "sql-datatype": "tinyint(1)"},
        },
        {
            "breadcrumb": ["properties", "mushroom_id"],
            "metadata": {"selected-by-default": True, "sql-datatype": "int(11)"},
        },
        {
            "breadcrumb": ["properties", "name"],
            "metadata": {
                "selected-by-default": True,
                "sql-datatype": "varchar(20)",
            },
        },
        {
            "breadcrumb": ["properties", "varbinary_data"],
            "metadata": {"selected-by-default": False, "sql-datatype": "varbinary(16)"},
        },
    ]

    selected_catalog = source.build_singer_catalog(singer_catalog, tables=None)
    assert select_breadcrumb(selected_catalog["streams"][0], []) == {
        "database-name": "mysqlschema",
        "is-view": False,
        "replication-key": "mushroom_id",
        "replication-method": "INCREMENTAL",
        "row-count": 2,
        "selected": True,
        "selected-by-default": False,
        "table-key-properties": ["mushroom_id"],
    }


@pytest.mark.skipif(shutil.which("tap-mysql") is None, reason="tap-mysql is missing in PATH")
@pytest.mark.mounting
def test_singer_tap_mysql_sync(local_engine_empty):
    source = _source(local_engine_empty)
    repo = Repository.from_schema(TEST_REPO)

    source.sync(repo, "latest")

    assert len(repo.images()) == 1
    image = repo.images["latest"]
    assert sorted(image.get_tables()) == ["_sg_ingestion_state", "mushrooms"]
    image.checkout()

    assert repo.run_sql("SELECT * FROM mushrooms ORDER BY mushroom_id ASC") == [
        (datetime(2012, 11, 11, 8, 6, 26), True, 1, "portobello"),
        (datetime(2018, 3, 17, 8, 6, 26), False, 2, "deathcap"),
    ]
    assert repo.run_sql("SELECT state FROM _sg_ingestion_state")[0][0] == {
        "bookmarks": {
            "mysqlschema-mushrooms": {
                "replication_key": "mushroom_id",
                "replication_key_value": 2,
                "version": mock.ANY,
            }
        },
        "currently_syncing": None,
    }
    assert image.get_table("mushrooms").objects == [
        "o69e4529709af65f37f2e2f3a8290340ae7ad9ada6bca9c393a09572f12cbb3"
    ]

    # Run replication one more time -- check that we didn't add any more rows
    source.sync(repo, "latest")
    assert len(repo.images()) == 1
    image = repo.images["latest"]
    assert image.get_table("mushrooms").objects == [
        "o69e4529709af65f37f2e2f3a8290340ae7ad9ada6bca9c393a09572f12cbb3",
        # TODO: this object has the pk=2 row from the previous one repeated, a tap-mysql bug
        #  but we don't conflate these with Singer now.
        "od487f26d32a347ae4cc81a7442ef5a28615f70a9fff426991ab0d9d14bf7aa",
    ]
