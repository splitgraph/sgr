import json
import os
from datetime import datetime
from decimal import Decimal

from click.testing import CliRunner
import psycopg2

from splitgraph.core.repository import Repository
from splitgraph.core.types import TableColumn
from splitgraph.engine import ResultShape
from splitgraph.ingestion.singer import singer_target
from test.splitgraph.conftest import INGESTION_RESOURCES

TEST_REPO = "test/singer"

_STARGAZERS_SCHEMA = [
    TableColumn(
        ordinal=0, name="_sdc_repository", pg_type="character varying", is_pk=False, comment=None,
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
        ordinal=0, name="_sdc_repository", pg_type="character varying", is_pk=False, comment=None,
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
        ordinal=11, name="target_commitish", pg_type="character varying", is_pk=False, comment=None,
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
        "o160e0b0db4ad7e7eb7c4db26bf8183461f65968be64b8594c7cc71fbf5ff2a"
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
        "o160e0b0db4ad7e7eb7c4db26bf8183461f65968be64b8594c7cc71fbf5ff2a",
        "ocf91fc59f89f9db3db9aea28c4719f8bd009b13990f3f12f93f282618d81a8",
    ]

    assert image.get_table("stargazers").table_schema == _STARGAZERS_SCHEMA
    # Extra DIFF at the end
    assert image.get_table("stargazers").objects == [
        "od68e932ebc99c1a337363c1b92056dcf7fc7c6c45494bc42e1e1ec4e0c88ac",
        "oc61804b31dcae8294a6b780efe41601eaeb7a1d0b7cd7bdfea4843db214df0",
    ]

    assert repo.run_sql(
        "SELECT sg_ud_flag, user_id, starred_at "
        "FROM splitgraph_meta.oc61804b31dcae8294a6b780efe41601eaeb7a1d0b7cd7bdfea4843db214df0 "
        "ORDER BY user_id"
    ) == [
        (True, Decimal("100004"), datetime(2020, 10, 11, 21, 9, 30)),
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
            "splitgraph/splitgraph": {"stargazers": {"since": "2020-10-14T11:06:42.565793Z"},}
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
        "o160e0b0db4ad7e7eb7c4db26bf8183461f65968be64b8594c7cc71fbf5ff2a"
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
