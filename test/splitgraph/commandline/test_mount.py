import json
import os
import tempfile
from decimal import Decimal
from test.splitgraph.conftest import INGESTION_RESOURCES, MG_MNT, OUTPUT
from unittest import mock

import pytest
from click.testing import CliRunner
from splitgraph.commandline import cleanup_c, import_c, init_c, mount_c, rm_c, status_c
from splitgraph.core.engine import repository_exists
from splitgraph.core.repository import Repository
from splitgraph.hooks.data_source import _load_source, get_data_source, get_data_sources
from splitgraph.hooks.data_source.fdw import PostgreSQLDataSource
from splitgraph.ingestion.socrata.mount import SocrataDataSource

_MONGO_PARAMS = {
    "tables": {
        "stuff": {
            "options": {
                "database": "origindb",
                "collection": "stuff",
            },
            "schema": {
                "name": "text",
                "duration": "numeric",
                "happy": "boolean",
            },
        }
    }
}


@pytest.mark.mounting
def test_misc_mountpoint_management(pg_repo_local, mg_repo_local):
    runner = CliRunner()

    result = runner.invoke(status_c)
    assert str(pg_repo_local) in result.output
    assert str(mg_repo_local) in result.output

    # sgr rm -y test/pg_mount (no prompting)
    result = runner.invoke(rm_c, [str(mg_repo_local), "-y"])
    assert result.exit_code == 0
    assert not repository_exists(mg_repo_local)

    # sgr cleanup
    result = runner.invoke(cleanup_c)
    assert "Deleted 1 object" in result.output

    # sgr init
    result = runner.invoke(init_c, ["output"])
    assert "Initialized empty repository output" in result.output
    assert repository_exists(OUTPUT)

    # sgr mount with a file
    with tempfile.NamedTemporaryFile("w") as f:
        json.dump(
            _MONGO_PARAMS,
            f,
        )
        f.flush()

        result = runner.invoke(
            mount_c,
            [
                "mongo_fdw",
                str(mg_repo_local),
                "-c",
                "originro:originpass@mongoorigin:27017",
                "-o",
                "@" + f.name,
            ],
        )
    assert result.exit_code == 0
    assert mg_repo_local.run_sql("SELECT duration from stuff WHERE name = 'James'") == [
        (Decimal(2),)
    ]


@pytest.mark.mounting
def test_mount_and_import(local_engine_empty):
    runner = CliRunner()
    try:
        # sgr mount
        result = runner.invoke(
            mount_c,
            [
                "mongo_fdw",
                "tmp",
                "-c",
                "originro:originpass@mongoorigin:27017",
                "-o",
                json.dumps(_MONGO_PARAMS),
            ],
        )
        assert result.exit_code == 0

        result = runner.invoke(import_c, ["tmp", "stuff", str(MG_MNT)])
        assert result.exit_code == 0
        assert MG_MNT.head.get_table("stuff")

        result = runner.invoke(
            import_c, ["tmp", "SELECT * FROM stuff WHERE duration > 10", str(MG_MNT), "stuff_query"]
        )
        assert result.exit_code == 0
        assert MG_MNT.head.get_table("stuff_query")
    finally:
        Repository("", "tmp").delete()


def test_mount_docstring_generation():
    runner = CliRunner()

    # General mount help: should have all the handlers autoregistered and listed
    result = runner.invoke(mount_c, ["--help"])
    assert result.exit_code == 0
    for handler_name in get_data_sources():
        assert handler_name in result.output

    # Test the reserved params (that we parse separately) don't make it into the help text
    # and that other function args from the docstring do.
    result = runner.invoke(mount_c, ["postgres_fdw", "--help"])
    assert result.exit_code == 0
    assert "mountpoint" not in result.output
    assert "remote_schema" in result.output


def test_mount_fallback(local_engine_empty):
    # Test that the handlers in old config files (that referred to functions) still load
    # as classes (the default overrides them in this case and emits a warning).

    assert (
        _load_source("postgres_fdw", "splitgraph.hooks.mount_handlers.mount_postgres")
        == PostgreSQLDataSource
    )

    assert (
        _load_source("socrata", "splitgraph.ingestion.socrata.mount.mount_socrata")
        == SocrataDataSource
    )


def test_mount_plugin_dir():
    with mock.patch(
        "splitgraph.hooks.data_source.get_singleton",
        return_value=os.path.join(INGESTION_RESOURCES, "../custom_plugin_dir"),
    ):
        plugin_class = get_data_source("some_plugin")

    plugin = plugin_class(
        engine=None, credentials={"access_token": "abc"}, params={"some_field": "some_value"}
    )
    assert plugin.introspect() == {"some_table": ([], {})}
    assert plugin.get_name() == "Test Data Source"
