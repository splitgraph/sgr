import json
import tempfile
from decimal import Decimal

import pytest
from click.testing import CliRunner
from test.splitgraph.conftest import OUTPUT, MG_MNT

from splitgraph.commandline import status_c, rm_c, cleanup_c, init_c, mount_c, import_c
from splitgraph.core.engine import repository_exists
from splitgraph.core.repository import Repository
from splitgraph.hooks.mount_handlers import get_mount_handlers

_MONGO_PARAMS = {
    "tables": {
        "stuff": {
            "options": {"db": "origindb", "coll": "stuff",},
            "schema": {"name": "text", "duration": "numeric", "happy": "boolean",},
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
            _MONGO_PARAMS, f,
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
                # TODO figure out a way to keep the more lightweight UX for mongo/pg?
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
    for handler_name in get_mount_handlers():
        assert handler_name in result.output

    # Test the reserved params (that we parse separately) don't make it into the help text
    # and that other function args from the docstring do.
    result = runner.invoke(mount_c, ["postgres_fdw", "--help"])
    assert result.exit_code == 0
    assert "mountpoint" not in result.output
    assert "remote_schema" in result.output
