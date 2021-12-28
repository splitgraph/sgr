from test.splitgraph.conftest import OUTPUT, RESOURCES
from unittest import mock
from unittest.mock import call

from click.testing import CliRunner

from splitgraph.commandline import build_c, dependents_c, provenance_c, rebuild_c
from splitgraph.core.repository import Repository


def test_splitfile_default():
    runner = CliRunner()

    with mock.patch("splitgraph.splitfile.execution.execute_commands") as ec:
        runner.invoke(
            build_c,
            [RESOURCES + "import_remote_multiple.splitfile", "-a", "TAG", "latest"],
        )
    assert ec.mock_calls == [
        call(mock.ANY, {"TAG": "latest"}, output=Repository("", "import_remote_multiple"))
    ]


def test_splitfile(local_engine_empty, pg_repo_remote):
    runner = CliRunner()

    result = runner.invoke(
        build_c,
        [
            RESOURCES + "import_remote_multiple.splitfile",
            "-a",
            "TAG",
            "latest",
            "-o",
            "output",
        ],
    )
    assert result.exit_code == 0
    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM join_table") == [
        (1, "apple", "potato"),
        (2, "orange", "carrot"),
    ]

    # Test the sgr provenance command. First, just list the dependencies of the new image.
    result = runner.invoke(provenance_c, ["output:latest"])
    assert "test/pg_mount:%s" % pg_repo_remote.images["latest"].image_hash in result.output

    # Second, output the full splitfile (-f)
    result = runner.invoke(provenance_c, ["output:latest", "-f"])
    assert (
        "FROM test/pg_mount:%s IMPORT" % pg_repo_remote.images["latest"].image_hash in result.output
    )
    assert "SQL {CREATE TABLE join_table" in result.output

    # Test reverse dependencies
    # We're looking at test/pg_mount on the local engine which doesn't exist -- this should fail.
    result = runner.invoke(
        dependents_c,
        [
            "test/pg_mount:%s" % pg_repo_remote.images["latest"].image_hash,
        ],
    )
    assert result.exit_code == 1

    # Now look at test/pg_mount on the remote and look for dependents on the local engine.
    result = runner.invoke(
        dependents_c,
        [
            "test/pg_mount:%s" % pg_repo_remote.images["latest"].image_hash,
            "--dependents-on",
            "LOCAL",
            "--source-on",
            pg_repo_remote.engine.name,
        ],
    )
    assert result.exit_code == 0
    assert "is depended on by" in result.output
    assert "%s:%s" % (OUTPUT, OUTPUT.head.image_hash) in result.output


def test_splitfile_rebuild_update(local_engine_empty, pg_repo_remote_multitag):
    runner = CliRunner()

    result = runner.invoke(
        build_c,
        [RESOURCES + "import_remote_multiple.splitfile", "-a", "TAG", "v1", "-o", "output"],
    )
    assert result.exit_code == 0

    # Rerun the output:latest against v2 of the test/pg_mount
    result = runner.invoke(rebuild_c, ["output:latest", "--against", "test/pg_mount:v2"])
    output_v2 = OUTPUT.head
    assert result.exit_code == 0
    v2 = pg_repo_remote_multitag.images["v2"]
    assert output_v2.provenance() == [(pg_repo_remote_multitag, v2.image_hash)]

    # Now rerun the output:latest against the latest version of everything.
    # In this case, this should all resolve to the same version of test/pg_mount (v2) and not produce
    # any extra commits.
    curr_commits = OUTPUT.images()
    result = runner.invoke(rebuild_c, ["output:latest", "-u"])
    assert result.exit_code == 0
    assert output_v2 == OUTPUT.head
    assert OUTPUT.images() == curr_commits
