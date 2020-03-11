from unittest import mock
from unittest.mock import PropertyMock, Mock, sentinel

import pytest
from click import UsageError
from click.testing import CliRunner

from splitgraph.commandline import clone_c, pull_c, push_c, reindex_c
from splitgraph.commandline.push_pull import _determine_push_target
from splitgraph.core.engine import repository_exists
from splitgraph.core.repository import Repository


@pytest.mark.parametrize(
    "repository,remote_repository,remote,available_remotes,upstream,expected_target,expected_remote",
    [
        (
            "climate",
            None,
            None,
            ["data.splitgraph.com"],
            None,
            "user/climate",
            "data.splitgraph.com",
        ),
        (
            "noaa/climate",
            None,
            None,
            ["data.splitgraph.com"],
            None,
            "user/climate",
            "data.splitgraph.com",
        ),
        (
            "noaa/climate",
            "noaa/climate",
            None,
            ["data.splitgraph.com"],
            None,
            "noaa/climate",
            "data.splitgraph.com",
        ),
        (
            "climate",
            None,
            None,
            ["data.splitgraph.com"],
            sentinel.upstream,
            "user/climate_upstream",
            "data.splitgraph.com",
        ),
        (
            "climate",
            None,
            None,
            ["data.splitgraph.com", "splitgraph.mycompany.com"],
            None,
            UsageError,
            None,
        ),
        (
            "climate",
            None,
            "splitgraph.mycompany.com",
            ["data.splitgraph.com", "splitgraph.mycompany.com"],
            None,
            "user/climate",
            "splitgraph.mycompany.com",
        ),
    ],
)
def test_push_target(
    repository,
    remote_repository,
    remote,
    available_remotes,
    upstream,
    expected_target,
    expected_remote,
):

    repository = Repository.from_schema(repository)
    remote_repository = Repository.from_schema(remote_repository) if remote_repository else None

    fake_config = {"remotes": {s: {"SG_NAMESPACE": "user"} for s in available_remotes}}

    with mock.patch.object(Repository, "upstream", new_callable=PropertyMock) as up:
        up.return_value = upstream
        with mock.patch("splitgraph.commandline.push_pull._REMOTES", available_remotes):
            with mock.patch("splitgraph.commandline.push_pull.CONFIG", fake_config):
                with mock.patch("splitgraph.engine.get_engine") as ge:
                    ge.return_value = Mock()
                    ge.return_value.name = expected_remote

                    if isinstance(expected_target, type):
                        with pytest.raises(expected_target):
                            _determine_push_target(repository, remote_repository, remote)
                    else:
                        result = _determine_push_target(repository, remote_repository, remote)
                        if upstream:
                            assert result == upstream
                        else:
                            assert result.to_schema() == expected_target

                            ge_call = ge.mock_calls[0]
                            assert ge_call[1][0] == expected_remote
                            assert result.engine.name == expected_remote


def test_pull_push(local_engine_empty, pg_repo_remote):
    runner = CliRunner()
    pg_repo_local = Repository.from_template(pg_repo_remote, engine=local_engine_empty)

    # Clone the base 0000.. image first to check single-image clones
    assert len(pg_repo_local.images()) == 0
    result = runner.invoke(clone_c, [str(pg_repo_local) + ":" + "00000000"])
    assert result.exit_code == 0
    assert len(pg_repo_local.images()) == 1
    assert repository_exists(pg_repo_local)

    # Clone the rest of the repo
    result = runner.invoke(clone_c, [str(pg_repo_local)])
    assert result.exit_code == 0
    assert len(pg_repo_local.images()) == 2

    pg_repo_remote.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    remote_engine_head = pg_repo_remote.commit()

    # Pull the new image
    result = runner.invoke(pull_c, [str(pg_repo_local) + ":" + remote_engine_head.image_hash[:10]])
    assert result.exit_code == 0
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0
    assert len(pg_repo_local.images()) == 3

    # Pull the whole repo (should be no changes)
    result = runner.invoke(pull_c, [str(pg_repo_local)])
    assert result.exit_code == 0
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0
    assert len(pg_repo_local.images()) == 3

    # Pull repo downloading everything
    result = runner.invoke(pull_c, [str(pg_repo_local), "--download-all"])
    assert result.exit_code == 0
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 3

    pg_repo_local.images.by_hash(remote_engine_head.image_hash).checkout()

    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'mustard')")
    local_head = pg_repo_local.commit()

    assert local_head.image_hash not in list(pg_repo_remote.images)

    # Push out the single new image first
    result = runner.invoke(
        push_c, [str(pg_repo_local) + ":" + local_head.image_hash[:10], "-h", "DB"]
    )
    assert result.exit_code == 0
    assert len(pg_repo_remote.images()) == 4

    # Push out the whole repo
    result = runner.invoke(push_c, [str(pg_repo_local), "-h", "DB"])
    assert result.exit_code == 0
    assert pg_repo_local.head.get_table("fruits")


def test_reindex_and_force_push(pg_repo_local, pg_repo_remote):
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(clone_c, [str(pg_repo_local)])
    assert result.exit_code == 0
    assert repository_exists(pg_repo_local)

    result = runner.invoke(
        reindex_c,
        [str(pg_repo_local) + ":latest", "fruits", '-i {"bloom": {"name": {"probability": 0.01}}}'],
    )
    assert result.exit_code == 0
    assert "Reindexed 1 object(s)" in result.output

    result = runner.invoke(push_c, [str(pg_repo_local), "-f"], catch_exceptions=False)
    assert result.exit_code == 0

    obj = pg_repo_remote.images["latest"].get_table("fruits").objects[0]
    assert "bloom" in pg_repo_remote.objects.get_object_meta([obj])[obj].object_index
