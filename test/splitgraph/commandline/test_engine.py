import logging
import os
import shutil
import subprocess
from pathlib import Path, PureWindowsPath
from test.splitgraph.conftest import SG_ENGINE_PREFIX
from unittest.mock import Mock, patch, sentinel

import docker
import pytest
import requests
from click.testing import CliRunner
from splitgraph.__version__ import __version__
from splitgraph.commandline.engine import (
    _convert_source_path,
    add_engine_c,
    configure_engine_c,
    delete_engine_c,
    inject_config_into_engines,
    list_engines,
    list_engines_c,
    log_engine_c,
    start_engine_c,
    stop_engine_c,
    upgrade_engine_c,
    version_engine_c,
)
from splitgraph.config import CONFIG
from splitgraph.config.config import patch_config
from splitgraph.config.keys import DEFAULTS
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.exceptions import DockerUnavailableError

TEST_ENGINE_NAME = "test"


def _nuke_engines_and_volumes():
    # Make sure we don't have the test engine (managed by `sgr engine`) running before/after tests.
    client = docker.from_env()
    for c in client.containers.list(all=True):
        if c.name == SG_ENGINE_PREFIX + TEST_ENGINE_NAME:
            logging.info("Killing %s. Logs (100 lines): %s", c.name, c.logs(tail=1000))
            c.remove(force=True, v=True)
    for v in client.volumes.list():
        if (
            v.name == SG_ENGINE_PREFIX + TEST_ENGINE_NAME + "_data"
            or v.name == SG_ENGINE_PREFIX + TEST_ENGINE_NAME + "_metadata"
        ):
            v.remove(force=True)


@pytest.fixture()
def teardown_test_engine():
    _nuke_engines_and_volumes()
    try:
        yield
    finally:
        _nuke_engines_and_volumes()


def _get_test_engine_image():
    # Make sure to run sgr engine integration tests against the engine we built earlier in CI
    # rather than the current version on Docker Hub.
    return "%s/%s:%s" % (
        os.environ.get("DOCKER_REPO", "splitgraph"),
        os.environ.get("DOCKER_ENGINE_IMAGE", "engine"),
        os.environ.get("DOCKER_TAG", "development"),
    )


def test_commandline_engine_creation_list_stop_deletion(teardown_test_engine):
    runner = CliRunner()
    client = docker.from_env()

    # Create an engine with default password and wait for it to initialize
    result = runner.invoke(
        add_engine_c,
        [
            "--image",
            _get_test_engine_image(),
            "--no-pull",
            "--port",
            "5428",
            "--username",
            "not_sgr",
            "--no-sgconfig",
            TEST_ENGINE_NAME,
        ],
        input="notsosecure\nnotsosecure\n",
    )
    assert result.exit_code == 0

    # Connect to the engine to check that it's up
    conn_params = {
        "SG_ENGINE_HOST": "localhost",
        "SG_ENGINE_PORT": "5428",
        "SG_ENGINE_USER": "not_sgr",
        "SG_ENGINE_PWD": "notsosecure",
        "SG_ENGINE_DB_NAME": "splitgraph",
        "SG_ENGINE_POSTGRES_DB_NAME": "postgres",
        "SG_ENGINE_ADMIN_USER": "not_sgr",
        "SG_ENGINE_ADMIN_PWD": "notsosecure",
    }

    engine = PostgresEngine(name="test", conn_params=conn_params)
    assert engine.run_sql("SELECT * FROM splitgraph_meta.images") == []
    engine.close()

    # List running engines
    result = runner.invoke(list_engines_c)
    assert result.exit_code == 0
    assert TEST_ENGINE_NAME in result.stdout
    assert "running" in result.stdout

    # Check engine version
    # (we didn't put it into the .sgconfig so have to patch instead)
    with patch("splitgraph.engine.get_engine", return_value=engine):
        result = runner.invoke(version_engine_c, [TEST_ENGINE_NAME])
        assert result.exit_code == 0
        assert __version__ in result.stdout

    # Get engine logs (no --follow since we won't be able to interrupt it)
    result = runner.invoke(log_engine_c, [TEST_ENGINE_NAME])
    assert "database system is ready to accept connections" in result.stdout

    # Try deleting the engine while it's still running
    with pytest.raises(docker.errors.APIError):
        runner.invoke(delete_engine_c, ["-y", TEST_ENGINE_NAME], catch_exceptions=False)

    # Stop the engine
    result = runner.invoke(stop_engine_c, [TEST_ENGINE_NAME])
    assert result.exit_code == 0

    # Check it's not running
    for c in client.containers.list(filters={"ancestor": "splitgraph/engine"}, all=False):
        assert c.name != "splitgraph_test_engine_" + TEST_ENGINE_NAME

    result = runner.invoke(list_engines_c)
    assert TEST_ENGINE_NAME not in result.stdout

    result = runner.invoke(list_engines_c, ["-a"])
    assert TEST_ENGINE_NAME in result.stdout

    # Bring it back up
    result = runner.invoke(start_engine_c, [TEST_ENGINE_NAME])
    assert result.exit_code == 0

    # Check it's running
    result = runner.invoke(list_engines_c)
    assert result.exit_code == 0
    assert TEST_ENGINE_NAME in result.stdout
    assert "running" in result.stdout

    # Try upgrading it to the same engine version as a smoke test
    with patch("splitgraph.engine.get_engine", return_value=engine):
        # Make sure the connection is closed as the client will use this Engine reference
        # after the upgrade to initialize it.
        engine.close()

        result = runner.invoke(
            upgrade_engine_c,
            [TEST_ENGINE_NAME, "--image", _get_test_engine_image(), "--no-pull"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        assert "Upgraded engine %s to %s" % (TEST_ENGINE_NAME, __version__) in result.stdout

        # Check the engine is running and has the right version
        result = runner.invoke(list_engines_c)
        assert result.exit_code == 0
        assert TEST_ENGINE_NAME in result.stdout
        assert "running" in result.stdout

        result = runner.invoke(version_engine_c, [TEST_ENGINE_NAME])
        assert result.exit_code == 0
        assert __version__ in result.stdout

    # Force delete it
    result = runner.invoke(delete_engine_c, ["-f", "--with-volumes", TEST_ENGINE_NAME], input="y\n")
    assert result.exit_code == 0

    # Check the engine (and the volumes) are gone
    for c in client.containers.list(filters={"ancestor": "splitgraph/engine"}, all=False):
        assert c.name != "splitgraph_test_engine_" + TEST_ENGINE_NAME
    for v in client.volumes.list():
        assert not v.name.startswith("splitgraph_test_engine_" + TEST_ENGINE_NAME)


# Sample Docker SDK pull progress response (to make sure progress bars get rendered etc)
_PULL_PROGRESS = [
    {"status": "Pulling fs layer", "progressDetail": {}, "id": "66f0f2cf1825"},
    {"status": "Waiting", "progressDetail": {}, "id": "66f0f2cf1825"},
    {
        "status": "Downloading",
        "progressDetail": {"current": 49837, "total": 4865303},
        "progress": "[>                                                  ]  49.84kB/4.865MB",
        "id": "66f0f2cf1825",
    },
    {
        "status": "Extracting",
        "progressDetail": {"current": 49837, "total": 4865303},
        "progress": "[>                                                  ]  49.84kB/4.865MB",
        "id": "66f0f2cf1825",
    },
]


@pytest.mark.parametrize(
    "test_case",
    [
        # Test case is a 4-tuple: CONFIG dict (values overriding DEFAULTS),
        #   engine name, output config file name, output config file contents (prefix)
        # Also note that values in the output config file that are the same as as defaults are omitted.
        # Case 1: no source config, default engine: gets inserted as default.
        (
            {},
            "default",
            "/home/user/.splitgraph/.sgconfig",
            "[defaults]\nSG_ENGINE_PORT=6432\nSG_ENGINE_USER=not_sgr\n"
            "SG_ENGINE_PWD=pwd\nSG_ENGINE_ADMIN_USER=not_sgr\n"
            "SG_ENGINE_ADMIN_PWD=pwd\n",
        ),
        # Case 2: no source config, a different engine: gets inserted as a new remote
        (
            {},
            "secondary",
            "/home/user/.splitgraph/.sgconfig",
            "[defaults]\n\n[remote: secondary]\n"
            "SG_ENGINE_HOST=localhost\nSG_ENGINE_PORT=6432\n"
            "SG_ENGINE_FDW_HOST=localhost\nSG_ENGINE_FDW_PORT=5432\n"
            "SG_ENGINE_USER=not_sgr\nSG_ENGINE_PWD=pwd\n"
            "SG_ENGINE_DB_NAME=splitgraph\n"
            "SG_ENGINE_POSTGRES_DB_NAME=postgres\n"
            "SG_ENGINE_ADMIN_USER=not_sgr\n"
            "SG_ENGINE_ADMIN_PWD=pwd\n",
        ),
        # Case 3: have source config, default engine gets overwritten
        (
            {"SG_CONFIG_FILE": "/home/user/.sgconfig", "SG_ENGINE_PORT": "5000"},
            "default",
            "/home/user/.sgconfig",
            "[defaults]\nSG_ENGINE_PORT=6432\nSG_ENGINE_USER=not_sgr\nSG_ENGINE_PWD=pwd\n"
            "SG_ENGINE_ADMIN_USER=not_sgr\nSG_ENGINE_ADMIN_PWD=pwd\n",
        ),
        # Case 4: have source config, non-default engine gets overwritten
        (
            {
                "SG_CONFIG_FILE": "/home/user/.sgconfig",
                "SG_ENGINE_PORT": "5000",
                "remotes": {
                    "secondary": {
                        "SG_ENGINE_HOST": "old_host",
                        "SG_ENGINE_PORT": "5000",
                        "SG_ENGINE_USER": "old_user",
                        "SG_ENGINE_PWD": "old_password",
                    }
                },
            },
            "secondary",
            "/home/user/.sgconfig",
            "[defaults]\nSG_ENGINE_PORT=5000\n\n[remote: secondary]\n"
            "SG_ENGINE_HOST=localhost\nSG_ENGINE_PORT=6432\n"
            "SG_ENGINE_USER=not_sgr\nSG_ENGINE_PWD=pwd\n"
            "SG_ENGINE_FDW_HOST=localhost\nSG_ENGINE_FDW_PORT=5432\n"
            "SG_ENGINE_DB_NAME=splitgraph\nSG_ENGINE_POSTGRES_DB_NAME=postgres\n"
            "SG_ENGINE_ADMIN_USER=not_sgr\nSG_ENGINE_ADMIN_PWD=pwd\n",
        ),
    ],
)
def test_commandline_engine_creation_config_patching(test_case, fs_fast):
    runner = CliRunner()

    source_config_patch, engine_name, target_path, target_config = test_case
    source_config = patch_config(DEFAULTS, source_config_patch)

    # Patch Docker stuff out (we've exercised the actual engine creation/connections
    # in the previous test) and test that `sgr engine add` correctly inserts the
    # new engine into the config file and calls inject_config_into_engines to synch
    # up the configuration.
    env = os.environ.copy()
    env["HOME"] = "/home/user"
    Path(env["HOME"]).mkdir(exist_ok=True, parents=True)
    client = Mock()
    client.api.base_url = "tcp://localhost:3333"
    client.api.pull.return_value = _PULL_PROGRESS

    with patch("splitgraph.config.CONFIG", source_config):
        with patch("docker.from_env", return_value=client):
            with patch("splitgraph.commandline.engine.inject_config_into_engines") as ic:
                result = runner.invoke(
                    add_engine_c,
                    args=["--username", "not_sgr", "--no-init", engine_name],
                    input="pwd\npwd\n",
                    catch_exceptions=False,
                    env=env,
                )
                assert result.exit_code == 0
                print(result.output)

                ic.assert_called_once_with("splitgraph_engine_", target_path)

    assert os.path.exists(target_path)
    with open(target_path, "r") as f:
        actual_lines = f.read().strip().split("\n")
    expected_lines = target_config.strip().split("\n")
    assert actual_lines[: len(expected_lines)] == expected_lines


def test_inject_config_into_engines_unit():
    client = Mock()
    client.api.base_url = "tcp://localhost:3333"
    container_1 = Mock(name="splitgraph_engine_default")
    container_2 = Mock(name="some_other_container")

    client.containers.list.return_value = [container_1, container_2]

    with patch("docker.from_env", return_value=client):
        with patch("splitgraph.commandline.engine.copy_to_container") as ctc:
            inject_config_into_engines("splitgraph_engine_", sentinel.config_path)
            assert ctc.called_once_with(container_1, sentinel.config_path, "/.sgconfig")


def test_commandline_engine_creation_config_patching_integration(teardown_test_engine, tmp_path):
    # An end-to-end test for config patching where we actually try and access the engine
    # with the generated config.

    config_path = os.path.join(tmp_path, ".sgconfig")
    shutil.copy(os.path.join(os.path.dirname(__file__), "../../resources/.sgconfig"), config_path)
    result = subprocess.run(
        "SG_CONFIG_FILE=%s sgr engine add %s "
        "--port 5428 --image %s --no-pull "
        "--username not_sgr --password password"
        % (config_path, TEST_ENGINE_NAME, _get_test_engine_image()),
        shell=True,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    print(result.stderr.decode())
    print(result.stdout.decode())
    assert result.returncode == 0
    # Check the engine container has the test prefix (splitgraph_test_engine) and actual
    # "test" name
    assert "splitgraph_test_engine_test" in result.stdout.decode()

    # Print out the config file for easier debugging.
    with open(config_path, "r") as f:
        config = f.read()

    print(config)

    # Do some spot checks to make sure we didn't overwrite anything.
    assert "SG_S3_HOST=objectstorage" in config
    assert "postgres_fdw" in config
    assert "[remote: %s]" % TEST_ENGINE_NAME in config
    assert "[remote: remote_engine]" in config

    # Check that we can access the new engine.
    result = subprocess.run(
        "SG_CONFIG_FILE=%s SG_ENGINE=%s sgr status" % (config_path, TEST_ENGINE_NAME),
        shell=True,
        stderr=subprocess.STDOUT,
    )
    assert result.returncode == 0


def test_commandline_engine_creation_port_conflict(teardown_test_engine, tmp_path):
    # Test creating the engine when there's a port conflict exits gracefully and
    # deletes the half-initialized container.

    config_path = os.path.join(tmp_path, ".sgconfig")
    shutil.copy(os.path.join(os.path.dirname(__file__), "../../resources/.sgconfig"), config_path)

    # Run the engine on 5432: we assume our Compose stack is running, so this will trigger
    # a port conflict.
    result = subprocess.run(
        "SG_CONFIG_FILE=%s sgr engine add %s "
        "--port 5432 --image %s --no-pull "
        "--username not_sgr --password password"
        % (config_path, TEST_ENGINE_NAME, _get_test_engine_image()),
        shell=True,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    print(result.stderr.decode())
    print(result.stdout.decode())
    assert result.returncode != 0

    assert "Port 5432 is already allocated" in result.stderr.decode()

    # List Docker containers and make sure the new engine was deleted
    client = docker.from_env()
    assert SG_ENGINE_PREFIX + TEST_ENGINE_NAME not in [
        c.name for c in client.containers.list(all=True)
    ]


def test_commandline_engine_config_reinject():
    client = Mock()
    client.api.base_url = "tcp://localhost:3333"
    container_1 = Mock(name="splitgraph_engine_default")

    runner = CliRunner()

    with patch("docker.from_env", return_value=client):
        with patch(
            "splitgraph.commandline.engine.copy_to_container",
        ) as ctc:
            result = runner.invoke(configure_engine_c)
            assert result.exit_code == 0
            assert ctc.called_once_with(container_1, CONFIG["SG_CONFIG_FILE"], "/.sgconfig")


def test_convert_source_path():
    """Test source path gets converted to a location that's mounted into the Docker
    VM on Windows and allows running `sgr engine add` with `--inject-source` to bind
    mount Splitgraph source code into the engine."""

    assert _convert_source_path("/c/Users/username/splitgraph") == "/c/Users/username/splitgraph"

    # a lot of patching here because we're not actually running on Win but this is what
    # I observed happens there.
    path = PureWindowsPath("C:\\Projects\\Splitgraph")

    with patch.object(PureWindowsPath, "as_posix", return_value="C:/Projects/Splitgraph"):
        with patch("splitgraph.commandline.engine.logging") as log:
            # Check user is warned if the directory might not get bind mounted on Docker VM.
            with patch(
                "splitgraph.commandline.engine.Path",
                return_value=path,
            ):
                assert _convert_source_path("C:\\Projects\\Splitgraph") == "/c/Projects/Splitgraph"
                assert log.warning.call_count == 1


def test_list_engines_docker_unavailable():
    client = Mock()
    client.api.base_url = "tcp://localhost:3333"
    client.ping.side_effect = requests.exceptions.ConnectionError

    with patch("docker.from_env", return_value=client):
        with patch("splitgraph.commandline.engine.logging") as log:
            assert list_engines("some_prefix", unavailable_ok=True) == []
            assert log.warning.call_count == 1

        with pytest.raises(DockerUnavailableError):
            list_engines("some_prefix", unavailable_ok=False)
