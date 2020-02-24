import logging
import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch, sentinel

import docker
import pytest
from click.testing import CliRunner
from test.splitgraph.conftest import SG_ENGINE_PREFIX

from splitgraph.commandline.engine import (
    add_engine_c,
    list_engines_c,
    delete_engine_c,
    stop_engine_c,
    start_engine_c,
    inject_config_into_engines,
    configure_engine_c,
)
from splitgraph.config import CONFIG
from splitgraph.config.config import patch_config
from splitgraph.config.keys import DEFAULTS
from splitgraph.engine.postgres.engine import PostgresEngine

TEST_ENGINE_NAME = "test"

# Default parameters for data.splitgraph.com that show up in every config
_CONFIG_DEFAULTS = (
    "\n[remote: data.splitgraph.com]\nSG_ENGINE_HOST=data.splitgraph.com\n"
    "SG_ENGINE_PORT=5432\nSG_ENGINE_DB_NAME=sgregistry\n"
    "SG_AUTH_API=https://api.splitgraph.com/auth\n"
    "SG_QUERY_API=https://data.splitgraph.com\n"
)


def _nuke_engines_and_volumes():
    # Make sure we don't have the test engine (managed by `sgr engine`) running before/after tests.
    client = docker.from_env()
    for c in client.containers.list(filters={"ancestor": "splitgraph/engine"}, all=True):
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

    # Force delete it
    result = runner.invoke(delete_engine_c, ["-f", "--with-volumes", TEST_ENGINE_NAME], input="y\n")
    assert result.exit_code == 0

    # Check the engine (and the volumes) are gone
    for c in client.containers.list(filters={"ancestor": "splitgraph/engine"}, all=False):
        assert c.name != "splitgraph_test_engine_" + TEST_ENGINE_NAME
    for v in client.volumes.list():
        assert not v.name.startswith("splitgraph_test_engine_" + TEST_ENGINE_NAME)


@pytest.mark.parametrize(
    "test_case",
    [
        # Test case is a 4-tuple: CONFIG dict (values overriding DEFAULTS),
        #   engine name, output config file name, output config file contents
        # Also note that values in the output config file that are the same as as defaults are omitted.
        # Case 1: no source config, default engine: gets inserted as default.
        (
            {},
            "default",
            "/home/user/.splitgraph/.sgconfig",
            "[defaults]\nSG_ENGINE_USER=not_sgr\n"
            "SG_ENGINE_PWD=pwd\nSG_ENGINE_ADMIN_USER=not_sgr\n"
            "SG_ENGINE_ADMIN_PWD=pwd\n"
            + _CONFIG_DEFAULTS
            + "[external_handlers]\nS3=splitgraph.hooks.s3.S3ExternalObjectHandler\n",
        ),
        # Case 2: no source config, a different engine: gets inserted as a new remote
        (
            {},
            "secondary",
            "/home/user/.splitgraph/.sgconfig",
            "[defaults]\n" + _CONFIG_DEFAULTS + "\n[remote: secondary]\n"
            "SG_ENGINE_HOST=localhost\nSG_ENGINE_PORT=5432\n"
            "SG_ENGINE_FDW_HOST=localhost\nSG_ENGINE_FDW_PORT=5432\n"
            "SG_ENGINE_USER=not_sgr\nSG_ENGINE_PWD=pwd\n"
            "SG_ENGINE_DB_NAME=splitgraph\n"
            "SG_ENGINE_POSTGRES_DB_NAME=postgres\n"
            "SG_ENGINE_ADMIN_USER=not_sgr\n"
            "SG_ENGINE_ADMIN_PWD=pwd\n"
            "[external_handlers]\nS3=splitgraph.hooks.s3.S3ExternalObjectHandler\n",
        ),
        # Case 3: have source config, default engine gets overwritten
        (
            {"SG_CONFIG_FILE": "/home/user/.sgconfig", "SG_ENGINE_PORT": "5000"},
            "default",
            "/home/user/.sgconfig",
            "[defaults]\nSG_ENGINE_USER=not_sgr\nSG_ENGINE_PWD=pwd\n"
            "SG_ENGINE_ADMIN_USER=not_sgr\nSG_ENGINE_ADMIN_PWD=pwd\n"
            + _CONFIG_DEFAULTS
            + "[external_handlers]\nS3=splitgraph.hooks.s3.S3ExternalObjectHandler\n",
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
            "[defaults]\nSG_ENGINE_PORT=5000\n" + _CONFIG_DEFAULTS + "\n[remote: secondary]\n"
            "SG_ENGINE_HOST=localhost\nSG_ENGINE_PORT=5432\n"
            "SG_ENGINE_USER=not_sgr\nSG_ENGINE_PWD=pwd\n"
            "SG_ENGINE_FDW_HOST=localhost\nSG_ENGINE_FDW_PORT=5432\n"
            "SG_ENGINE_DB_NAME=splitgraph\nSG_ENGINE_POSTGRES_DB_NAME=postgres\n"
            "SG_ENGINE_ADMIN_USER=not_sgr\nSG_ENGINE_ADMIN_PWD=pwd\n"
            "[external_handlers]\nS3=splitgraph.hooks.s3.S3ExternalObjectHandler\n",
        ),
    ],
)
def test_commandline_engine_creation_config_patching(test_case, fs):
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
        actual_lines = f.read().split("\n")
    expected_lines = target_config.split("\n")
    assert expected_lines == actual_lines


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
        "SG_CONFIG_FILE=%s sgr engine add %s --port 5428 --username not_sgr --password password"
        % (config_path, TEST_ENGINE_NAME),
        shell=True,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    print(result.stderr.decode())
    print(result.stdout.decode())
    assert result.returncode == 0
    assert "Updating the existing config file" in result.stdout.decode()
    # Check the engine container has the test prefix (splitgraph_test_engine) and actual
    # "test" name
    assert "splitgraph_test_engine_test" in result.stdout.decode()

    # Print out the config file for easier debugging.
    with open(config_path, "r") as f:
        config = f.read()

    print(config)

    # Do some spot checks to make sure we didn't overwrite anything.
    assert "SG_S3_HOST=//objectstorage" in config
    assert "POSTGRES_FDW=splitgraph.hooks.mount_handlers.mount_postgres" in config
    assert "[remote: %s]" % TEST_ENGINE_NAME in config
    assert "[remote: remote_engine]" in config

    # Check that we can access the new engine.
    result = subprocess.run(
        "SG_CONFIG_FILE=%s SG_ENGINE=%s sgr status" % (config_path, TEST_ENGINE_NAME),
        shell=True,
        stderr=subprocess.STDOUT,
    )
    assert result.returncode == 0


def test_commandline_engine_config_reinject():
    client = Mock()
    client.api.base_url = "tcp://localhost:3333"
    container_1 = Mock(name="splitgraph_engine_default")

    runner = CliRunner()

    with patch("docker.from_env", return_value=client):
        with patch("splitgraph.commandline.engine.copy_to_container",) as ctc:
            result = runner.invoke(configure_engine_c)
            assert result.exit_code == 0
            assert ctc.called_once_with(container_1, CONFIG["SG_CONFIG_FILE"], "/.sgconfig")
