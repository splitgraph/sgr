import json
import os
from contextlib import contextmanager
from test.splitgraph.commandline.http_fixtures import (
    ACCESS_TOKEN,
    API_KEY,
    API_SECRET,
    AUTH_ENDPOINT,
    GQL_ENDPOINT,
    REFRESH_TOKEN,
    REMOTE,
    REMOTE_CONFIG,
    access_token,
    create_credentials,
    gql_plugins_callback,
    refresh_token,
    register_user,
    tos,
)
from typing import Optional
from unittest.mock import Mock, PropertyMock, call, patch

import httpretty
import pytest
from click.testing import CliRunner
from httpretty.core import HTTPrettyRequest
from splitgraph.__version__ import __version__
from splitgraph.commandline import cli
from splitgraph.commandline.cloud import (
    add_c,
    curl_c,
    login_api_c,
    login_c,
    plugins_c,
    register_c,
    sql_c,
)
from splitgraph.config import create_config_dict
from splitgraph.config.config import patch_config
from splitgraph.config.config_file_config import get_config_dict_from_config_file
from splitgraph.config.keys import DEFAULTS
from splitgraph.exceptions import AuthAPIError


@httpretty.activate(allow_net_connect=False)
def test_commandline_registration_normal():
    httpretty.register_uri(
        httpretty.HTTPretty.POST, AUTH_ENDPOINT + "/register_user", body=register_user
    )

    httpretty.register_uri(httpretty.HTTPretty.GET, AUTH_ENDPOINT + "/tos", body=tos)

    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        AUTH_ENDPOINT + "/create_machine_credentials",
        body=create_credentials,
    )

    source_config = _make_dummy_config_dict()

    runner = CliRunner()

    with patch("splitgraph.commandline.cloud.patch_and_save_config") as pc, patch(
        "splitgraph.config.CONFIG", source_config
    ), patch("splitgraph.cloud.create_config_dict", return_value=source_config), patch(
        "splitgraph.commandline.cloud.inject_config_into_engines"
    ) as ic:
        pc.return_value = source_config["SG_CONFIG_FILE"]
        # First don't agree to ToS, then agree
        args = [
            "--username",
            "someuser",
            "--password",
            "somepassword",
            "--email",
            "someuser@example.com",
            "--remote",
            REMOTE,
        ]
        result = runner.invoke(
            register_c,
            args=args,
            catch_exceptions=False,
            input="n",
        )
        assert result.exit_code == 1
        assert "Sample ToS message" in result.output

        result = runner.invoke(
            register_c,
            args=args,
            catch_exceptions=False,
            input="y",
        )
        assert result.exit_code == 0
        assert "Sample ToS message" in result.output
    assert pc.mock_calls == [
        call(
            source_config,
            {
                "SG_REPO_LOOKUP": "remote_engine",
                "remotes": {"remote_engine": REMOTE_CONFIG},
            },
        )
    ]

    assert ic.mock_calls == [call("splitgraph_test_engine_", source_config["SG_CONFIG_FILE"])]


@httpretty.activate(allow_net_connect=False)
def test_commandline_registration_user_error():
    # Test a user error response propagates back to the command line client
    # (tests for handling other failure states are API client-specific).

    def register_callback(request, uri, response_headers):
        return [403, response_headers, json.dumps({"error": "Username exists"})]

    httpretty.register_uri(httpretty.HTTPretty.GET, AUTH_ENDPOINT + "/tos", body=tos)

    httpretty.register_uri(
        httpretty.HTTPretty.POST, AUTH_ENDPOINT + "/register_user", body=register_callback
    )

    runner = CliRunner()
    result = runner.invoke(
        register_c,
        args=[
            "--username",
            "someuser",
            "--password",
            "somepassword",
            "--email",
            "someuser@example.com",
            "--remote",
            REMOTE,
            "--accept-tos",
        ],
    )
    print(result.output)
    assert result.exit_code == 1
    assert isinstance(result.exception, AuthAPIError)
    assert "Username exists" in str(result.exception)


@contextmanager
def _patch_login_funcs(source_config):
    with patch("splitgraph.commandline.cloud.patch_and_save_config") as pc:
        with patch("splitgraph.commandline.cloud.inject_config_into_engines") as ic:
            with patch("splitgraph.config.CONFIG", source_config):
                pc.return_value = source_config["SG_CONFIG_FILE"]
                yield pc, ic


@contextmanager
def _patch_config_funcs(config):
    with patch("splitgraph.cloud.create_config_dict", return_value=config):
        with patch("splitgraph.cloud.patch_and_save_config") as oc:
            with patch("splitgraph.config.CONFIG", config):
                yield oc


@httpretty.activate(allow_net_connect=False)
def test_commandline_login_normal():
    httpretty.register_uri(
        httpretty.HTTPretty.POST, AUTH_ENDPOINT + "/refresh_token", body=refresh_token
    )

    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        AUTH_ENDPOINT + "/create_machine_credentials",
        body=create_credentials,
    )

    source_config = _make_dummy_config_dict()

    runner = CliRunner()
    with _patch_login_funcs(source_config) as (pc, ic):
        result = runner.invoke(
            login_c,
            args=[
                "--username",
                "someuser",
                "--password",
                "somepassword",
                "--remote",
                REMOTE,
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        print(result.output)

    assert pc.mock_calls == [
        call(
            source_config,
            {
                "SG_REPO_LOOKUP": REMOTE,
                "remotes": {
                    "remote_engine": {
                        "SG_NAMESPACE": "someuser",
                        "SG_CLOUD_REFRESH_TOKEN": REFRESH_TOKEN,
                        "SG_CLOUD_ACCESS_TOKEN": ACCESS_TOKEN,
                    }
                },
            },
        )
    ]

    assert ic.mock_calls == [call("splitgraph_test_engine_", source_config["SG_CONFIG_FILE"])]

    # Do the same overwriting the current API keys
    with _patch_login_funcs(source_config) as (pc, ic):
        result = runner.invoke(
            login_c,
            args=[
                "--username",
                "someuser",
                "--password",
                "somepassword",
                "--remote",
                REMOTE,
                "--overwrite",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

    assert pc.mock_calls == [
        call(
            source_config,
            {
                "SG_REPO_LOOKUP": REMOTE,
                "remotes": {
                    "remote_engine": {
                        "SG_ENGINE_USER": API_KEY,
                        "SG_ENGINE_PWD": API_SECRET,
                        "SG_NAMESPACE": "someuser",
                        "SG_CLOUD_REFRESH_TOKEN": REFRESH_TOKEN,
                        "SG_CLOUD_ACCESS_TOKEN": ACCESS_TOKEN,
                    }
                },
            },
        )
    ]

    assert ic.mock_calls == [call("splitgraph_test_engine_", source_config["SG_CONFIG_FILE"])]

    # Do the same changing the user -- check new API keys are still acquired
    source_config["remotes"][REMOTE]["SG_NAMESPACE"] = "someotheruser"
    with _patch_login_funcs(source_config) as (pc, ic):
        result = runner.invoke(
            login_c,
            args=[
                "--username",
                "someuser",
                "--password",
                "somepassword",
                "--remote",
                REMOTE,
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

    assert pc.mock_calls == [
        call(
            source_config,
            {
                "SG_REPO_LOOKUP": REMOTE,
                "remotes": {
                    "remote_engine": {
                        "SG_ENGINE_USER": API_KEY,
                        "SG_ENGINE_PWD": API_SECRET,
                        "SG_NAMESPACE": "someuser",
                        "SG_CLOUD_REFRESH_TOKEN": REFRESH_TOKEN,
                        "SG_CLOUD_ACCESS_TOKEN": ACCESS_TOKEN,
                    }
                },
            },
        )
    ]


@httpretty.activate(allow_net_connect=False)
def test_commandline_login_api():
    httpretty.register_uri(
        httpretty.HTTPretty.POST, AUTH_ENDPOINT + "/access_token", body=access_token
    )

    source_config = _make_dummy_config_dict()

    runner = CliRunner()
    with _patch_login_funcs(source_config) as (pc, ic):
        result = runner.invoke(
            login_api_c,
            args=[
                "--api-key",
                API_KEY,
                "--api-secret",
                API_SECRET,
                "--remote",
                REMOTE,
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        print(result.output)

    pc.assert_called_once_with(
        source_config,
        {
            "SG_REPO_LOOKUP": REMOTE,
            "remotes": {
                "remote_engine": {
                    "SG_NAMESPACE": "someuser",
                    "SG_CLOUD_ACCESS_TOKEN": ACCESS_TOKEN,
                    "SG_ENGINE_USER": API_KEY,
                    "SG_ENGINE_PWD": API_SECRET,
                }
            },
        },
    )
    ic.assert_called_once_with("splitgraph_test_engine_", source_config["SG_CONFIG_FILE"])


@httpretty.activate(allow_net_connect=False)
def test_commandline_login_password_prompt():
    httpretty.register_uri(
        httpretty.HTTPretty.POST, AUTH_ENDPOINT + "/refresh_token", body=refresh_token
    )

    source_config = _make_dummy_config_dict()

    runner = CliRunner()
    with _patch_login_funcs(source_config) as (pc, ic):
        result = runner.invoke(
            login_c,
            args=["--username", "someuser", "--remote", REMOTE],
            input="somepassword\n",
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        # Check the password prompt contains a hint on how to actually obtain it.
        assert (
            "Password (visit http://www.example.com/settings/security if you don't have it)"
            in result.output
        )


def _make_dummy_config_dict():
    # Sanitize the test config so that there isn't a ton of spam
    source_config = create_config_dict()
    source_config["SG_CONFIG_FILE"] = ".sgconfig"
    source_config["remotes"] = {REMOTE: source_config["remotes"][REMOTE]}
    del source_config["data_sources"]
    del source_config["commands"]
    del source_config["external_handlers"]
    return source_config


@pytest.mark.parametrize(
    "test_case",
    [
        (
            ["ns/repo", "table?id=eq.5"],
            "http://some-query-service.example.com/ns/repo/latest/-/rest/table?id=eq.5",
            [],
        ),
        (
            ["ns/repo:image", "/table?id=eq.5"],
            "http://some-query-service.example.com/ns/repo/image/-/rest/table?id=eq.5",
            [],
        ),
        (
            ["ns/repo"],
            "http://some-query-service.example.com/ns/repo/latest/-/rest",
            [],
        ),
        (
            [
                "ns/repo",
                "-t",
                "splitfile",
                '{"command": "FROM some/repo IMPORT some_table AS alias", "tag": "new_tag"}',
            ],
            "http://some-query-service.example.com/ns/repo/latest/-/splitfile",
            [
                "-H",
                "Content-Type: application/json",
                "-X",
                "POST",
                "-d",
                '{"command": "FROM some/repo IMPORT some_table AS alias", "tag": "new_tag"}',
            ],
        ),
    ],
)
def test_commandline_curl(test_case):
    runner = CliRunner()
    args, result_url, extra_curl_args = test_case

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value="AAAABBBBCCCCDDDD",
    ):
        with patch("splitgraph.commandline.cloud.subprocess.call") as sc:
            result = runner.invoke(
                curl_c,
                ["--remote", REMOTE] + args + ["-c", "--some-curl-arg", "-c", "-Ssl"],
                catch_exceptions=False,
            )
            assert result.exit_code == 0
            sc.assert_called_once_with(
                [
                    "curl",
                    result_url,
                    "-H",
                    "User-Agent: sgr %s" % __version__,
                    "-H",
                    "Authorization: Bearer AAAABBBBCCCCDDDD",
                ]
                + extra_curl_args
                + [
                    "--some-curl-arg",
                    "-Ssl",
                ]
            )


def test_commandline_cloud_sql():
    runner = CliRunner()

    fake_engine = Mock()

    remote_config = REMOTE_CONFIG.copy()
    remote_config.update(
        {
            "SG_ENGINE_HOST": "data.example.com",
            "SG_ENGINE_PORT": "5432",
            "SG_ENGINE_DB_NAME": "sgregistry",
        }
    )
    fake_engine.conn_params = remote_config

    fake_ddn_engine = Mock()
    fake_ddn_engine.run_sql.return_value = [("one", "two"), ("three", "four")]

    with patch("splitgraph.engine.get_engine", return_value=fake_engine):
        with patch(
            "splitgraph.engine.postgres.engine.PostgresEngine", return_value=fake_ddn_engine
        ):
            result = runner.invoke(sql_c)
            assert result.exit_code == 0
            assert (
                result.output == f"postgresql://{API_KEY}:{API_SECRET}@data.example.com:5432/ddn\n"
            )

            result = runner.invoke(sql_c, ["SELECT 1"], catch_exceptions=False)
            assert result.exit_code == 0
            assert fake_ddn_engine.run_sql.mock_calls == [call("SELECT 1")]
            assert "three" in result.output


@httpretty.activate(allow_net_connect=False)
def test_commandline_update_check():
    runner = CliRunner()

    last_request: Optional[HTTPrettyRequest] = None

    def _version_callback(request, uri, response_headers):
        nonlocal last_request
        last_request = request
        return [200, response_headers, json.dumps({"latest_version": "99999.42.15"})]

    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        AUTH_ENDPOINT + "/update_check",
        body=_version_callback,
    )

    config = _make_dummy_config_dict()
    config["SG_UPDATE_FREQUENCY"] = "15"
    config["SG_UPDATE_REMOTE"] = REMOTE

    with _patch_config_funcs(config) as oc:
        # Invoke cli here to call the wrapper around the actual entrypoint.
        result = runner.invoke(cli, ["config"], catch_exceptions=False)
        assert result.exit_code == 0
        assert "version 99999.42.15 is available" in result.output
        assert oc.call_count == 1
        new_config = oc.mock_calls[0][1][1]
        assert new_config["SG_UPDATE_LAST"] != "0"
        assert last_request is not None

    new_config = patch_config(config, new_config)
    # Check not running the version check every time
    with _patch_config_funcs(new_config) as oc:
        last_request = None
        result = runner.invoke(cli, ["config"], catch_exceptions=False)
        assert result.exit_code == 0
        assert "version 99999.42.15 is available" not in result.output
        assert oc.mock_calls == []
        assert last_request is None

    # Check passing access token to the version check
    new_config["remotes"][REMOTE] = REMOTE_CONFIG
    new_config["SG_UPDATE_LAST"] = "0"

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ):
        with _patch_config_funcs(new_config) as oc:
            last_request = None
            result = runner.invoke(cli, ["config"], catch_exceptions=False)
            assert result.exit_code == 0
            assert "version 99999.42.15 is available" in result.output
            assert oc.call_count == 1
            assert last_request is not None
            assert last_request.headers["Authorization"] == "Bearer " + ACCESS_TOKEN

        # Check not passing API key if SG_UPDATE_ANONYMOUS is set
        # Set SG_UPDATE_LAST back to 0 since the code mutates it
        new_config["SG_UPDATE_ANONYMOUS"] = "true"
        new_config["SG_UPDATE_LAST"] = "0"

        with _patch_config_funcs(new_config) as oc:
            last_request = None
            result = runner.invoke(cli, ["config"], catch_exceptions=False)
            assert result.exit_code == 0
            assert "version 99999.42.15 is available" in result.output
            assert oc.call_count == 1
            assert last_request is not None
            assert "Authorization" not in last_request.headers


def test_commandline_cloud_add(fs_fast):
    runner = CliRunner()

    env = os.environ.copy()
    env["HOME"] = "/home/user"
    target_path = "/home/user/.splitgraph/.sgconfig"

    with patch("splitgraph.config.CONFIG", DEFAULTS):
        result = runner.invoke(
            add_c,
            args=["democompany.splitgraph.io", "--skip-inject"],
            catch_exceptions=False,
            env=env,
        )
    assert result.exit_code == 0
    print(result.output)

    assert os.path.exists(target_path)
    actual_config = get_config_dict_from_config_file(target_path)
    assert actual_config["remotes"]["democompany"] == {
        "SG_AUTH_API": "https://api.democompany.splitgraph.io/auth",
        "SG_ENGINE_DB_NAME": "sgregistry",
        "SG_ENGINE_HOST": "data.democompany.splitgraph.io",
        "SG_ENGINE_PORT": "5432",
        "SG_GQL_API": "https://api.democompany.splitgraph.io/gql/cloud/unified/graphql",
        "SG_IS_REGISTRY": "true",
        "SG_QUERY_API": "https://data.democompany.splitgraph.io",
    }


@httpretty.activate(allow_net_connect=False)
def test_commandline_plugins():
    runner = CliRunner(mix_stderr=False)
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        GQL_ENDPOINT + "/",
        body=gql_plugins_callback,
    )

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ), patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
        result = runner.invoke(
            plugins_c,
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        assert (
            result.stdout
            == """ID                Name                Description
----------------  ------------------  ---------------------------------------------------------------------------------------------------------------
postgres_fdw      PostgreSQL          Data source for PostgreSQL databases that supports live querying, based on postgres_fdw
airbyte-postgres  Postgres (Airbyte)  Airbyte connector for Postgres. For more information, see https://docs.airbyte.io/integrations/sources/postgres
"""
        )

        result = runner.invoke(
            plugins_c,
            ["--filter", "fdw"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        assert (
            result.stdout
            == """ID            Name        Description
------------  ----------  ---------------------------------------------------------------------------------------
postgres_fdw  PostgreSQL  Data source for PostgreSQL databases that supports live querying, based on postgres_fdw
"""
        )
