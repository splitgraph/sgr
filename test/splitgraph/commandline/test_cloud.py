import json
from unittest.mock import patch, PropertyMock

import httpretty
import pytest
from click.testing import CliRunner

from splitgraph.__version__ import __version__
from splitgraph.commandline.cloud import register_c, login_c, curl_c
from splitgraph.config import create_config_dict
from splitgraph.exceptions import AuthAPIError

_REMOTE = "remote_engine"
_ENDPOINT = "http://some-auth-service"
_SAMPLE_ACCESS = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE1ODA1OTQyMzQsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiZW1haWwiOiJzb21ldXNlckBleGFtcGxlLmNvbSIsImV4cCI6MTU4MDU5NzgzNCwidXNlcl9pZCI6IjEyM2U0NTY3LWU4OWItMTJkMy1hNDU2LTQyNjY1NTQ0MDAwMCIsImdyYW50IjoiYWNjZXNzIiwidXNlcm5hbWUiOiJzb21ldXNlciIsImlhdCI6MTU4MDU5NDIzNH0.YEuNhqKfFoxHloohfxInSEV9rnivXcF9SvFP72Vv1mDDsaqlRqCjKYM4S7tdSMap5__e3_UTwE_CpH8eI7DdePjMu8AOFXwFHPl34AAxZgavP4Mly0a0vrMsxNJ4KbtmL5-7ih3uneTEuZLt9zQLUh-Bi_UYlEYwGl8xgz5dDZ1YlwTEMsqSrDnXdjl69CTk3vVHIQdxtki4Ng7dZhbOnEdJIRsZi9_VdMlsg2TIU-0FsU2bYYBWktms5hyAAH0RkHYfvjGwIRirSEjxTpO9vci-eAsF8C4ohTUg6tajOcyWz8d7JSaJv_NjLFMZI9mC09hchbQZkw-37CdbS_8Yvw"
_SAMPLE_REFRESH = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE1NzYzMTk5MTYsImlhdCI6MTU3NjMxOTkxNiwiZW1haWwiOiJzb21ldXNlckBleGFtcGxlLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwicmVmcmVzaF90b2tlbl9zZWNyZXQiOiJzb21lc2VjcmV0IiwiZXhwIjoxNTc4OTExOTE2LCJ1c2VyX2lkIjoiMTIzZTQ1NjctZTg5Yi0xMmQzLWE0NTYtNDI2NjU1NDQwMDAwIiwidXNlcm5hbWUiOiJzb21ldXNlciIsInJlZnJlc2hfdG9rZW5fa2V5Ijoic29tZWtleSIsImdyYW50IjoicmVmcmVzaCJ9.lO3nN3Tmu3twwUjrWsVpBq7nHHEvLnOGXeMkXXv4PRBADUAHyhmmaIPzgccq9XlwpLIexBAxTKJ4GaxSQufKUVLbzAKIMHqxiGTzELY6JMyUvMDHKeKNsq6FdhHxXoKa96fHaDDa65eGcSRSKS3Yr-9sBiANMBJGRbwypYw41gf61pewMA8TXqBmA-mvsBzMUaQNz1DfjkkpHs4SCERPK0GhYSJwDAwK8U3wG47S9k-CQqpq2B99yRRrdSVRzA_lcKe7GlF-Pw6hbRR7xBPBtX61pPME5hFUCPcwYWYXa_KhqEx9IF9edt9UahZuBudaVLmTdKKWgE9M53jQofxNzg"


def _register_callback(request, uri, response_headers):
    assert json.loads(request.body) == {
        "username": "someuser",
        "password": "somepassword",
        "email": "someuser@example.com",
        "accept_tos": True,
    }
    return [
        200,
        response_headers,
        json.dumps(
            {
                "user_id": "123e4567-e89b-12d3-a456-426655440000",
                "access_token": _SAMPLE_ACCESS,
                "refresh_token": _SAMPLE_REFRESH,
            }
        ),
    ]


def _refresh_token_callback(request, uri, response_headers):
    assert json.loads(request.body) == {"username": "someuser", "password": "somepassword"}
    return [
        200,
        response_headers,
        json.dumps({"access_token": _SAMPLE_ACCESS, "refresh_token": _SAMPLE_REFRESH}),
    ]


def _create_creds_callback(request, uri, response_headers):
    assert json.loads(request.body) == {"password": "somepassword"}
    assert request.headers["Authorization"] == "Bearer %s" % _SAMPLE_ACCESS
    return [
        200,
        response_headers,
        json.dumps({"key": "abcdef123456", "secret": "654321fedcba"}),
    ]


def _tos_callback(request, uri, response_headers):
    return [
        200,
        response_headers,
        json.dumps({"tos": "Sample ToS message"}),
    ]


@httpretty.activate(allow_net_connect=False)
def test_commandline_registration_normal():
    httpretty.register_uri(
        httpretty.HTTPretty.POST, _ENDPOINT + "/register_user", body=_register_callback
    )

    httpretty.register_uri(httpretty.HTTPretty.GET, _ENDPOINT + "/tos", body=_tos_callback)

    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        _ENDPOINT + "/create_machine_credentials",
        body=_create_creds_callback,
    )

    source_config = _make_dummy_config_dict()

    runner = CliRunner()

    with patch("splitgraph.config.export.overwrite_config"):
        with patch("splitgraph.config.config.patch_config") as pc:
            with patch("splitgraph.config.CONFIG", source_config):
                with patch("splitgraph.commandline.cloud.inject_config_into_engines") as ic:
                    # First don't agree to ToS, then agree
                    args = [
                        "--username",
                        "someuser",
                        "--password",
                        "somepassword",
                        "--email",
                        "someuser@example.com",
                        "--remote",
                        _REMOTE,
                    ]
                    result = runner.invoke(
                        register_c, args=args, catch_exceptions=False, input="n",
                    )
                    assert result.exit_code == 1
                    assert "Sample ToS message" in result.output

                    result = runner.invoke(
                        register_c, args=args, catch_exceptions=False, input="y",
                    )
                    assert result.exit_code == 0
                    assert "Sample ToS message" in result.output

    pc.assert_called_once_with(
        source_config,
        {
            "SG_REPO_LOOKUP": "remote_engine",
            "remotes": {
                "remote_engine": {
                    "SG_ENGINE_USER": "abcdef123456",
                    "SG_ENGINE_PWD": "654321fedcba",
                    "SG_NAMESPACE": "someuser",
                    "SG_CLOUD_REFRESH_TOKEN": _SAMPLE_REFRESH,
                    "SG_CLOUD_ACCESS_TOKEN": _SAMPLE_ACCESS,
                    "SG_IS_REGISTRY": "true",
                }
            },
        },
    )

    ic.assert_called_once_with("splitgraph_test_engine_", source_config["SG_CONFIG_FILE"])


@httpretty.activate(allow_net_connect=False)
def test_commandline_registration_user_error():
    # Test a user error response propagates back to the command line client
    # (tests for handling other failure states are API client-specific).

    def register_callback(request, uri, response_headers):
        return [403, response_headers, json.dumps({"error": "Username exists"})]

    httpretty.register_uri(httpretty.HTTPretty.GET, _ENDPOINT + "/tos", body=_tos_callback)

    httpretty.register_uri(
        httpretty.HTTPretty.POST, _ENDPOINT + "/register_user", body=register_callback
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
            _REMOTE,
            "--accept-tos",
        ],
    )
    print(result.output)
    assert result.exit_code == 1
    assert isinstance(result.exception, AuthAPIError)
    assert "Username exists" in str(result.exception)


@httpretty.activate(allow_net_connect=False)
def test_commandline_login_normal():
    httpretty.register_uri(
        httpretty.HTTPretty.POST, _ENDPOINT + "/refresh_token", body=_refresh_token_callback
    )

    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        _ENDPOINT + "/create_machine_credentials",
        body=_create_creds_callback,
    )

    source_config = _make_dummy_config_dict()

    runner = CliRunner()

    with patch("splitgraph.config.export.overwrite_config"):
        with patch("splitgraph.config.config.patch_config") as pc:
            with patch("splitgraph.commandline.cloud.inject_config_into_engines") as ic:
                with patch("splitgraph.config.CONFIG", source_config):
                    result = runner.invoke(
                        login_c,
                        args=[
                            "--username",
                            "someuser",
                            "--password",
                            "somepassword",
                            "--remote",
                            _REMOTE,
                        ],
                        catch_exceptions=False,
                    )
                    assert result.exit_code == 0
                    print(result.output)

    pc.assert_called_once_with(
        source_config,
        {
            "SG_REPO_LOOKUP": _REMOTE,
            "remotes": {
                "remote_engine": {
                    "SG_NAMESPACE": "someuser",
                    "SG_CLOUD_REFRESH_TOKEN": _SAMPLE_REFRESH,
                    "SG_CLOUD_ACCESS_TOKEN": _SAMPLE_ACCESS,
                }
            },
        },
    )
    ic.assert_called_once_with("splitgraph_test_engine_", source_config["SG_CONFIG_FILE"])

    # Do the same overwriting the current API keys
    with patch("splitgraph.config.export.overwrite_config"):
        with patch("splitgraph.config.config.patch_config") as pc:
            with patch("splitgraph.commandline.cloud.inject_config_into_engines") as ic:
                with patch("splitgraph.config.CONFIG", source_config):
                    result = runner.invoke(
                        login_c,
                        args=[
                            "--username",
                            "someuser",
                            "--password",
                            "somepassword",
                            "--remote",
                            _REMOTE,
                            "--overwrite",
                        ],
                        catch_exceptions=False,
                    )
                    assert result.exit_code == 0

    pc.assert_called_once_with(
        source_config,
        {
            "SG_REPO_LOOKUP": _REMOTE,
            "remotes": {
                "remote_engine": {
                    "SG_ENGINE_USER": "abcdef123456",
                    "SG_ENGINE_PWD": "654321fedcba",
                    "SG_NAMESPACE": "someuser",
                    "SG_CLOUD_REFRESH_TOKEN": _SAMPLE_REFRESH,
                    "SG_CLOUD_ACCESS_TOKEN": _SAMPLE_ACCESS,
                }
            },
        },
    )
    ic.assert_called_once_with("splitgraph_test_engine_", source_config["SG_CONFIG_FILE"])


def _make_dummy_config_dict():
    # Sanitize the test config so that there isn't a ton of spam
    source_config = create_config_dict()
    source_config["SG_CONFIG_FILE"] = ".sgconfig"
    source_config["remotes"] = {_REMOTE: source_config["remotes"][_REMOTE]}
    del source_config["mount_handlers"]
    del source_config["commands"]
    del source_config["external_handlers"]
    return source_config


@pytest.mark.parametrize(
    "test_case",
    [
        ("ns/repo/image/table?id=eq.5", "http://some-query-service/ns/repo/image/table?id=eq.5"),
        ("ns/repo/latest/table?id=eq.5", "http://some-query-service/ns/repo/latest/table?id=eq.5"),
    ],
)
def test_commandline_curl_normal(test_case):
    runner = CliRunner()
    request, result_url = test_case

    with patch(
        "splitgraph.cloud.AuthAPIClient.access_token",
        new_callable=PropertyMock,
        return_value="AAAABBBBCCCCDDDD",
    ):
        with patch("splitgraph.commandline.cloud.subprocess.call") as sc:
            result = runner.invoke(
                curl_c,
                ["--remote", _REMOTE, request, "--some-curl-arg", "-Ssl"],
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
                    "--some-curl-arg",
                    "-Ssl",
                ]
            )
