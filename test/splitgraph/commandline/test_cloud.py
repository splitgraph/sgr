import json
from contextlib import contextmanager
from unittest.mock import patch, PropertyMock, call

import httpretty
import pytest
from click.testing import CliRunner

from splitgraph.__version__ import __version__
from splitgraph.commandline.cloud import (
    register_c,
    login_c,
    curl_c,
    login_api_c,
    readme_c,
    metadata_c,
    description_c,
)
from splitgraph.config import create_config_dict
from splitgraph.exceptions import (
    AuthAPIError,
    GQLUnauthorizedError,
    GQLUnauthenticatedError,
    GQLRepoDoesntExistError,
    GQLAPIError,
)

_REMOTE = "remote_engine"
_ENDPOINT = "http://some-auth-service.example.com"
_GQL_ENDPOINT = "http://some-gql-service.example.com"
_SAMPLE_ACCESS = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE1ODA1OTQyMzQsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiZW1haWwiOiJzb21ldXNlckBleGFtcGxlLmNvbSIsImV4cCI6MTU4MDU5NzgzNCwidXNlcl9pZCI6IjEyM2U0NTY3LWU4OWItMTJkMy1hNDU2LTQyNjY1NTQ0MDAwMCIsImdyYW50IjoiYWNjZXNzIiwidXNlcm5hbWUiOiJzb21ldXNlciIsImlhdCI6MTU4MDU5NDIzNH0.YEuNhqKfFoxHloohfxInSEV9rnivXcF9SvFP72Vv1mDDsaqlRqCjKYM4S7tdSMap5__e3_UTwE_CpH8eI7DdePjMu8AOFXwFHPl34AAxZgavP4Mly0a0vrMsxNJ4KbtmL5-7ih3uneTEuZLt9zQLUh-Bi_UYlEYwGl8xgz5dDZ1YlwTEMsqSrDnXdjl69CTk3vVHIQdxtki4Ng7dZhbOnEdJIRsZi9_VdMlsg2TIU-0FsU2bYYBWktms5hyAAH0RkHYfvjGwIRirSEjxTpO9vci-eAsF8C4ohTUg6tajOcyWz8d7JSaJv_NjLFMZI9mC09hchbQZkw-37CdbS_8Yvw"
_SAMPLE_REFRESH = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYmYiOjE1NzYzMTk5MTYsImlhdCI6MTU3NjMxOTkxNiwiZW1haWwiOiJzb21ldXNlckBleGFtcGxlLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwicmVmcmVzaF90b2tlbl9zZWNyZXQiOiJzb21lc2VjcmV0IiwiZXhwIjoxNTc4OTExOTE2LCJ1c2VyX2lkIjoiMTIzZTQ1NjctZTg5Yi0xMmQzLWE0NTYtNDI2NjU1NDQwMDAwIiwidXNlcm5hbWUiOiJzb21ldXNlciIsInJlZnJlc2hfdG9rZW5fa2V5Ijoic29tZWtleSIsImdyYW50IjoicmVmcmVzaCJ9.lO3nN3Tmu3twwUjrWsVpBq7nHHEvLnOGXeMkXXv4PRBADUAHyhmmaIPzgccq9XlwpLIexBAxTKJ4GaxSQufKUVLbzAKIMHqxiGTzELY6JMyUvMDHKeKNsq6FdhHxXoKa96fHaDDa65eGcSRSKS3Yr-9sBiANMBJGRbwypYw41gf61pewMA8TXqBmA-mvsBzMUaQNz1DfjkkpHs4SCERPK0GhYSJwDAwK8U3wG47S9k-CQqpq2B99yRRrdSVRzA_lcKe7GlF-Pw6hbRR7xBPBtX61pPME5hFUCPcwYWYXa_KhqEx9IF9edt9UahZuBudaVLmTdKKWgE9M53jQofxNzg"
_SAMPLE_API_KEY = "abcdef123456"
_SAMPLE_API_SECRET = "654321fedcba"


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


def _access_token_callback(request, uri, response_headers):
    assert json.loads(request.body) == {
        "api_key": _SAMPLE_API_KEY,
        "api_secret": _SAMPLE_API_SECRET,
    }
    return [
        200,
        response_headers,
        json.dumps({"access_token": _SAMPLE_ACCESS}),
    ]


def _create_creds_callback(request, uri, response_headers):
    assert json.loads(request.body) == {"password": "somepassword"}
    assert request.headers["Authorization"] == "Bearer %s" % _SAMPLE_ACCESS
    return [
        200,
        response_headers,
        json.dumps({"key": _SAMPLE_API_KEY, "secret": _SAMPLE_API_SECRET}),
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
    assert pc.mock_calls == [
        call(
            source_config,
            {
                "SG_REPO_LOOKUP": "remote_engine",
                "remotes": {
                    "remote_engine": {
                        "SG_ENGINE_USER": _SAMPLE_API_KEY,
                        "SG_ENGINE_PWD": _SAMPLE_API_SECRET,
                        "SG_NAMESPACE": "someuser",
                        "SG_CLOUD_REFRESH_TOKEN": _SAMPLE_REFRESH,
                        "SG_CLOUD_ACCESS_TOKEN": _SAMPLE_ACCESS,
                        "SG_IS_REGISTRY": "true",
                    }
                },
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


@contextmanager
def _patch_login_funcs(source_config):
    with patch("splitgraph.config.export.overwrite_config"):
        with patch("splitgraph.config.config.patch_config") as pc:
            with patch("splitgraph.commandline.cloud.inject_config_into_engines") as ic:
                with patch("splitgraph.config.CONFIG", source_config):
                    yield pc, ic


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
    with _patch_login_funcs(source_config) as (pc, ic):
        result = runner.invoke(
            login_c,
            args=["--username", "someuser", "--password", "somepassword", "--remote", _REMOTE,],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        print(result.output)

    assert pc.mock_calls == [
        call(
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
                _REMOTE,
                "--overwrite",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

    assert pc.mock_calls == [
        call(
            source_config,
            {
                "SG_REPO_LOOKUP": _REMOTE,
                "remotes": {
                    "remote_engine": {
                        "SG_ENGINE_USER": _SAMPLE_API_KEY,
                        "SG_ENGINE_PWD": _SAMPLE_API_SECRET,
                        "SG_NAMESPACE": "someuser",
                        "SG_CLOUD_REFRESH_TOKEN": _SAMPLE_REFRESH,
                        "SG_CLOUD_ACCESS_TOKEN": _SAMPLE_ACCESS,
                    }
                },
            },
        )
    ]

    assert ic.mock_calls == [call("splitgraph_test_engine_", source_config["SG_CONFIG_FILE"])]

    # Do the same changing the user -- check new API keys are still acquired
    source_config["remotes"][_REMOTE]["SG_NAMESPACE"] = "someotheruser"
    with _patch_login_funcs(source_config) as (pc, ic):
        result = runner.invoke(
            login_c,
            args=["--username", "someuser", "--password", "somepassword", "--remote", _REMOTE,],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

    assert pc.mock_calls == [
        call(
            source_config,
            {
                "SG_REPO_LOOKUP": _REMOTE,
                "remotes": {
                    "remote_engine": {
                        "SG_ENGINE_USER": _SAMPLE_API_KEY,
                        "SG_ENGINE_PWD": _SAMPLE_API_SECRET,
                        "SG_NAMESPACE": "someuser",
                        "SG_CLOUD_REFRESH_TOKEN": _SAMPLE_REFRESH,
                        "SG_CLOUD_ACCESS_TOKEN": _SAMPLE_ACCESS,
                    }
                },
            },
        )
    ]


@httpretty.activate(allow_net_connect=False)
def test_commandline_login_api():
    httpretty.register_uri(
        httpretty.HTTPretty.POST, _ENDPOINT + "/access_token", body=_access_token_callback
    )

    source_config = _make_dummy_config_dict()

    runner = CliRunner()
    with _patch_login_funcs(source_config) as (pc, ic):
        result = runner.invoke(
            login_api_c,
            args=[
                "--api-key",
                _SAMPLE_API_KEY,
                "--api-secret",
                _SAMPLE_API_SECRET,
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
                    "SG_CLOUD_ACCESS_TOKEN": _SAMPLE_ACCESS,
                    "SG_ENGINE_USER": _SAMPLE_API_KEY,
                    "SG_ENGINE_PWD": _SAMPLE_API_SECRET,
                }
            },
        },
    )
    ic.assert_called_once_with("splitgraph_test_engine_", source_config["SG_CONFIG_FILE"])


@httpretty.activate(allow_net_connect=False)
def test_commandline_login_password_prompt():
    httpretty.register_uri(
        httpretty.HTTPretty.POST, _ENDPOINT + "/refresh_token", body=_refresh_token_callback
    )

    source_config = _make_dummy_config_dict()

    runner = CliRunner()
    with _patch_login_funcs(source_config) as (pc, ic):
        result = runner.invoke(
            login_c,
            args=["--username", "someuser", "--remote", _REMOTE],
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
    source_config["remotes"] = {_REMOTE: source_config["remotes"][_REMOTE]}
    del source_config["mount_handlers"]
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
        (["ns/repo"], "http://some-query-service.example.com/ns/repo/latest/-/rest", [],),
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
        "splitgraph.cloud.AuthAPIClient.access_token",
        new_callable=PropertyMock,
        return_value="AAAABBBBCCCCDDDD",
    ):
        with patch("splitgraph.commandline.cloud.subprocess.call") as sc:
            result = runner.invoke(
                curl_c,
                ["--remote", _REMOTE] + args + ["-c", "--some-curl-arg", "-c", "-Ssl"],
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
                + ["--some-curl-arg", "-Ssl",]
            )


def _gql_callback(request, uri, response_headers):
    body = json.loads(request.body)

    if body["operationName"] == "UpsertRepoReadme":
        assert body["query"] == (
            "mutation UpsertRepoReadme($namespace: String!, $repository: String!, "
            "$readme: String!) {\n  __typename\n  "
            "upsertRepoProfileByNamespaceAndRepository(input: {repoProfile: "
            "{namespace: $namespace, repository: $repository, readme: $readme}, "
            "patch: {readme: $readme}}) {\n    "
            "clientMutationId\n    __typename\n  }\n}\n"
        )
    elif body["operationName"] == "UpsertRepoDescription":
        assert body["query"] == (
            "mutation UpsertRepoDescription( "
            "$namespace: String!, $repository: String!, $description: String!) {\n "
            " __typename\n  "
            "upsertRepoProfileByNamespaceAndRepository(input: {repoProfile: "
            "{namespace: $namespace, repository: $repository, "
            "description: $description}, patch: {description: $description}}) {\n    "
            "clientMutationId\n    __typename\n  }\n}\n"
        )
    else:
        raise AssertionError()

    namespace = body["variables"]["namespace"]
    repository = body["variables"]["repository"]

    error_response = {
        "errors": [
            {
                "message": "An error has occurred",
                "locations": [{"line": 3, "column": 3}],
                "path": ["upsertRepoProfileByNamespaceAndRepository"],
            }
        ],
        "data": {"__typename": "Mutation", "upsertRepoProfileByNamespaceAndRepository": None},
    }

    success_response = {
        "data": {
            "__typename": "Mutation",
            "upsertRepoProfileByNamespaceAndRepository": {
                "clientMutationId": None,
                "__typename": "UpsertRepoProfilePayload",
            },
        }
    }

    if body["operationName"] == "UpsertRepoReadme" and not body["variables"].get("readme"):
        response = error_response
    elif body["operationName"] == "UpsertRepoDescription" and not body["variables"].get(
        "description"
    ):
        response = error_response
    elif request.headers.get("Authorization") != "Bearer " + _SAMPLE_ACCESS:
        response = error_response
        response["errors"][0]["message"] = "Invalid token"
    elif namespace != "someuser":
        response = error_response
        response["errors"][0][
            "message"
        ] = 'new row violates row-level security policy for table "repo_profiles"'
    elif repository != "somerepo":
        response = error_response
        response["errors"][0][
            "message"
        ] = 'insert or update on table "repo_profiles" violates foreign key constraint "repo_fk"'
    else:
        response = success_response

    return [
        200,
        response_headers,
        json.dumps(response),
    ]


@httpretty.activate(allow_net_connect=False)
@pytest.mark.parametrize(
    "namespace,repository,readme,token,expected",
    [
        ("someuser", "somerepo", "somereadme", _SAMPLE_ACCESS, True),
        ("someuser", "somerepo", "somereadme", "not_a_token", GQLUnauthenticatedError),
        ("otheruser", "somerepo", "somereadme", _SAMPLE_ACCESS, GQLUnauthorizedError),
        ("someuser", "otherrepo", "somereadme", _SAMPLE_ACCESS, GQLRepoDoesntExistError),
        # Use empty readme (is normally allowed) to test API returning an error we can't
        # decode, raising a generic error instead.
        ("someuser", "otherrepo", "", _SAMPLE_ACCESS, GQLAPIError),
    ],
)
def test_commandline_readme(namespace, repository, readme, token, expected):
    runner = CliRunner()
    httpretty.register_uri(httpretty.HTTPretty.POST, _GQL_ENDPOINT + "/", body=_gql_callback)

    with patch(
        "splitgraph.cloud.AuthAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=token,
    ):
        with patch("splitgraph.cloud.get_remote_param", return_value=_GQL_ENDPOINT):
            result = runner.invoke(readme_c, [namespace + "/" + repository, "-"], input=readme)

            if expected is True:
                assert result.exit_code == 0
                assert (
                    "README updated for repository %s/%s." % (namespace, repository)
                    in result.output
                )
            else:
                assert result.exit_code != 0
                assert isinstance(result.exception, expected)


@httpretty.activate(allow_net_connect=False)
def test_commandline_description():
    runner = CliRunner()
    httpretty.register_uri(httpretty.HTTPretty.POST, _GQL_ENDPOINT + "/", body=_gql_callback)

    with patch(
        "splitgraph.cloud.AuthAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=_SAMPLE_ACCESS,
    ):
        with patch("splitgraph.cloud.get_remote_param", return_value=_GQL_ENDPOINT):
            result = runner.invoke(
                description_c, ["someuser/somerepo", "some description"], catch_exceptions=False,
            )

            assert result.exit_code == 0
            assert "Description updated for repository someuser/somerepo." in result.output

            result = runner.invoke(description_c, ["someuser/somerepo", "way too long" * 16])
            assert result.exit_code == 1
            assert "The description should be 160 characters or shorter!" in str(result.exc_info[1])


_BROKEN_META = """
what_is_this_key: no_value
"""

_EXPECTED_META = """
readme: test-readme.md
description: Description for a sample repo
extra_keys: are ok for now
"""


@httpretty.activate(allow_net_connect=False)
def test_commandline_metadata(fs):
    runner = CliRunner()
    httpretty.register_uri(httpretty.HTTPretty.POST, _GQL_ENDPOINT + "/", body=_gql_callback)

    with open("test-readme.md", "w") as f:
        f.write("# Sample dataset readme\n\nHello there\n")

    with patch(
        "splitgraph.cloud.AuthAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=_SAMPLE_ACCESS,
    ):
        with patch("splitgraph.cloud.get_remote_param", return_value=_GQL_ENDPOINT):
            result = runner.invoke(metadata_c, ["someuser/somerepo", "-"], input=_BROKEN_META)
            assert result.exit_code == 2
            assert "Invalid metadata file" in result.output

            result = runner.invoke(
                metadata_c,
                ["someuser/somerepo", "-"],
                input=_EXPECTED_META,
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            assert "README updated for repository someuser/somerepo." in result.output
            assert "Description updated for repository someuser/somerepo." in result.output
