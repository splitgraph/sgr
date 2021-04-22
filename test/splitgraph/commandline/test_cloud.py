import json
import os
import tempfile
from contextlib import contextmanager
from tempfile import TemporaryDirectory
from typing import Optional
from unittest.mock import patch, PropertyMock, call, Mock

import httpretty
import pytest
import yaml
from click.testing import CliRunner
from httpretty.core import HTTPrettyRequest

from splitgraph.__version__ import __version__
from splitgraph.cloud import _PROFILE_UPSERT_QUERY
from splitgraph.commandline import cli
from splitgraph.commandline.cloud import (
    register_c,
    login_c,
    curl_c,
    login_api_c,
    readme_c,
    metadata_c,
    description_c,
    sql_c,
    search_c,
    dump_c,
)
from splitgraph.config import create_config_dict
from splitgraph.config.config import patch_config
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
_SAMPLE_REMOTE_CONFIG = {
    "SG_ENGINE_USER": _SAMPLE_API_KEY,
    "SG_ENGINE_PWD": _SAMPLE_API_SECRET,
    "SG_NAMESPACE": "someuser",
    "SG_CLOUD_REFRESH_TOKEN": _SAMPLE_REFRESH,
    "SG_CLOUD_ACCESS_TOKEN": _SAMPLE_ACCESS,
    "SG_IS_REGISTRY": "true",
}


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

    with patch("splitgraph.config.export.overwrite_config"), patch(
        "splitgraph.config.config.patch_config"
    ) as pc, patch("splitgraph.config.CONFIG", source_config), patch(
        "splitgraph.cloud.create_config_dict", return_value=source_config
    ), patch(
        "splitgraph.commandline.cloud.inject_config_into_engines"
    ) as ic:
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
                "remotes": {"remote_engine": _SAMPLE_REMOTE_CONFIG},
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


@contextmanager
def _patch_config_funcs(config):
    with patch("splitgraph.cloud.create_config_dict", return_value=config):
        with patch("splitgraph.cloud.patch_and_save_config") as oc:
            with patch("splitgraph.config.CONFIG", config):
                yield oc


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
                + [
                    "--some-curl-arg",
                    "-Ssl",
                ]
            )


def _make_gql_callback(expect_variables):
    def _gql_callback(request, uri, response_headers):
        body = json.loads(request.body)
        assert body["variables"] == expect_variables
        if body["operationName"] == "UpsertRepoProfile":
            assert body["query"] == _PROFILE_UPSERT_QUERY
        elif body["operationName"] == "FindRepositories":
            response = {
                "data": {
                    "findRepository": {
                        "edges": [
                            {
                                "node": {
                                    "namespace": "namespace1",
                                    "repository": "repo1",
                                    "highlight": "<<some_query>> is here",
                                }
                            },
                            {
                                "node": {
                                    "namespace": "namespace2",
                                    "repository": "repo2",
                                    "highlight": "this is another result for <<ome_query>>",
                                }
                            },
                        ],
                        "totalCount": 42,
                    }
                }
            }
            return [
                200,
                response_headers,
                json.dumps(response),
            ]
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

        if request.headers.get("Authorization") != "Bearer " + _SAMPLE_ACCESS:
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

    return _gql_callback


def _make_gql_callback_metadata():
    def _gql_callback(request, uri, response_headers):
        body = json.loads(request.body)
        if body["operationName"] == "GetRepositoryMetadata":
            response = {
                "data": {
                    "repositories": {
                        "nodes": [
                            {
                                "namespace": "someuser",
                                "repository": "somerepo_1",
                                "repoTopicsByNamespaceAndRepository": {"nodes": []},
                                "repoProfileByNamespaceAndRepository": {
                                    "description": "Repository Description 1",
                                    "license": "Public Domain",
                                    "metadata": {
                                        "created_at": "2020-01-01 12:00:00",
                                        "upstream_metadata": {"key_1": {"key_2": "value_1"}},
                                    },
                                    "readme": "Test Repo 1 Readme",
                                    "sources": [
                                        {
                                            "anchor": "test data source",
                                            "href": "https://example.com",
                                            "isCreator": True,
                                            "isSameAs": False,
                                        }
                                    ],
                                },
                            },
                            {
                                "namespace": "otheruser",
                                "repository": "somerepo_2",
                                "repoTopicsByNamespaceAndRepository": {
                                    "nodes": [{"topics": ["topic_1", "topic_2"]}]
                                },
                                "repoProfileByNamespaceAndRepository": {
                                    "description": "Repository Description 2",
                                    "license": None,
                                    "metadata": None,
                                    "readme": "Test Repo 2 Readme",
                                    "sources": [
                                        {
                                            "anchor": "test data source",
                                            "href": "https://example.com",
                                        }
                                    ],
                                },
                            },
                        ]
                    }
                }
            }
            return [
                200,
                response_headers,
                json.dumps(response),
            ]
        elif body["operationName"] == "GetRepositoryDataSource":
            response = {
                "data": {
                    "repositoryDataSources": {
                        "nodes": [
                            {
                                "namespace": "otheruser",
                                "repository": "somerepo_2",
                                "credentialId": "abcdef-123456",
                                "dataSource": "plugin",
                                "params": {"plugin": "specific", "params": "here"},
                                "tableParams": {
                                    "table_1": {"param_1": "val_1"},
                                    "table_2": {"param_1": "val_2"},
                                },
                                "externalImageByNamespaceAndRepository": {
                                    "imageByNamespaceAndRepositoryAndImageHash": {
                                        "tablesByNamespaceAndRepositoryAndImageHash": {
                                            "nodes": [
                                                {
                                                    "tableName": "table_1",
                                                    "tableSchema": [
                                                        [
                                                            0,
                                                            "id",
                                                            "text",
                                                            False,
                                                            "Column ID",
                                                        ],
                                                        [
                                                            1,
                                                            "val",
                                                            "text",
                                                            False,
                                                            "Some value",
                                                        ],
                                                    ],
                                                },
                                                {
                                                    "tableName": "table_3",
                                                    "tableSchema": [
                                                        [
                                                            0,
                                                            "id",
                                                            "text",
                                                            False,
                                                            "Column ID",
                                                        ],
                                                        [
                                                            1,
                                                            "val",
                                                            "text",
                                                            False,
                                                            "Some value",
                                                        ],
                                                    ],
                                                },
                                            ]
                                        }
                                    }
                                },
                            },
                        ]
                    }
                }
            }
            return [
                200,
                response_headers,
                json.dumps(response),
            ]
        else:
            raise AssertionError()

    return _gql_callback


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
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        _GQL_ENDPOINT + "/",
        body=_make_gql_callback(
            expect_variables={
                "namespace": namespace,
                "readme": readme,
                "repository": repository,
            }
        ),
    )

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
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
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        _GQL_ENDPOINT + "/",
        body=_make_gql_callback(
            expect_variables={
                "namespace": "someuser",
                "description": "some description",
                "repository": "somerepo",
            }
        ),
    )

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=_SAMPLE_ACCESS,
    ):
        with patch("splitgraph.cloud.get_remote_param", return_value=_GQL_ENDPOINT):
            result = runner.invoke(
                description_c,
                ["someuser/somerepo", "some description"],
                catch_exceptions=False,
            )

            assert result.exit_code == 0
            assert "Description updated for repository someuser/somerepo." in result.output

            result = runner.invoke(description_c, ["someuser/somerepo", "way too long" * 16])
            assert result.exit_code == 1
            assert "The description should be 160 characters or shorter!" in str(result.exc_info[1])


_BROKEN_META = """
what_is_this_key: no_value
"""

_VALID_META = """
readme: {}
description: Description for a sample repo
extra_keys: are ok for now
topics:
  - topic_1
  - topic_2
sources:
  - anchor: Creator of the dataset
    href: https://www.splitgraph.com
    isCreator: true
    isSameAs: false
  - anchor: Source 2
    href: https://www.splitgraph.com
    isCreator: false
    isSameAs: true
license: Public Domain
extra_metadata:
  created_at: 2020-01-01 12:00:00
  Some Metadata Key 1:
    key_1_1: value_1_1
    key_1_2: value_1_2
  key_2:
    key_2_1: value_2_1
    key_2_2: value_2_2
"""


@httpretty.activate(allow_net_connect=False)
def test_commandline_metadata():
    runner = CliRunner()
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        _GQL_ENDPOINT + "/",
        body=_make_gql_callback(
            expect_variables={
                "description": "Description for a sample repo",
                "license": "Public Domain",
                "metadata": {
                    "created_at": "2020-01-01 12:00:00",
                    "upstream_metadata": {
                        "Some Metadata Key 1": {"key_1_1": "value_1_1", "key_1_2": "value_1_2"},
                        "key_2": {"key_2_1": "value_2_1", "key_2_2": "value_2_2"},
                    },
                },
                "namespace": "someuser",
                "readme": "# Sample dataset readme\n\nHello there\n",
                "repository": "somerepo",
                "sources": [
                    {
                        "anchor": "Creator of the dataset",
                        "href": "https://www.splitgraph.com",
                        "isCreator": True,
                        "isSameAs": False,
                    },
                    {
                        "anchor": "Source 2",
                        "href": "https://www.splitgraph.com",
                        "isCreator": False,
                        "isSameAs": True,
                    },
                ],
                "topics": ["topic_1", "topic_2"],
            }
        ),
    )

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=_SAMPLE_ACCESS,
    ):
        with patch("splitgraph.cloud.get_remote_param", return_value=_GQL_ENDPOINT):
            result = runner.invoke(metadata_c, ["someuser/somerepo", "-"], input=_BROKEN_META)
            assert result.exit_code == 2
            assert "Invalid metadata file" in result.output

            with TemporaryDirectory() as tmpdir:
                test_readme_path = os.path.join(tmpdir, "test-readme.md")
                with open(test_readme_path, "w") as f:
                    f.write("# Sample dataset readme\n\nHello there\n")

                result = runner.invoke(
                    metadata_c,
                    ["someuser/somerepo", "-"],
                    input=_VALID_META.format(test_readme_path),
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                assert "Metadata updated for repository someuser/somerepo." in result.output


@httpretty.activate(allow_net_connect=False)
def test_commandline_search():
    runner = CliRunner()
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        _GQL_ENDPOINT + "/",
        body=_make_gql_callback(expect_variables={"query": "some_query", "limit": 20}),
    )

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=_SAMPLE_ACCESS,
    ):
        with patch("splitgraph.cloud.get_remote_param", return_value=_GQL_ENDPOINT):
            result = runner.invoke(search_c, ["some_query", "--limit", "20"])
            assert result.exit_code == 0
            assert "namespace1/repo1" in result.output
            assert "http://www.example.com/namespace2/repo2" in result.output
            assert "Total results: 42" in result.output
            assert "Visit http://www.example.com/search?q=some_query" in result.output


@httpretty.activate(allow_net_connect=False)
def test_commandline_dump():
    runner = CliRunner()
    httpretty.register_uri(
        httpretty.HTTPretty.POST, _GQL_ENDPOINT + "/", body=_make_gql_callback_metadata()
    )

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=_SAMPLE_ACCESS,
    ):
        with patch("splitgraph.cloud.get_remote_param", return_value=_GQL_ENDPOINT):
            with tempfile.TemporaryDirectory() as tmpdir:
                result = runner.invoke(dump_c, ["--directory", tmpdir], catch_exceptions=False)
                assert result.exit_code == 0

                assert list(os.walk(tmpdir)) == [
                    (tmpdir, ["readmes"], ["repositories.yml"]),
                    (
                        os.path.join(tmpdir, "readmes"),
                        [],
                        ["otheruser-somerepo_2.fe37.md", "someuser-somerepo_1.b7f3.md"],
                    ),
                ]

                with open(os.path.join(tmpdir, "repositories.yml")) as f:
                    output = f.read()
                assert yaml.load(output) == {
                    "repositories": [
                        {
                            "namespace": "otheruser",
                            "repository": "somerepo_2",
                            "metadata": {
                                "readme": {"file": "otheruser-somerepo_2.fe37.md"},
                                "description": "Repository Description 2",
                                "topics": ["topic_1", "topic_2"],
                                "sources": [
                                    {
                                        "anchor": "test data source",
                                        "href": "https://example.com",
                                    }
                                ],
                                "license": None,
                            },
                            "external": {
                                "credential_id": "abcdef-123456",
                                "plugin": "plugin",
                                "params": {"plugin": "specific", "params": "here"},
                                "tables": {
                                    "table_1": {
                                        "options": {"param_1": "val_1"},
                                        "schema": [
                                            {"name": "id", "type": "text"},
                                            {"name": "val", "type": "text"},
                                        ],
                                    },
                                    "table_2": {"options": {"param_1": "val_2"}, "schema": []},
                                    "table_3": {
                                        "options": {},
                                        "schema": [
                                            {"name": "id", "type": "text"},
                                            {"name": "val", "type": "text"},
                                        ],
                                    },
                                },
                            },
                        },
                        {
                            "namespace": "someuser",
                            "repository": "somerepo_1",
                            "metadata": {
                                "readme": {"file": "someuser-somerepo_1.b7f3.md"},
                                "description": "Repository Description 1",
                                "topics": [],
                                "sources": [
                                    {
                                        "anchor": "test data source",
                                        "href": "https://example.com",
                                        "isCreator": True,
                                        "isSameAs": False,
                                    }
                                ],
                                "license": "Public Domain",
                            },
                            "external": None,
                        },
                    ]
                }

                with open(os.path.join(tmpdir, "readmes", "someuser-somerepo_1.b7f3.md")) as f:
                    assert f.read() == "Test Repo 1 Readme"


def test_commandline_cloud_sql():
    runner = CliRunner()

    fake_engine = Mock()

    remote_config = _SAMPLE_REMOTE_CONFIG.copy()
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
                result.output
                == f"postgresql://{_SAMPLE_API_KEY}:{_SAMPLE_API_SECRET}@data.example.com:5432/ddn\n"
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
        _ENDPOINT + "/update_check",
        body=_version_callback,
    )

    config = _make_dummy_config_dict()
    config["SG_UPDATE_FREQUENCY"] = "15"
    config["SG_UPDATE_REMOTE"] = _REMOTE

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
    new_config["remotes"][_REMOTE] = _SAMPLE_REMOTE_CONFIG
    new_config["SG_UPDATE_LAST"] = "0"

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=_SAMPLE_ACCESS,
    ):
        with _patch_config_funcs(new_config) as oc:
            last_request = None
            result = runner.invoke(cli, ["config"], catch_exceptions=False)
            assert result.exit_code == 0
            assert "version 99999.42.15 is available" in result.output
            assert oc.call_count == 1
            assert last_request is not None
            assert last_request.headers["Authorization"] == "Bearer " + _SAMPLE_ACCESS

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
