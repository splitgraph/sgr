import os
import tempfile
from tempfile import TemporaryDirectory
from unittest.mock import patch, PropertyMock

import httpretty
import pytest
import yaml
from click.testing import CliRunner

from splitgraph.commandline.cloud import (
    readme_c,
    description_c,
    metadata_c,
    search_c,
    dump_c,
    load_c,
)
from splitgraph.exceptions import (
    GQLUnauthenticatedError,
    GQLUnauthorizedError,
    GQLRepoDoesntExistError,
    GQLAPIError,
)
from test.splitgraph.commandline.http_fixtures import (
    GQL_ENDPOINT,
    QUERY_ENDPOINT,
    ACCESS_TOKEN,
    gql_metadata_operation,
    gql_metadata_get,
    INVALID_METADATA,
    VALID_METADATA,
    list_external_credentials,
    update_external_credential,
    add_external_credential,
    add_external_repo,
    upsert_repository_metadata,
    AUTH_ENDPOINT,
)
from test.splitgraph.conftest import RESOURCES


@httpretty.activate(allow_net_connect=False)
@pytest.mark.parametrize(
    "namespace,repository,readme,token,expected",
    [
        ("someuser", "somerepo", "somereadme", ACCESS_TOKEN, True),
        ("someuser", "somerepo", "somereadme", "not_a_token", GQLUnauthenticatedError),
        ("otheruser", "somerepo", "somereadme", ACCESS_TOKEN, GQLUnauthorizedError),
        ("someuser", "otherrepo", "somereadme", ACCESS_TOKEN, GQLRepoDoesntExistError),
        # Use empty readme (is normally allowed) to test API returning an error we can't
        # decode, raising a generic error instead.
        ("someuser", "otherrepo", "", ACCESS_TOKEN, GQLAPIError),
    ],
)
def test_commandline_readme(namespace, repository, readme, token, expected):
    runner = CliRunner()
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        GQL_ENDPOINT + "/",
        body=gql_metadata_operation(
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
        with patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
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
        GQL_ENDPOINT + "/",
        body=gql_metadata_operation(
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
        return_value=ACCESS_TOKEN,
    ):
        with patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
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


@httpretty.activate(allow_net_connect=False)
def test_commandline_metadata():
    runner = CliRunner()
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        GQL_ENDPOINT + "/",
        body=gql_metadata_operation(
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
        return_value=ACCESS_TOKEN,
    ):
        with patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
            result = runner.invoke(metadata_c, ["someuser/somerepo", "-"], input=INVALID_METADATA)
            assert result.exit_code == 2
            assert "Invalid metadata file" in result.output

            with TemporaryDirectory() as tmpdir:
                test_readme_path = os.path.join(tmpdir, "test-readme.md")
                with open(test_readme_path, "w") as f:
                    f.write("# Sample dataset readme\n\nHello there\n")

                result = runner.invoke(
                    metadata_c,
                    ["someuser/somerepo", "-"],
                    input=VALID_METADATA.format(test_readme_path),
                    catch_exceptions=False,
                )

                assert result.exit_code == 0
                assert "Metadata updated for repository someuser/somerepo." in result.output


@httpretty.activate(allow_net_connect=False)
def test_commandline_search():
    runner = CliRunner()
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        GQL_ENDPOINT + "/",
        body=gql_metadata_operation(expect_variables={"query": "some_query", "limit": 20}),
    )

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ):
        with patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
            result = runner.invoke(search_c, ["some_query", "--limit", "20"])
            assert result.exit_code == 0
            assert "namespace1/repo1" in result.output
            assert "http://www.example.com/namespace2/repo2" in result.output
            assert "Total results: 42" in result.output
            assert "Visit http://www.example.com/search?q=some_query" in result.output


@httpretty.activate(allow_net_connect=False)
def test_commandline_dump():
    runner = CliRunner()
    httpretty.register_uri(httpretty.HTTPretty.POST, GQL_ENDPOINT + "/", body=gql_metadata_get())

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ):
        with patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
            with tempfile.TemporaryDirectory() as tmpdir:
                result = runner.invoke(
                    dump_c,
                    [
                        "--readme-dir",
                        os.path.join(tmpdir, "readmes"),
                        os.path.join(tmpdir, "repositories.yml"),
                    ],
                    catch_exceptions=False,
                )
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


@httpretty.activate(allow_net_connect=False)
def test_commandline_load():
    runner = CliRunner()

    httpretty.register_uri(
        httpretty.HTTPretty.GET,
        AUTH_ENDPOINT + "/list_external_credentials",
        body=list_external_credentials,
    )

    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        AUTH_ENDPOINT + "/update_external_credential",
        body=update_external_credential,
    )

    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        AUTH_ENDPOINT + "/add_external_credential",
        body=add_external_credential,
    )

    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        QUERY_ENDPOINT + "/api/external/add",
        body=add_external_repo,
    )

    httpretty.register_uri(
        httpretty.HTTPretty.POST, GQL_ENDPOINT + "/", body=upsert_repository_metadata
    )

    def get_remote_param(remote, param):
        if param == "SG_AUTH_API":
            return AUTH_ENDPOINT
        elif param == "SG_QUERY_API":
            return QUERY_ENDPOINT
        elif param == "SG_GQL_API":
            return GQL_ENDPOINT
        else:
            raise KeyError()

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ):
        with patch("splitgraph.cloud.get_remote_param", get_remote_param):
            result = runner.invoke(
                load_c,
                [
                    "--readme-dir",
                    os.path.join(RESOURCES, "repositories_yml", "readmes"),
                    os.path.join(RESOURCES, "repositories_yml", "repositories.yml"),
                ],
                catch_exceptions=False,
            )
            assert result.exit_code == 0
            assert "someuser/somerepo_1" in result.output
