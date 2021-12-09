import os
import tempfile
from tempfile import TemporaryDirectory
from test.splitgraph.commandline.http_fixtures import (
    ACCESS_TOKEN,
    AUTH_ENDPOINT,
    GQL_ENDPOINT,
    INVALID_METADATA,
    QUERY_ENDPOINT,
    VALID_METADATA,
    add_external_credential,
    add_external_repo,
    assert_repository_profiles,
    assert_repository_sources,
    assert_repository_topics,
    gql_metadata_get,
    gql_metadata_operation,
    list_external_credentials,
    update_external_credential,
)
from test.splitgraph.conftest import RESOURCES
from unittest.mock import PropertyMock, patch

import httpretty
import pytest
from click.testing import CliRunner
from splitgraph.commandline.cloud import (
    description_c,
    dump_c,
    load_c,
    metadata_c,
    readme_c,
    search_c,
)
from splitgraph.exceptions import (
    GQLAPIError,
    GQLRepoDoesntExistError,
    GQLUnauthenticatedError,
    GQLUnauthorizedError,
)


@httpretty.activate(allow_net_connect=False)
@pytest.mark.parametrize(
    ("namespace", "repository", "readme", "token", "expected"),
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
    ), patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
        result = runner.invoke(readme_c, [namespace + "/" + repository, "-"], input=readme)

        if expected is True:
            assert result.exit_code == 0
            assert "README updated for repository %s/%s." % (namespace, repository) in result.output
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
    ), patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
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
    ), patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
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
    ), patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
        result = runner.invoke(search_c, ["some_query", "--limit", "20"])
        assert result.exit_code == 0
        assert "namespace1/repo1" in result.output
        assert "http://www.example.com/namespace2/repo2" in result.output
        assert "Total results: 42" in result.output
        assert "Visit http://www.example.com/search?q=some_query" in result.output


@httpretty.activate(allow_net_connect=False)
def test_commandline_dump(snapshot):
    runner = CliRunner()
    httpretty.register_uri(httpretty.HTTPretty.POST, GQL_ENDPOINT + "/", body=gql_metadata_get())

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ), patch(
        "splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT
    ), tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(
            dump_c,
            [
                "--readme-dir",
                os.path.join(tmpdir, "readmes"),
                "-f",
                os.path.join(tmpdir, "repositories.yml"),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        with open(os.path.join(tmpdir, "readmes", "someuser-somerepo_1.b7f3.md")) as f:
            readme_1 = f.read()
        with open(os.path.join(tmpdir, "readmes", "otheruser-somerepo_2.fe37.md")) as f:
            readme_2 = f.read()
        with open(os.path.join(tmpdir, "repositories.yml")) as f:
            repo_yml = f.read()

        snapshot.assert_match_dir(
            {
                "readmes": {
                    "someuser-somerepo_1.b7f3.md": readme_1,
                    "otheruser-somerepo_2.fe37.md": readme_2,
                },
                "repositories.yml": repo_yml,
            },
            "sgr_cloud_dump_multiple",
        )

        # Dump a single repo
        result = runner.invoke(
            dump_c,
            [
                "--readme-dir",
                os.path.join(tmpdir, "readmes"),
                "-f",
                os.path.join(tmpdir, "repositories.yml"),
                "someuser/somerepo_1",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        with open(os.path.join(tmpdir, "repositories.yml")) as f:
            repo_yml = f.read()

        snapshot.assert_match_dir(
            {
                "readmes": {"someuser-somerepo_1.b7f3.md": readme_1},
                "repositories.yml": repo_yml,
            },
            "sgr_cloud_dump_single",
        )


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
        QUERY_ENDPOINT + "/api/external/bulk-add",
        body=add_external_repo,
    )

    httpretty.register_uri(httpretty.HTTPretty.POST, GQL_ENDPOINT + "/")

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
    ), patch("splitgraph.cloud.get_remote_param", get_remote_param):
        result = runner.invoke(
            load_c,
            [
                "--readme-dir",
                os.path.join(RESOURCES, "repositories_yml", "readmes"),
                "-f",
                os.path.join(RESOURCES, "repositories_yml", "repositories.yml"),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        reqs = httpretty.latest_requests()

        assert_repository_topics(reqs.pop())
        reqs.pop()  # discard duplicate request
        assert_repository_sources(reqs.pop())
        reqs.pop()  # discard duplicate request
        assert_repository_profiles(reqs.pop())
