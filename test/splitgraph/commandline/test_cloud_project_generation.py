import os
from tempfile import TemporaryDirectory
from test.splitgraph.commandline.http_fixtures import GQL_ENDPOINT, gql_plugins_callback
from unittest.mock import patch

import httpretty
from splitgraph.cloud import GQLAPIClient
from splitgraph.cloud.project.generation import ProjectSeed, generate_project


@httpretty.activate(allow_net_connect=False)
def test_generate_project_no_dbt(snapshot):
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        GQL_ENDPOINT + "/",
        body=gql_plugins_callback,
    )
    with patch(
        "splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT
    ), TemporaryDirectory() as tmpdir:
        generate_project(
            GQLAPIClient("anyremote"),
            seed=ProjectSeed(namespace="myns", plugins=["postgres_fdw", "airbyte-postgres"]),
            basedir=tmpdir,
        )

        with open(os.path.join(tmpdir, "splitgraph.yml")) as f:
            splitgraph_yml = f.read()
        with open(os.path.join(tmpdir, "splitgraph.credentials.yml")) as f:
            splitgraph_credentials_yml = f.read()
        with open(os.path.join(tmpdir, ".github/workflows/build.yml")) as f:
            github_build_yml = f.read()

        snapshot.assert_match_dir(
            {
                ".github": {
                    "workflows": {
                        "build.yml": github_build_yml,
                    }
                },
                "splitgraph.yml": splitgraph_yml,
                "splitgraph.credentials.yml": splitgraph_credentials_yml,
            },
            "generate_project",
        )
