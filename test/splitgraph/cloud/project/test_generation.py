import os
from tempfile import TemporaryDirectory
from test.splitgraph.commandline.http_fixtures import GQL_ENDPOINT, gql_plugins_callback
from unittest.mock import patch

import httpretty
from click.testing import CliRunner
from splitgraph.cloud import GQLAPIClient
from splitgraph.cloud.project.generation import ProjectSeed, generate_project
from splitgraph.commandline.cloud import validate_c


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


@httpretty.activate(allow_net_connect=False)
def test_generate_project_with_dbt(snapshot):
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
            seed=ProjectSeed(
                namespace="myns", plugins=["postgres_fdw", "airbyte-postgres"], include_dbt=True
            ),
            basedir=tmpdir,
        )

        project_files = [
            os.path.join(dp, f) for dp, dn, fn in sorted(os.walk(tmpdir)) for f in sorted(fn)
        ]
        assert project_files == [
            os.path.join(tmpdir, d)
            for d in [
                "dbt_project.yml",
                "splitgraph.credentials.yml",
                "splitgraph.yml",
                ".github/workflows/build.yml",
                "models/staging/sources.yml",
                "models/staging/myns_airbyte_postgres/myns_airbyte_postgres.sql",
                "models/staging/myns_postgres_fdw/myns_postgres_fdw.sql",
            ]
        ]

        with open(os.path.join(tmpdir, "splitgraph.yml")) as f:
            splitgraph_yml = f.read()
        with open(os.path.join(tmpdir, "splitgraph.credentials.yml")) as f:
            splitgraph_credentials_yml = f.read()
        with open(os.path.join(tmpdir, ".github/workflows/build.yml")) as f:
            github_build_yml = f.read()
        with open(os.path.join(tmpdir, "models/staging/sources.yml")) as f:
            sources_yml = f.read()
        with open(os.path.join(tmpdir, "dbt_project.yml")) as f:
            dbt_project_yml = f.read()

        snapshot.assert_match_dir(
            {
                ".github": {
                    "workflows": {
                        "build.yml": github_build_yml,
                    }
                },
                "models": {"staging": {"sources.yml": sources_yml}},
                "splitgraph.yml": splitgraph_yml,
                "splitgraph.credentials.yml": splitgraph_credentials_yml,
                "dbt_project.yml": dbt_project_yml,
            },
            "generate_project_dbt",
        )

        # Check that we can correctly parse the resulting file
        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(
            validate_c,
            [
                "-f",
                os.path.join(tmpdir, "splitgraph.yml"),
                "-f",
                os.path.join(tmpdir, "splitgraph.credentials.yml"),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
