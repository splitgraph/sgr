import os
from tempfile import TemporaryDirectory

from splitgraph.cloud.project.dbt import generate_dbt_plugin_params, generate_dbt_project


def test_generate_dbt_plugin_params():
    assert generate_dbt_plugin_params(
        ["some-data/source", "some-other/data-raw", "and-third/data"]
    ) == (
        {
            "sources": [
                {
                    "dbt_source_name": "some_data_source",
                    "namespace": "some-data",
                    "repository": "source",
                    "hash_or_tag": "latest",
                },
                {
                    "dbt_source_name": "some_other_data_raw",
                    "namespace": "some-other",
                    "repository": "data-raw",
                    "hash_or_tag": "latest",
                },
                {
                    "dbt_source_name": "and_third_data",
                    "namespace": "and-third",
                    "repository": "data",
                    "hash_or_tag": "latest",
                },
            ]
        },
        {"git_url": "$THIS_REPO_URL"},
    )


def test_generate_dbt_project(snapshot):
    with TemporaryDirectory() as tmpdir:
        generate_dbt_project(["some-data/source", "some-other/data-raw", "and-third/data"], tmpdir)

        project_files = [os.path.join(dp, f) for dp, dn, fn in os.walk(tmpdir) for f in fn]
        assert project_files == [
            os.path.join(tmpdir, d)
            for d in [
                "dbt_project.yml",
                "models/staging/sources.yml",
                "models/staging/some_data_source/some_data_source.sql",
                "models/staging/some_other_data_raw/some_other_data_raw.sql",
                "models/staging/and_third_data/and_third_data.sql",
            ]
        ]

        with open(project_files[0]) as f:
            actual_dict = {"dbt_project.yml": f.read()}
        with open(project_files[1]) as f:
            actual_dict["models"] = {"staging": {"sources.yml": f.read()}}
        with open(project_files[2]) as f:
            actual_dict["models"]["staging"]["some_data_source"] = {
                "some_data_source.sql": f.read()
            }
        with open(project_files[3]) as f:
            actual_dict["models"]["staging"]["some_other_data_raw"] = {
                "some_other_data_raw.sql": f.read()
            }
        with open(project_files[4]) as f:
            actual_dict["models"]["staging"]["and_third_data"] = {"and_third_data.sql": f.read()}

        snapshot.assert_match_dir(actual_dict, "splitgraph_template")
