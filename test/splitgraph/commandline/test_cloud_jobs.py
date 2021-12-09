import os
import re
from pathlib import Path
from test.splitgraph.commandline.http_fixtures import (
    ACCESS_TOKEN,
    GQL_ENDPOINT,
    STORAGE_ENDPOINT,
    gql_job_logs,
    gql_job_status,
    gql_sync,
    gql_upload,
    job_log_callback,
)
from test.splitgraph.conftest import RESOURCES
from unittest import mock
from unittest.mock import PropertyMock, call, patch

import click
import httpretty
import pytest
import requests
from click.testing import CliRunner
from splitgraph.commandline.cloud import (
    _deduplicate_items,
    _get_external_from_yaml,
    logs_c,
    status_c,
    sync_c,
    upload_c,
)
from splitgraph.core.repository import Repository


@httpretty.activate(allow_net_connect=False)
def test_job_status_yaml(snapshot):
    runner = CliRunner(mix_stderr=False)
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        GQL_ENDPOINT + "/",
        body=gql_job_status(),
    )

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ), patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
        result = runner.invoke(
            status_c,
            [
                "-f",
                os.path.join(RESOURCES, "repositories_yml", "repositories.yml"),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        snapshot.assert_match(result.stdout, "sgr_cloud_status_yml.txt")


@httpretty.activate(allow_net_connect=False)
def test_job_status_explicit_repos(snapshot):
    runner = CliRunner(mix_stderr=False)
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        GQL_ENDPOINT + "/",
        body=gql_job_status(),
    )

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ), patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
        result = runner.invoke(
            status_c,
            ["someuser/somerepo_1", "otheruser/somerepo_2"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        snapshot.assert_match(result.stdout, "sgr_cloud_status_explicit.txt")


@httpretty.activate(allow_net_connect=False)
def test_job_logs():
    runner = CliRunner(mix_stderr=False)
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        GQL_ENDPOINT + "/",
        body=gql_job_logs(),
    )

    httpretty.register_uri(
        httpretty.HTTPretty.GET,
        re.compile(re.escape(STORAGE_ENDPOINT + "/") + ".*"),
        body=job_log_callback,
    )

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ), patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
        result = runner.invoke(
            logs_c,
            ["someuser/somerepo_1", "sometask"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        assert result.stdout == "Logs for /someuser/somerepo_1/sometask\n"

        with pytest.raises(requests.exceptions.HTTPError, match="404 Client Error: Not Found"):
            runner.invoke(logs_c, ["someuser/somerepo_1", "notfound"], catch_exceptions=False)


def test_deduplicate_items():
    assert _deduplicate_items(["item", "otheritem", "otheritem"]) == [
        "item",
        "otheritem_000",
        "otheritem_001",
    ]
    assert _deduplicate_items(["otheritem", "item"]) == ["otheritem", "item"]


@httpretty.activate(allow_net_connect=False)
@pytest.mark.parametrize("success", [True, False])
def test_csv_upload(success, snapshot):
    gql_upload_cb, file_upload_cb = gql_upload(
        namespace="someuser",
        repository="somerepo_1",
        final_status="SUCCESS" if success else "FAILURE",
    )

    runner = CliRunner(mix_stderr=False)
    httpretty.register_uri(
        httpretty.HTTPretty.POST,
        GQL_ENDPOINT + "/",
        body=gql_upload_cb,
    )

    httpretty.register_uri(
        httpretty.HTTPretty.PUT,
        re.compile(re.escape(STORAGE_ENDPOINT + "/") + ".*"),
        body=file_upload_cb,
    )

    httpretty.register_uri(
        httpretty.HTTPretty.GET,
        re.compile(re.escape(STORAGE_ENDPOINT + "/") + ".*"),
        body=job_log_callback,
    )

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ), patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT), patch(
        "splitgraph.commandline.cloud.GQL_POLL_TIME", 0
    ):
        # Also patch out the poll frequency so that we don't wait between calls to the job
        # status endpoint.
        result = runner.invoke(
            upload_c,
            [
                "someuser/somerepo_1",
                os.path.join(RESOURCES, "ingestion", "csv", "base_df.csv"),
                os.path.join(RESOURCES, "ingestion", "csv", "patch_df.csv"),
            ],
        )

        if success:
            assert result.exit_code == 0
            snapshot.assert_match(result.stdout, "sgr_cloud_upload_success.txt")
        else:
            assert result.exit_code == 1
            snapshot.assert_match(result.stdout, "sgr_cloud_upload_failure.txt")


@httpretty.activate(allow_net_connect=False)
def test_sync_existing():
    gql_sync_cb = gql_sync("someuser", "somerepo_1")
    runner = CliRunner(mix_stderr=False)
    httpretty.register_uri(httpretty.HTTPretty.POST, GQL_ENDPOINT + "/", body=gql_sync_cb)

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ), patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
        result = runner.invoke(
            sync_c,
            [
                "someuser/somerepo_1",
            ],
        )
        assert result.exit_code == 0
        assert "Started task ingest_task" in result.stdout


@httpretty.activate(allow_net_connect=False)
def test_sync_yaml_file():
    gql_sync_cb = gql_sync("otheruser", "somerepo_2", is_existing=False)
    runner = CliRunner(mix_stderr=False)
    httpretty.register_uri(httpretty.HTTPretty.POST, GQL_ENDPOINT + "/", body=gql_sync_cb)

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ), patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT):
        result = runner.invoke(
            sync_c,
            [
                "--use-file",
                "--repositories-file",
                os.path.join(RESOURCES, "repositories_yml", "repositories.yml"),
                "otheruser/somerepo_2",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
        assert "Started task ingest_task" in result.stdout


@httpretty.activate(allow_net_connect=False)
def test_sync_wait():
    gql_sync_cb = gql_sync("someuser", "somerepo_1")
    runner = CliRunner(mix_stderr=False)
    httpretty.register_uri(httpretty.HTTPretty.POST, GQL_ENDPOINT + "/", body=gql_sync_cb)

    with patch(
        "splitgraph.cloud.RESTAPIClient.access_token",
        new_callable=PropertyMock,
        return_value=ACCESS_TOKEN,
    ), patch("splitgraph.cloud.get_remote_param", return_value=GQL_ENDPOINT), patch(
        "splitgraph.commandline.cloud.wait_for_load"
    ) as wfl:
        result = runner.invoke(
            sync_c,
            [
                "--wait",
                "someuser/somerepo_1",
            ],
        )
        assert result.exit_code == 0

        assert wfl.mock_calls == [call(mock.ANY, "someuser", "somerepo_1", "ingest_task")]


def test_get_external_from_yaml():
    path = Path(os.path.join(RESOURCES, "repositories_yml", "repositories.yml"))

    with pytest.raises(click.UsageError, match="Repository doesnt/exist not found"):
        _get_external_from_yaml(path, Repository("doesnt", "exist"))

    with pytest.raises(click.UsageError, match="Credential my_other_credential not defined"):
        _get_external_from_yaml(path, Repository("someuser", "somerepo_1"))

    external, credentials = _get_external_from_yaml(path, Repository("someuser", "somerepo_2"))
    assert external.plugin == "plugin_3"
    assert credentials is None

    external, credentials = _get_external_from_yaml(path, Repository("otheruser", "somerepo_2"))
    assert external.plugin == "plugin"
    assert credentials == {"username": "my_username", "password": "secret"}
