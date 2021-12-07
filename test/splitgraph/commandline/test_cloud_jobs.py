import os
import re
from test.splitgraph.commandline.http_fixtures import (
    ACCESS_TOKEN,
    GQL_ENDPOINT,
    LOGS_ENDPOINT,
    gql_job_logs,
    gql_job_status,
    job_log_callback,
)
from test.splitgraph.conftest import RESOURCES
from unittest.mock import PropertyMock, patch

import httpretty
import pytest
import requests
from click.testing import CliRunner
from splitgraph.commandline.cloud import logs_c, status_c


@httpretty.activate(allow_net_connect=False)
def test_job_status_yaml():
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

        assert (
            result.stdout
            == """Repository            Task ID         Started              Finished             Manual    Status
--------------------  --------------  -------------------  -------------------  --------  --------
otheruser/somerepo_2
someuser/somerepo_1   somerepo1_task  2020-01-01 00:00:00                       False     STARTED
someuser/somerepo_2   somerepo2_task  2021-01-01 00:00:00  2021-01-01 01:00:00  False     SUCCESS
"""
        )


@httpretty.activate(allow_net_connect=False)
def test_job_status_explicit_repos():
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

        assert (
            result.stdout
            == """Repository            Task ID         Started              Finished    Manual    Status
--------------------  --------------  -------------------  ----------  --------  --------
someuser/somerepo_1   somerepo1_task  2020-01-01 00:00:00              False     STARTED
otheruser/somerepo_2
"""
        )


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
        re.compile(re.escape(LOGS_ENDPOINT + "/") + ".*"),
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
            result = runner.invoke(
                logs_c, ["someuser/somerepo_1", "notfound"], catch_exceptions=False
            )
