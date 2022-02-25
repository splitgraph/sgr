import base64
from unittest.mock import Mock

import pytest
from psycopg2 import DatabaseError

from splitgraph.core.types import Credentials, Params
from splitgraph.hooks.mount_handlers import mount
from splitgraph.ingestion.bigquery import BigQueryDataSource


def test_bigquery_data_source_options_creds_file(local_engine_empty):
    source = BigQueryDataSource.from_commandline(
        local_engine_empty,
        {
            "credentials": {
                "secret_type": "file",
                "path": "test/resources/ingestion/bigquery/dummy_credentials.json",
            },
            "project": "bigquery-public-data",
            "dataset_name": "hacker_news",
        },
    )

    with open("test/resources/ingestion/bigquery/dummy_credentials.json", "r") as credentials_file:
        credentials_str = credentials_file.read()
        credentials_base64 = base64.urlsafe_b64encode(credentials_str.encode()).decode()

    assert source.get_server_options() == {
        "db_url": f"bigquery://bigquery-public-data/hacker_news?credentials_base64={credentials_base64}",
        "wrapper": "multicorn.sqlalchemyfdw.SqlAlchemyFdw",
    }


def test_bigquery_data_source_options_creds_raw():
    source = BigQueryDataSource(
        Mock(),
        credentials=Credentials(
            {"credentials": {"secret_type": "raw", "credentials_str": "test-raw-creds"}}
        ),
        params=Params(
            {
                "project": "bigquery-public-data",
                "dataset_name": "hacker_news",
            }
        ),
    )

    credentials_base64 = base64.urlsafe_b64encode("test-raw-creds".encode()).decode()

    assert source.get_server_options() == {
        "db_url": f"bigquery://bigquery-public-data/hacker_news?credentials_base64={credentials_base64}",
        "wrapper": "multicorn.sqlalchemyfdw.SqlAlchemyFdw",
    }


def test_bigquery_data_source_options_no_creds_file():
    source = BigQueryDataSource(
        Mock(),
        credentials=Credentials({}),
        params=Params(
            {
                "project": "bigquery-public-data",
                "dataset_name": "hacker_news",
            }
        ),
    )

    assert source.get_server_options() == {
        "db_url": "bigquery://bigquery-public-data/hacker_news",
        "wrapper": "multicorn.sqlalchemyfdw.SqlAlchemyFdw",
    }


@pytest.mark.mounting
def test_bigquery_mount_expected_error():
    with pytest.raises(DatabaseError, match="Could not automatically determine credentials"):
        mount(
            "bq",
            "bigquery",
            {"project": "bigquery-public-data", "dataset_name": "hacker_news"},
        )
