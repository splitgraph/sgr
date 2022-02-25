import base64
from unittest.mock import Mock

import pytest
from psycopg2 import DatabaseError

from splitgraph.core.types import Credentials, Params
from splitgraph.hooks.mount_handlers import mount
from splitgraph.ingestion.big_query import BigQueryDataSource


def test_big_query_data_source_options_creds_file():
    source = BigQueryDataSource(
        Mock(),
        credentials=Credentials(
            {"credentials_path": "test/resources/ingestion/big_query/dummy_credentials.json"}
        ),
        params=Params(
            {
                "project": "bigquery-public-data",
                "dataset_name": "hacker_news",
            }
        ),
    )

    with open("test/resources/ingestion/big_query/dummy_credentials.json", "r") as credentials_file:
        credentials_str = credentials_file.read()
        credentials_base64 = base64.urlsafe_b64encode(credentials_str.encode()).decode()

    assert source.get_server_options() == {
        "db_url": f"bigquery://bigquery-public-data/hacker_news?credentials_base64={credentials_base64}",
        "wrapper": "multicorn.sqlalchemyfdw.SqlAlchemyFdw",
    }


def test_big_query_data_source_options_no_creds_file():
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
def test_big_query_mount_expected_error():
    with pytest.raises(DatabaseError, match="Could not automatically determine credentials"):
        mount(
            "bq",
            "big_query",
            {"project": "bigquery-public-data", "dataset_name": "hacker_news"},
        )
