from unittest.mock import Mock

import pytest
from psycopg2 import DatabaseError

from splitgraph.core.types import Credentials, Params
from splitgraph.hooks.mount_handlers import mount
from splitgraph.ingestion.athena import AmazonAthenaDataSource


def test_athena_data_source_options():
    source = AmazonAthenaDataSource(
        Mock(),
        credentials=Credentials({"aws_access_key_id": "key", "aws_secret_access_key": "secret"}),
        params=Params(
            {
                "region_name": "eu-west-3",
                "schema_name": "mydb",
                "s3_staging_dir": "s3://athena/results/",
            }
        ),
    )

    assert source.get_server_options() == {
        "db_url": "awsathena+rest://key:secret@athena.eu-west-3.amazonaws.com:443/mydb?s3_staging_dir=s3://athena/results/",
        "wrapper": "multicorn.sqlalchemyfdw.SqlAlchemyFdw",
        "cast_quals": "yes",
    }


@pytest.mark.mounting
def test_athena_mount_expected_error():
    with pytest.raises(
        DatabaseError, match="The security token included in the request is invalid"
    ):
        mount(
            "aws",
            "athena",
            {
                "aws_access_key_id": "key",
                "aws_secret_access_key": "secret",
                "region_name": "eu-west-3",
                "schema_name": "mydb",
                "s3_staging_dir": "s3://athena/results/",
            },
        )
