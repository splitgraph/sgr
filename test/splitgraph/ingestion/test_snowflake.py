from unittest.mock import Mock

from splitgraph.ingestion.snowflake import SnowflakeDataSource


def test_snowflake_data_source_dburl_conversion():
    source = SnowflakeDataSource(
        Mock(),
        credentials={
            "username": "username",
            "password": "password",
            "account": "abcdef.eu-west-1.aws",
        },
        params={
            "database": "SOME_DB",
            "schema": "TPCH_SF100",
            "warehouse": "my_warehouse",
            "role": "role",
        },
    )

    assert source.get_server_options() == {
        "db_url": "snowflake://username:password@abcdef.eu-west-1.aws/SOME_DB/TPCH_SF100warehouse=my_warehouse&role=role",
        "wrapper": "multicorn.sqlalchemyfdw.SqlAlchemyFdw",
    }

    source = SnowflakeDataSource(
        Mock(),
        credentials={
            "username": "username",
            "password": "password",
            "account": "abcdef.eu-west-1.aws",
        },
        params={
            "database": "SOME_DB",
            "tables": {
                "test_table": {
                    "schema": {"col_1": "int", "col_2": "varchar"},
                    "options": {"subquery": "SELECT col_1, col_2 FROM other_table"},
                }
            },
        },
    )

    assert source.get_table_options("test_table") == {
        "subquery": "SELECT col_1, col_2 FROM other_table",
        "tablename": "test_table",
    }
