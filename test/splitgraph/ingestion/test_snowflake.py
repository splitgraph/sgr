import json
from unittest import mock
from unittest.mock import Mock

import pytest

from splitgraph.ingestion.snowflake import SnowflakeDataSource

_sample_privkey = """-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDWG8iggSJb3lIt
RXKuiCdRP72ehXK1oi1f7yxM11NwtJlyIqghbZ3czBMdTBjT2QYU2nDkT0oElRuP
W7StIDIiKvJmlDLZtv2owNdW6On+eZmo1dRmd7vXSYWG+vjOK6stA4eu0Nmt9ZNM
H/hgkm5n0pjuco7L2o9/qATPX227Tkh5aXTbhKSbcJAaFnC5Q71bE9jJCmpmFnUX
5M00fnI1eJK1ZGEX6cB2g34t//lFjWSbqb8MDM4+7V3OGaUDFG25AXPivNyMDFZe
EOYR4aev+9A6aqnOJuzXUbxxnw2s2HShcIWq8TrSn+p9EqoXpqP3SXywpP+mgBL5
q0ojL0y7AgMBAAECggEBAJJQtlAJL2O8kEfjt7VR0hySBJD5/SPmyj9PAOUaGSli
IaJ/0InXkRO8WiuhPy42lxNVG+TJ0nlDNGxJbTUKVXhIBRLYn89sX/gcoIwB6zY9
/yYDynyjwjgjRB60D6dE7Ft8mBJ9IuTgd2KEToYgS1aj8mKw4qiomXvRZEganLBs
VCYUydSP5p0boqleubPdO0Vc2EWHDDxS+4R8j5/BOQzeRBlWkIxP1ocxV5aotE3E
KJSzs5bYqrTYmr7udoY5JWK/q0LpHwMECIulWIjLj4X2VBfhvqiWu+Me2IYiLJ69
xNV1zqPQ1KQFcVGziEWaVaNRUJPHrOHazSrFKFIT3LECgYEA+i/DRtxsjfGT3J54
cMf8BV/1AZ5f2aReiteraj3NgQhD0PKO2g7/MyHvbVyVhK7iRPRJ9oBr4kztbAkM
M6AMQvNAQ2IIW6H1SBJPGCBJte2ar0/7fnKYSk/9ayk3h4D6bFsut9qoPXk4n0wu
Zds3aoRsNDNtOKfUhsp9ASDcR48CgYEA2xVpCoPJ3PLDcuWemXv4Nor1g/BDaiGO
/Sa39m+9xppOk5mvnoAArg4CLPbGQAJp7JTdKHU+NSjX/cyLiuZDbA/i1Y6GPPYC
ABZtPWgEwPJfVVqStXS+4yHsDAV0KeuPLQKkyGt30xRo93GpTLlCMpGK9f0P5fG9
/kSjF7TkshUCgYAJJY3iHVTqq5ZYToLgvK7+E1AFyyB9+IBsWw4tSC0nNoIkNXn7
hujVmbwDJ4tf2nTzSGsb0/4du+pCNOJ5ULSiDfqffAoKL5WkGOdDXorTV+h72FS9
frsKnHoLXOpmzdRZ+ctvdVMJTFFBoatgle8kucqq7eZkV95xPx3q2KS1CQKBgQCj
+dZxBErmkN0w9iRLBLq3ODKi2gXbPdrkJ0KxtNj5+Syu1OzZWT0pCVsfhGTGLAhU
BuexDG/PIg7n61zWTZpRG2LQLKjUn9zHbAG/YEeOkto/7Fa6cfMd1ZnzNXHInoK/
Uac8SxOYbUJTUkNBJbgiWUUE8LAhj1qBIaZgbAhwMQKBgQCSq0E7GfKhDHFmtxH0
C82sEfK+SpG9f2xBA/K+aFnZRPOr1EJth21W8h9U0sSWRVTWpimqHzlmqnz1xzse
5WqI23eaKsWo6wL+K2SrxIZj+cegozyR0LN+3LBiliXg4a8GbjJLeIuWjSwBKrEM
BwzG3ZjEjRTm10AOOGb62hi9JA==
-----END PRIVATE KEY-----
"""

_sample_privkey_b64 = "".join(l for l in _sample_privkey.strip().split("\n")[1:-1] if l)


def test_snowflake_data_source_dburl_conversion_warehouse():
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
        "db_url": "snowflake://username:password@abcdef.eu-west-1.aws/SOME_DB/TPCH_SF100?warehouse=my_warehouse&role=role",
        "schema": "TPCH_SF100",
        "wrapper": "multicorn.sqlalchemyfdw.SqlAlchemyFdw",
    }


def test_snowflake_data_source_dburl_conversion_no_warehouse():
    source = SnowflakeDataSource(
        Mock(),
        credentials={
            "username": "username",
            "password": "password",
            "account": "abcdef.eu-west-1.aws",
        },
        params={"database": "SOME_DB", "schema": "TPCH_SF100",},
    )

    assert source.get_server_options() == {
        "db_url": "snowflake://username:password@abcdef.eu-west-1.aws/SOME_DB/TPCH_SF100",
        "schema": "TPCH_SF100",
        "wrapper": "multicorn.sqlalchemyfdw.SqlAlchemyFdw",
    }


@pytest.mark.parametrize("private_key", [_sample_privkey, _sample_privkey_b64])
def test_snowflake_data_source_private_key(private_key):
    source = SnowflakeDataSource(
        Mock(),
        credentials={
            "username": "username",
            "private_key": private_key,
            "account": "abcdef.eu-west-1.aws",
        },
        params={"database": "SOME_DB", "schema": "TPCH_SF100",},
    )

    opts = source.get_server_options()

    assert opts == {
        "db_url": "snowflake://username@abcdef.eu-west-1.aws/SOME_DB/TPCH_SF100",
        "schema": "TPCH_SF100",
        "wrapper": "multicorn.sqlalchemyfdw.SqlAlchemyFdw",
        "connect_args": mock.ANY,
    }

    assert json.loads(opts["connect_args"])["private_key"] == _sample_privkey_b64


def test_snowflake_data_source_table_options():
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
