import datetime
import os
from test.splitgraph.conftest import INGESTION_RESOURCES
from unittest.mock import MagicMock

from click.testing import CliRunner
from splitgraph.commandline.ingestion import csv_import
from splitgraph.core.repository import Repository
from splitgraph.core.types import Credentials, Params
from splitgraph.engine import ResultShape
from splitgraph.engine.postgres.engine import PsycopgEngine
from splitgraph.ingestion.dbt.data_source import DBTDataSource

_REPO_PATH = "https://github.com/splitgraph/jaffle_shop_archive"


def test_dbt_data_source_params_parsing():
    engine = MagicMock(PsycopgEngine)

    source = DBTDataSource(
        engine,
        credentials=Credentials({"git_url": _REPO_PATH}),
        params=Params(
            {
                "sources": [
                    {
                        "dbt_source_name": "raw_jaffle_shop",
                        "namespace": "ingestion-raw",
                        "repository": "jaffle-shop",
                        "hash_or_tag": "test-branch",
                    },
                    {
                        "dbt_source_name": "other_source",
                        "namespace": "other-ns",
                        "repository": "other-repo",
                    },
                ],
            }
        ),
    )

    assert source.source_map == {
        "other_source": (
            "other-ns",
            "other-repo",
            "latest",
        ),
        "raw_jaffle_shop": (
            "ingestion-raw",
            "jaffle-shop",
            "test-branch",
        ),
    }
    assert source.git_branch == "master"


def test_dbt_data_source_introspection(local_engine_empty):
    # We can do introspection without the source map defined, but we do need an engine connection.
    # Use the branch with the v2 config version
    source = DBTDataSource(
        local_engine_empty,
        credentials=Credentials({"git_url": _REPO_PATH}),
        params=Params({"git_branch": "sg-integration-test"}),
    )

    result = source.introspect()
    assert len(result) == 5
    # We currently don't return a table schema (we can't know it) or params (pointless, as we
    # don't let the user remap dbt model names to other table names).
    assert result["customer_orders"] == ([], {})


def test_dbt_data_source_load(local_engine_empty):
    # Make a local Splitgraph repo out of the CSV files
    basedir = os.path.join(INGESTION_RESOURCES, "dbt", "jaffle_csv")
    repo = Repository("test", "raw-jaffle-data")
    repo.init()
    repo.commit_engines()
    tables = ["customers", "orders", "payments"]

    for table in tables:
        runner = CliRunner()
        result = runner.invoke(
            csv_import,
            [
                str(repo),
                table,
                "-f",
                os.path.join(basedir, f"raw_{table}.csv"),
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0
    repo.commit()

    assert sorted(repo.images["latest"].get_tables()) == tables

    # Set up the data source

    source = DBTDataSource(
        local_engine_empty,
        credentials=Credentials({"git_url": _REPO_PATH}),
        params=Params(
            {
                "sources": [
                    {
                        "dbt_source_name": "raw_jaffle_shop",
                        "namespace": repo.namespace,
                        "repository": repo.repository,
                    },
                ],
                "git_branch": "sg-integration-test",
            }
        ),
    )

    assert source.get_required_images() == [(repo.namespace, repo.repository, "latest")]
    target_repo = Repository("test", "jaffle-processed")

    # Test build of one model (including its parents)
    source.load(repository=target_repo, tables=["fct_orders"])
    result = target_repo.images["latest"]
    # fct_orders depends on order_payments, so we pull it here too
    assert sorted(result.get_tables()) == ["fct_orders", "order_payments"]

    with result.query_schema() as s:
        assert (
            result.engine.run_sql_in(
                s, "SELECT COUNT(1) FROM fct_orders", return_shape=ResultShape.ONE_ONE
            )
            == 99
        )
        assert result.engine.run_sql_in(
            s, "SELECT * FROM fct_orders ORDER BY order_date DESC LIMIT 1"
        ) == [
            (
                99,
                85,
                datetime.date(2018, 4, 9),
                "placed",
                24,
                0,
                0,
                0,
                24,
            ),
        ]

        assert (
            result.engine.run_sql_in(
                s, "SELECT COUNT(1) FROM order_payments", return_shape=ResultShape.ONE_ONE
            )
            == 99
        )

    # Test build of all models
    source.load(repository=target_repo)
    result = target_repo.images["latest"]
    assert sorted(result.get_tables()) == [
        "customer_orders",
        "customer_payments",
        "dim_customers",
        "fct_orders",
        "order_payments",
    ]
