from unittest.mock import MagicMock

from splitgraph.core.types import Params, Credentials
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
