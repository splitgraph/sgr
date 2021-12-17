import json
import pytest
from decimal import Decimal

from test.splitgraph.conftest import _mount_elasticsearch
from splitgraph.core.repository import Repository
from splitgraph.engine import get_engine


def _extract_query_from_explain(result):
    query_str = "{"

    for o in result[3:]:
        query_str += o[0] + "\n"

    return json.loads(query_str)


@pytest.mark.mounting
def test_elasticsearch_aggregation_functions_only(local_engine_empty):
    _mount_elasticsearch()

    query = """SELECT max(account_number), avg(balance), max(balance),
        sum(balance), min(age), avg(age)
        FROM es.account
    """

    # Ensure query is going to be aggregated on the foreign server
    result = get_engine().run_sql("EXPLAIN " + query)
    assert len(result) > 2
    assert _extract_query_from_explain(result) == {
        "aggs": {
            "max.account_number": {"max": {"field": "account_number"}},
            "avg.balance": {"avg": {"field": "balance"}},
            "max.balance": {"max": {"field": "balance"}},
            "sum.balance": {"sum": {"field": "balance"}},
            "min.age": {"min": {"field": "age"}},
            "avg.age": {"avg": {"field": "age"}},
        }
    }

    # Ensure results are correct
    result = get_engine().run_sql(query)
    assert len(result) == 1

    # Assert aggregation result
    assert result[0] == (999, 25714.837, 49989, 25714837, 20, Decimal("30.171"))
