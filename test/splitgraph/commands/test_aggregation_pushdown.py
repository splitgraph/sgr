import json
import pytest
from decimal import Decimal

from test.splitgraph.conftest import _mount_elasticsearch
from splitgraph.engine import get_engine, ResultShape


def _extract_query_from_explain(result):
    query_str = ""

    for o in result:
        if query_str != "":
            query_str += o[0] + "\n"
        elif "Multicorn: Query:" in o[0]:
            query_str = "{"

        if o == ("}",):
            break

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


@pytest.mark.mounting
def test_elasticsearch_gropuing_clauses_only(local_engine_empty):
    _mount_elasticsearch()

    # Single column grouping
    query = "SELECT state FROM es.account GROUP BY state"

    # Ensure grouping is going to be pushed down
    result = get_engine().run_sql("EXPLAIN " + query)
    assert len(result) > 2
    assert _extract_query_from_explain(result) == {
        "aggs": {
            "group_buckets": {
                "composite": {"sources": [{"state": {"terms": {"field": "state"}}}], "size": 5}
            }
        }
    }

    # Ensure results are correct
    result = get_engine().run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 51

    # Assert aggregation result
    assert result == [
        "AK",
        "AL",
        "AR",
        "AZ",
        "CA",
        "CO",
        "CT",
        "DC",
        "DE",
        "FL",
        "GA",
        "HI",
        "IA",
        "ID",
        "IL",
        "IN",
        "KS",
        "KY",
        "LA",
        "MA",
        "MD",
        "ME",
        "MI",
        "MN",
        "MO",
        "MS",
        "MT",
        "NC",
        "ND",
        "NE",
        "NH",
        "NJ",
        "NM",
        "NV",
        "NY",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VA",
        "VT",
        "WA",
        "WI",
        "WV",
        "WY",
    ]

    # Multi-column grouping
    query = "SELECT gender, age FROM es.account GROUP BY age, gender"

    # Ensure grouping is going to be pushed down
    result = get_engine().run_sql("EXPLAIN " + query)
    assert len(result) > 2
    assert _extract_query_from_explain(result) == {
        "aggs": {
            "group_buckets": {
                "composite": {
                    "sources": [
                        {"gender": {"terms": {"field": "gender"}}},
                        {"age": {"terms": {"field": "age"}}},
                    ],
                    "size": 5,
                }
            }
        }
    }

    # Ensure results are correct
    result = get_engine().run_sql(query)
    assert len(result) == 42

    # Assert aggregation result
    gender = "F"
    age = 20
    for row in result:
        assert row == (gender, age)

        age += 1
        if age == 41:
            age = 20
            gender = "M"


@pytest.mark.mounting
def test_elasticsearch_gropuing_and_aggregations_bare(local_engine_empty):
    _mount_elasticsearch()

    # Single column grouping
    query = "SELECT gender, avg(balance), avg(age) FROM es.account GROUP BY gender"

    # Ensure query is going to be pushed down
    result = get_engine().run_sql("EXPLAIN " + query)
    assert len(result) > 2
    assert _extract_query_from_explain(result) == {
        "aggs": {
            "group_buckets": {
                "composite": {"sources": [{"gender": {"terms": {"field": "gender"}}}], "size": 5},
                "aggregations": {
                    "avg.balance": {"avg": {"field": "balance"}},
                    "avg.age": {"avg": {"field": "age"}},
                },
            }
        }
    }

    # Ensure results are correct
    result = get_engine().run_sql(query)
    assert len(result) == 2

    # Assert aggregation result
    assert result == [
        ("F", 25623.34685598377, Decimal("30.3184584178499")),
        ("M", 25803.800788954635, Decimal("30.027613412228796")),
    ]


@pytest.mark.mounting
def test_elasticsearch_not_pushed_down(local_engine_empty):
    _mount_elasticsearch()

    _bare_filtering_query = {"query": {"bool": {"must": [{"range": {"age": {"gt": 30}}}]}}}

    # Aggregation only filtering is not going to be pushed down
    result = get_engine().run_sql(
        "EXPLAIN SELECT max(balance), avg(age) FROM es.account WHERE age > 30"
    )
    assert len(result) > 2
    assert _extract_query_from_explain(result) == _bare_filtering_query

    # Grouping only filtering is not going to be pushed down
    result = get_engine().run_sql("EXPLAIN SELECT age FROM es.account WHERE age > 30 GROUP BY age")
    assert len(result) > 2
    assert _extract_query_from_explain(result) == _bare_filtering_query

    # Aggregation and grouping filtering is not going to be pushed down
    result = get_engine().run_sql(
        "EXPLAIN SELECT age, min(balance) FROM es.account WHERE age > 30 GROUP BY age"
    )
    assert len(result) > 2
    assert _extract_query_from_explain(result) == _bare_filtering_query

    _bare_sequential_scan = {"query": {"bool": {"must": []}}}

    # SELECT STAR is not going to be pushed down
    result = get_engine().run_sql("EXPLAIN SELECT * FROM es.account")
    assert len(result) > 2
    assert _extract_query_from_explain(result) == _bare_sequential_scan

    # DISTINCT queries are not going to be pushed down
    result = get_engine().run_sql("EXPLAIN SELECT COUNT(DISTINCT city) FROM es.account")
    assert len(result) > 2
    assert _extract_query_from_explain(result) == _bare_sequential_scan
