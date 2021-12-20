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

    # Aggregations functions and grouping bare combination
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

    # We support pushing down aggregation queries with sorting, with the caveat
    # that the sorting operation is performed on the PG side for now
    query = "SELECT age, COUNT(account_number) FROM es.account GROUP BY age ORDER BY age DESC"

    # Ensure query is going to be pushed down
    result = get_engine().run_sql("EXPLAIN " + query)
    assert len(result) > 2
    assert _extract_query_from_explain(result) == {
        "aggs": {
            "group_buckets": {
                "composite": {"sources": [{"age": {"terms": {"field": "age"}}}], "size": 5},
                "aggregations": {
                    "count.account_number": {"value_count": {"field": "account_number"}}
                },
            }
        }
    }

    # Ensure results are correct
    result = get_engine().run_sql(query)
    assert len(result) == 21

    # Assert aggregation result
    assert result == [
        (40, 45),
        (39, 60),
        (38, 39),
        (37, 42),
        (36, 52),
        (35, 52),
        (34, 49),
        (33, 50),
        (32, 52),
        (31, 61),
        (30, 47),
        (29, 35),
        (28, 51),
        (27, 39),
        (26, 59),
        (25, 42),
        (24, 42),
        (23, 42),
        (22, 51),
        (21, 46),
        (20, 44),
    ]


@pytest.mark.mounting
def test_elasticsearch_agg_subquery_pushdown(local_engine_empty):
    """
    Most of the magic in these examples is coming from PG, not our Multicorn code
    (i.e. discarding redundant targets from subqueries).
    Here we just make sure that we don't break that somehow.
    """

    _mount_elasticsearch()

    # DISTINCT on a grouping clause from a subquery
    query = """SELECT DISTINCT gender FROM(
        SELECT state, gender, min(age), max(balance)
        FROM es.account GROUP BY state, gender
    ) AS t"""

    # Ensure only the relevant part is pushed down (i.e. no aggregations as they are redundant)
    result = get_engine().run_sql("EXPLAIN " + query)
    assert len(result) > 2
    assert _extract_query_from_explain(result) == {
        "aggs": {
            "group_buckets": {
                "composite": {
                    "sources": [
                        {"state": {"terms": {"field": "state"}}},
                        {"gender": {"terms": {"field": "gender"}}},
                    ],
                    "size": 5,
                }
            }
        }
    }

    # Ensure results are correct
    result = get_engine().run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 2

    # Assert aggregation result
    assert result == ["F", "M"]

    # DISTINCT on a aggregated column from a subquery
    query = """SELECT DISTINCT min FROM(
        SELECT state, gender, min(age), max(balance)
        FROM es.account GROUP BY state, gender
    ) AS t"""

    # Ensure only the relevant part is pushed down (no redundant aggregations, i.e. only min)
    result = get_engine().run_sql("EXPLAIN " + query)
    assert len(result) > 2
    assert _extract_query_from_explain(result) == {
        "aggs": {
            "group_buckets": {
                "composite": {
                    "sources": [
                        {"state": {"terms": {"field": "state"}}},
                        {"gender": {"terms": {"field": "gender"}}},
                    ],
                    "size": 5,
                },
                "aggregations": {"min.age": {"min": {"field": "age"}}},
            }
        }
    }

    # Ensure results are correct
    result = get_engine().run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 8

    # Assert aggregation result
    assert result == [20, 21, 22, 23, 24, 25, 26, 28]


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

    # Grouping only post-aggregation filtering is not going to be pushed down either
    result = get_engine().run_sql("EXPLAIN SELECT age FROM es.account GROUP BY age HAVING age > 30")
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
