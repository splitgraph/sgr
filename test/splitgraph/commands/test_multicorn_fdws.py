import json
import math
from decimal import Decimal
from test.splitgraph.conftest import _mount_elasticsearch

import pytest
import yaml

from splitgraph.engine import ResultShape


def _extract_es_queries_from_explain(result):
    queries = []
    query_str = ""

    for o in result:
        if query_str != "":
            query_str += o[0] + "\n"
        elif "Multicorn: Query:" in o[0]:
            query_str = "{"

        if o == ("}",):
            queries.append(json.loads(query_str))
            query_str = ""

    return queries


def _extract_pg_queries_from_explain(result):
    queries = []
    query_str = None

    for o in result:
        if query_str is not None:
            query_str += o[0]
        elif "Multicorn: " in o[0]:
            query_str = ""

        if o == ("",):
            queries.append(query_str)
            query_str = None

    return queries


_bare_es_sequential_scan = {"query": {"bool": {"must": []}}}


@pytest.mark.mounting
def test_es_specific_pattern_matching_queries(local_engine_empty):
    _mount_elasticsearch()

    # Test pattern matching conversion mechanism on generic examples
    query = r"""
    SELECT *
    FROM es.account
    WHERE firstname ~~ 'any: %; percent: \%; backslash-and-any: \\%; backslash-and-percent: \\\%'
    """

    # Ensure proper query translation
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_es_queries_from_explain(result)[0] == {
        "query": {
            "bool": {
                "must": [
                    {
                        "wildcard": {
                            "firstname": r"any: *; percent: %; backslash-and-any: \*; backslash-and-percent: \%"
                        }
                    }
                ]
            }
        }
    }

    query = r"""
    SELECT *
    FROM es.account
    WHERE firstname ~~ 'single-char: _; underscore: \_; backslash-and-single-char: \\_; backslash-and-underscore: \\\_'
    """

    # Ensure proper query translation
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_es_queries_from_explain(result)[0] == {
        "query": {
            "bool": {
                "must": [
                    {
                        "wildcard": {
                            "firstname": r"single-char: ?; underscore: _; backslash-and-single-char: \?; backslash-and-underscore: \_"
                        }
                    }
                ]
            }
        }
    }

    # Special ES pattern characters are escaped
    query = r"""
    SELECT *
    FROM es.account
    WHERE firstname ~~ 'star: *; question-mark: ?; multiple-stars-and-question-marks: ****??'
    """

    # Ensure proper query translation
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_es_queries_from_explain(result)[0] == {
        "query": {
            "bool": {
                "must": [
                    {
                        "wildcard": {
                            "firstname": r"star: \*; question-mark: \?; multiple-stars-and-question-marks: \*\*\*\*\?\?"
                        }
                    }
                ]
            }
        }
    }


@pytest.mark.mounting
@pytest.mark.parametrize("data_source", ["es", "pg"])
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_pattern_matching_queries(data_source, local_engine_empty):
    # Test meaningful pattern match query returns correct result
    query = f"SELECT firstname FROM {data_source}.account WHERE firstname ~~ 'Su_an%'"

    # Ensure proper query translation
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": [{"wildcard": {"firstname": "Su?an*"}}]}}
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.firstname FROM public.account "
            "WHERE public.account.firstname LIKE 'Su_an%%'"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 4
    assert set(result) == {"Susan", "Susana", "Susanne", "Suzanne"}

    # Test meaningful pattern match query returns correct result
    query = f"SELECT firstname FROM {data_source}.account WHERE firstname !~~ 'Su_an%'"

    # Ensure proper query translation
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": [{"match_all": {}}]}}
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.firstname FROM public.account "
            "WHERE public.account.firstname NOT LIKE 'Su_an%%'"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 996
    assert not {"Susan", "Susana", "Susanne", "Suzanne"}.issubset(set(result))


@pytest.mark.mounting
@pytest.mark.parametrize("data_source", ["es", "pg"])
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_simple_aggregation_functions(data_source, local_engine_empty):
    _mount_elasticsearch()

    query = f"""
    SELECT max(account_number), avg(balance), max(balance),
        sum(balance), min(age), avg(age)
    FROM {data_source}.account
    """

    # Ensure query is going to be aggregated on the foreign server
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": []}},
            "aggs": {
                "max.account_number": {"max": {"field": "account_number"}},
                "avg.balance": {"avg": {"field": "balance"}},
                "max.balance": {"max": {"field": "balance"}},
                "sum.balance": {"sum": {"field": "balance"}},
                "min.age": {"min": {"field": "age"}},
                "avg.age": {"avg": {"field": "age"}},
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            'SELECT max(public.account.account_number) AS "max.account_number", '
            'avg(public.account.balance) AS "avg.balance", '
            'max(public.account.balance) AS "max.balance", '
            'sum(public.account.balance) AS "sum.balance", '
            'min(public.account.age) AS "min.age", '
            'avg(public.account.age) AS "avg.age" '
            "FROM public.account"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert len(result) == 1

    # Assert aggregation result
    assert result[0][0] == 999
    assert math.isclose(result[0][1], 25714.837, rel_tol=1e-04)
    assert result[0][2] == 49989
    assert result[0][3] == 25714837
    assert result[0][4] == 20
    assert math.isclose(result[0][5], 30.171, rel_tol=1e-04)

    # Test COUNT(*)
    query = f"SELECT COUNT(*) FROM {data_source}.account"

    # Ensure query is going to be aggregated on the foreign server
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": []}},
            "track_total_hits": True,
            "aggs": {},
        }
    elif data_source == "pg":
        assert (
            _extract_pg_queries_from_explain(result)[0]
            == 'SELECT count(*) AS "count.*" FROM public.account'
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.ONE_ONE)
    assert result == 1000

    # Not like qual operator
    query = f"SELECT COUNT(*) FROM {data_source}.account WHERE firstname !~~ 'Su_an%'"

    result = local_engine_empty.run_sql("EXPLAIN " + query)
    if data_source == "es":
        # Ensure query is not going to be aggregated on the foreign server
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": [{"match_all": {}}]}}
        }
    elif data_source == "pg":
        # Ensure query is going to be aggregated on the foreign server
        assert _extract_pg_queries_from_explain(result)[0] == (
            'SELECT count(*) AS "count.*" '
            "FROM public.account "
            "WHERE public.account.firstname NOT LIKE 'Su_an%%'"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.ONE_ONE)
    assert result == 996


@pytest.mark.mounting
@pytest.mark.parametrize("data_source", ["es", "pg"])
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_simple_aggregation_functions_filtering(data_source, local_engine_empty):
    _mount_elasticsearch()
    query = f"""
    SELECT avg(age), max(balance)
    FROM {data_source}.account
    WHERE balance > 20000 AND age < 30
    """

    # Ensure query is going to be aggregated on the foreign server
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"balance": {"gt": "20000"}}},
                        {"range": {"age": {"lt": 30}}},
                    ]
                }
            },
            "aggs": {
                "avg.age": {"avg": {"field": "age"}},
                "max.balance": {"max": {"field": "balance"}},
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            'SELECT avg(public.account.age) AS "avg.age", max(public.account.balance) AS "max.balance" '
            "FROM public.account "
            "WHERE public.account.balance > 20000 AND public.account.age < 30"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert len(result) == 1

    # Assert aggregation result
    assert math.isclose(result[0][0], 24.4399, rel_tol=1e-05)
    assert result[0][1] == 49795.0

    # Variant with COUNT(*)
    query = f"SELECT avg(balance), COUNT(*) FROM {data_source}.account WHERE firstname ~~ 'Su_an%'"

    # Ensure query is going to be aggregated on the foreign server
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": [{"wildcard": {"firstname": "Su?an*"}}]}},
            "track_total_hits": True,
            "aggs": {
                "avg.balance": {"avg": {"field": "balance"}},
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            'SELECT avg(public.account.balance) AS "avg.balance", count(*) AS "count.*" '
            "FROM public.account WHERE public.account.firstname LIKE 'Su_an%%'"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert len(result) == 1

    # Assert aggregation result
    assert math.isclose(result[0][0], 20321.7500, rel_tol=1e-05)
    assert result[0][1] == 4


@pytest.mark.mounting
@pytest.mark.parametrize("data_source", ["es", "pg"])
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_simple_grouping_clauses(data_source, snapshot, local_engine_empty):
    _mount_elasticsearch()

    # Single column grouping
    query = f"SELECT state FROM {data_source}.account GROUP BY state ORDER BY state"

    # Ensure grouping is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": []}},
            "aggs": {
                "group_buckets": {
                    "composite": {"sources": [{"state": {"terms": {"field": "state"}}}], "size": 5}
                }
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.state " "FROM public.account GROUP BY public.account.state"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 51

    # Assert aggregation result
    snapshot.assert_match(yaml.dump(result), "account_states.yml")

    # Multi-column grouping
    query = (
        f"SELECT gender, age FROM {data_source}.account GROUP BY age, gender ORDER BY age, gender"
    )

    # Ensure grouping is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": []}},
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
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.gender, public.account.age "
            "FROM public.account GROUP BY public.account.gender, public.account.age"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert len(result) == 42

    # Assert aggregation result
    snapshot.assert_match(yaml.dump(result), "account_genders_and_ages.yml")


@pytest.mark.mounting
@pytest.mark.parametrize("data_source", ["es", "pg"])
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_simple_grouping_clauses_filtering(data_source, snapshot, local_engine_empty):
    _mount_elasticsearch()

    # Single column grouping
    query = f"""
        SELECT state, gender FROM {data_source}.account
        WHERE state IN ('TX', 'WA', 'CO')
        GROUP BY state, gender
        ORDER BY state desc, gender
    """

    # Ensure grouping is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    {"term": {"state": "TX"}},
                                    {"term": {"state": "WA"}},
                                    {"term": {"state": "CO"}},
                                ]
                            }
                        }
                    ]
                }
            },
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
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.state, public.account.gender "
            "FROM public.account "
            "WHERE public.account.state IN ('TX', 'WA', 'CO') GROUP BY public.account.state, public.account.gender"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert result == [("WA", "F"), ("WA", "M"), ("TX", "F"), ("TX", "M"), ("CO", "F"), ("CO", "M")]


@pytest.mark.mounting
@pytest.mark.parametrize("data_source", ["es", "pg"])
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_grouping_and_aggregations_bare(data_source, snapshot, local_engine_empty):
    _mount_elasticsearch()

    # Aggregations functions and grouping bare combination
    query = f"SELECT gender, avg(balance), avg(age) FROM {data_source}.account GROUP BY gender ORDER BY gender"

    # Ensure query is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": []}},
            "aggs": {
                "group_buckets": {
                    "composite": {
                        "sources": [{"gender": {"terms": {"field": "gender"}}}],
                        "size": 5,
                    },
                    "aggregations": {
                        "avg.balance": {"avg": {"field": "balance"}},
                        "avg.age": {"avg": {"field": "age"}},
                    },
                }
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.gender, "
            'avg(public.account.balance) AS "avg.balance", '
            'avg(public.account.age) AS "avg.age" '
            "FROM public.account GROUP BY public.account.gender"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert len(result) == 2
    assert result[0][0] == "F"
    assert math.isclose(result[0][1], 25623.3468, rel_tol=1e-05)
    assert math.isclose(result[0][2], 30.3184, rel_tol=1e-05)
    assert result[1][0] == "M"
    assert math.isclose(result[1][1], 25803.8007, rel_tol=1e-05)
    assert math.isclose(result[1][2], 30.0276, rel_tol=1e-05)

    # We support pushing down aggregation queries with sorting, with the caveat
    # that the sorting operation is performed on the PG side for now
    query = f"SELECT age, COUNT(account_number), min(balance) FROM {data_source}.account GROUP BY age ORDER BY age DESC"

    # Ensure query is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": []}},
            "aggs": {
                "group_buckets": {
                    "composite": {"sources": [{"age": {"terms": {"field": "age"}}}], "size": 5},
                    "aggregations": {
                        "count.account_number": {"value_count": {"field": "account_number"}},
                        "min.balance": {"min": {"field": "balance"}},
                    },
                }
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.age, "
            'count(public.account.account_number) AS "count.account_number", '
            'min(public.account.balance) AS "min.balance" '
            "FROM public.account GROUP BY public.account.age"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert len(result) == 21

    # Assert aggregation result
    snapshot.assert_match(yaml.dump(result), "account_count_by_age.yml")


@pytest.mark.mounting
@pytest.mark.parametrize("data_source", ["es", "pg"])
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_grouping_and_aggregations_filtering(data_source, snapshot, local_engine_empty):
    _mount_elasticsearch()

    # Aggregation functions and grouping with filtering
    query = f"""
    SELECT state, age, min(balance), COUNT(*)
    FROM {data_source}.account
    WHERE gender = 'M' AND age = ANY(ARRAY[25, 35])
    GROUP BY state, age
    ORDER BY state, age desc
    """

    # Ensure query is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    {"term": {"age": 25}},
                                    {"term": {"age": 35}},
                                ]
                            }
                        },
                        {"term": {"gender": "M"}},
                    ]
                }
            },
            "aggs": {
                "group_buckets": {
                    "composite": {
                        "sources": [
                            {"state": {"terms": {"field": "state"}}},
                            {"age": {"terms": {"field": "age"}}},
                        ],
                        "size": 5,
                    },
                    "aggregations": {
                        "min.balance": {"min": {"field": "balance"}},
                    },
                }
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.state, "
            "public.account.age, "
            'min(public.account.balance) AS "min.balance", '
            'count(*) AS "count.*" '
            "FROM public.account "
            "WHERE public.account.age IN (25, 35) AND public.account.gender = 'M' "
            "GROUP BY public.account.state, public.account.age"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert len(result) == 45

    # Assert aggregation result
    snapshot.assert_match(yaml.dump(result), "min_balance_state_age_filtered.yml")

    # Aggregation functions and grouping with HAVING eligible to be translated
    # to a WHERE by PG internally for performance reasons
    query = f"""
    SELECT state, gender, avg(age)
    FROM {data_source}.account
    GROUP BY gender, state
    HAVING gender IS NOT NULL AND state <> ALL(ARRAY['MA', 'ME', 'MI', 'MO'])
    ORDER BY gender desc, state desc
    """

    # Ensure query is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {
                "bool": {
                    "must": [
                        {"exists": {"field": "gender"}},
                        {
                            "bool": {
                                "must": [
                                    {"bool": {"must_not": {"term": {"state": "MA"}}}},
                                    {"bool": {"must_not": {"term": {"state": "ME"}}}},
                                    {"bool": {"must_not": {"term": {"state": "MI"}}}},
                                    {"bool": {"must_not": {"term": {"state": "MO"}}}},
                                ]
                            }
                        },
                    ]
                }
            },
            "aggs": {
                "group_buckets": {
                    "composite": {
                        "sources": [
                            {"state": {"terms": {"field": "state"}}},
                            {"gender": {"terms": {"field": "gender"}}},
                        ],
                        "size": 5,
                    },
                    "aggregations": {
                        "avg.age": {"avg": {"field": "age"}},
                    },
                }
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            'SELECT public.account.state, public.account.gender, avg(public.account.age) AS "avg.age" '
            "FROM public.account "
            "WHERE public.account.gender IS NOT NULL AND (public.account.state NOT IN ('MA', 'ME', 'MI', 'MO')) "
            "GROUP BY public.account.state, public.account.gender"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)

    # Assert aggregation result
    snapshot.assert_match(yaml.dump(result), "avg_age_state_gender_filter_by_having.yml")


@pytest.mark.mounting
@pytest.mark.parametrize("data_source", ["es", "pg"])
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_agg_subquery_pushdown(data_source, local_engine_empty):
    """
    Most of the magic in these examples is coming from PG, not our Multicorn code
    (i.e. discarding redundant targets from subqueries).
    Here we just make sure that we don't break that somehow.
    """

    _mount_elasticsearch()

    # DISTINCT on a grouping clause from a subquery
    query = f"""
    SELECT DISTINCT gender FROM(
        SELECT state, gender, min(age), max(balance)
        FROM {data_source}.account GROUP BY state, gender
    ) AS t
    ORDER BY gender
    """

    # Ensure only the relevant part is pushed down (i.e. no aggregations as they are redundant)
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": []}},
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
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.state, public.account.gender "
            "FROM public.account GROUP BY public.account.state, public.account.gender"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 2

    # Assert aggregation result
    assert result == ["F", "M"]

    # DISTINCT on a aggregated column from a subquery
    query = f"""
    SELECT DISTINCT min FROM(
        SELECT state, gender, min(age), max(balance)
        FROM {data_source}.account GROUP BY state, gender
    ) AS t
    ORDER BY min
    """

    # Ensure only the relevant part is pushed down (no redundant aggregations, i.e. only min)
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": []}},
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
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            'SELECT public.account.state, public.account.gender, min(public.account.age) AS "min.age" '
            "FROM public.account GROUP BY public.account.state, public.account.gender"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 8

    # Assert aggregation result
    assert result == [20, 21, 22, 23, 24, 25, 26, 28]

    # Aggregation of the sub-aggregation through a CTE
    query = f"""
    WITH sub_agg AS (
        SELECT state, gender, min(age), max(balance) as max_balance
        FROM {data_source}.account GROUP BY state, gender
    )
    SELECT min(max_balance), gender FROM sub_agg
    GROUP BY gender ORDER BY gender desc
    """

    # Only the subqueries are pushed-down
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == {
            "query": {"bool": {"must": []}},
            "aggs": {
                "group_buckets": {
                    "composite": {
                        "sources": [
                            {"state": {"terms": {"field": "state"}}},
                            {"gender": {"terms": {"field": "gender"}}},
                        ],
                        "size": 5,
                    },
                    "aggregations": {"max.balance": {"max": {"field": "balance"}}},
                }
            },
        }
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            'SELECT public.account.state, public.account.gender, max(public.account.balance) AS "max.balance" '
            "FROM public.account GROUP BY public.account.state, public.account.gender"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert len(result) == 2

    # Assert aggregation result
    assert result == [(37358, "M"), (31968, "F")]


@pytest.mark.mounting
@pytest.mark.parametrize("data_source", ["es", "pg"])
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_aggregations_join_combinations(data_source, snapshot, local_engine_empty):
    # Sub-aggregations in a join are pushed down
    query = f"""
    SELECT t1.*, t2.min FROM (
        SELECT age, max(balance) as max
        FROM {data_source}.account
        GROUP BY age
    ) AS t1
    JOIN (
        SELECT age, min(balance) as min
        FROM {data_source}.account
        GROUP BY age
    ) AS t2
    ON t1.age = t2.age
    ORDER BY t1.age
    """

    # Only the subquery is pushed-down, with no redundant aggregations
    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        queries = _extract_es_queries_from_explain(result)
        assert queries[0] == {
            "query": {"bool": {"must": []}},
            "aggs": {
                "group_buckets": {
                    "composite": {
                        "sources": [{"age": {"terms": {"field": "age"}}}],
                        "size": 5,
                    },
                    "aggregations": {"max.balance": {"max": {"field": "balance"}}},
                }
            },
        }
        assert queries[1] == {
            "query": {"bool": {"must": []}},
            "aggs": {
                "group_buckets": {
                    "composite": {
                        "sources": [{"age": {"terms": {"field": "age"}}}],
                        "size": 5,
                    },
                    "aggregations": {"min.balance": {"min": {"field": "balance"}}},
                }
            },
        }
    elif data_source == "pg":
        queries = _extract_pg_queries_from_explain(result)
        assert queries[0] == (
            'SELECT public.account.age, max(public.account.balance) AS "max.balance" '
            "FROM public.account GROUP BY public.account.age"
        )
        assert queries[1] == (
            'SELECT public.account.age, min(public.account.balance) AS "min.balance" '
            "FROM public.account GROUP BY public.account.age"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert len(result) == 21

    # Assert aggregation result
    snapshot.assert_match(yaml.dump(result), "account_join_sub_aggs.yml")

    # However, aggregation of a joined table are not pushed down
    query = f"""
        EXPLAIN SELECT t.state, AVG(t.balance) FROM (
            SELECT l.state AS state, l.balance + r.balance AS balance
            FROM {data_source}.account l
            JOIN {data_source}.account r USING(state)
        ) t GROUP BY state
    """

    result = local_engine_empty.run_sql(query)

    if data_source == "es":
        queries = _extract_es_queries_from_explain(result)
        assert queries[0] == _bare_es_sequential_scan
        assert queries[1] == _bare_es_sequential_scan
    elif data_source == "pg":
        queries = _extract_pg_queries_from_explain(result)
        assert queries[0] == (
            "SELECT public.account.state, public.account.balance "
            "FROM public.account ORDER BY public.account.state"
        )
        assert queries[1] == (
            "SELECT public.account.balance, public.account.state "
            "FROM public.account ORDER BY public.account.state"
        )


@pytest.mark.mounting
@pytest.mark.parametrize("data_source", ["es", "pg"])
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_various_not_pushed_down(data_source, local_engine_empty):
    _mount_elasticsearch()

    # COUNT(1) not going to be pushed down, as 1 is treated like an expression (single T_Const node)
    query = f"SELECT COUNT(1) FROM {data_source}.account"

    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == _bare_es_sequential_scan
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.account_number FROM public.account"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.ONE_ONE)
    assert result == 1000

    # COUNT DISTINCT queries are not going to be pushed down
    query = f"SELECT COUNT(DISTINCT state) FROM {data_source}.account"

    result = local_engine_empty.run_sql("EXPLAIN " + query)

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == _bare_es_sequential_scan
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.state FROM public.account"
        )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.ONE_ONE)
    assert result == 51

    # SUM DISTINCT queries are not going to be pushed down
    result = local_engine_empty.run_sql(
        f"EXPLAIN SELECT SUM(DISTINCT age) FROM {data_source}.account"
    )

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == _bare_es_sequential_scan
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.age FROM public.account"
        )

    # AVG DISTINCT queries are not going to be pushed down
    result = local_engine_empty.run_sql(
        f"EXPLAIN SELECT AVG(DISTINCT balance) FROM {data_source}.account"
    )

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == _bare_es_sequential_scan
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.balance FROM public.account"
        )

    # Queries with proper HAVING are not goint to be pushed down
    result = local_engine_empty.run_sql(
        f"EXPLAIN SELECT max(balance) FROM {data_source}.account HAVING max(balance) > 30"
    )

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == _bare_es_sequential_scan
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.balance FROM public.account "
            "WHERE public.account.balance IS NOT NULL ORDER BY public.account.balance DESC"
        )

    # Aggregation with a nested expression won't be pushed down
    result = local_engine_empty.run_sql(
        f"EXPLAIN SELECT avg(age * balance) FROM {data_source}.account GROUP BY state"
    )

    if data_source == "es":
        assert _extract_es_queries_from_explain(result)[0] == _bare_es_sequential_scan
    elif data_source == "pg":
        assert _extract_pg_queries_from_explain(result)[0] == (
            "SELECT public.account.age, public.account.balance, public.account.state "
            "FROM public.account ORDER BY public.account.state"
        )
