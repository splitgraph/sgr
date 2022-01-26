import json
import math
from decimal import Decimal

import pytest
import yaml

from splitgraph.engine import ResultShape


def _extract_queries_from_explain(result):
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


@pytest.mark.mounting
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_pattern_matching_queries(local_engine_empty):
    # Test meaningful pattern match query returns correct result
    query = "SELECT firstname FROM pg.account WHERE firstname ~~ 'Su_an%'"

    # Ensure proper query translation
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.firstname FROM public.account "
        "WHERE public.account.firstname LIKE 'Su_an%%'"
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 4
    assert set(result) == {"Susan", "Susana", "Susanne", "Suzanne"}

    # Test meaningful pattern match query returns correct result
    query = "SELECT firstname FROM pg.account WHERE firstname !~~ 'Su_an%'"

    # Ensure proper query translation
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.firstname FROM public.account "
        "WHERE public.account.firstname NOT LIKE 'Su_an%%'"
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 996
    assert not {"Susan", "Susana", "Susanne", "Suzanne"}.issubset(set(result))


@pytest.mark.mounting
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_simple_aggregation_functions(local_engine_empty):
    query = """
    SELECT max(account_number), avg(balance), max(balance),
        sum(balance), min(age), avg(age)
    FROM pg.account
    """

    # Ensure query is going to be aggregated on the foreign server
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
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
    query = "SELECT COUNT(*) FROM pg.account"

    # Ensure query is going to be aggregated on the foreign server
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert (
        _extract_queries_from_explain(result)[0]
        == 'SELECT count(*) AS "count.*" FROM public.account'
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.ONE_ONE)
    assert result == 1000

    # Not like qual operator
    query = "SELECT COUNT(*) FROM pg.account WHERE firstname !~~ 'Su_an%'"

    # Ensure query is going to be aggregated on the foreign server
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
        'SELECT count(*) AS "count.*" '
        "FROM public.account "
        "WHERE public.account.firstname NOT LIKE 'Su_an%%'"
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.ONE_ONE)
    assert result == 996


@pytest.mark.mounting
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_simple_aggregation_functions_filtering(local_engine_empty):
    query = """
    SELECT avg(age), max(balance)
    FROM pg.account
    WHERE balance > 20000 AND age < 30
    """

    # Ensure query is going to be aggregated on the foreign server
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
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
    query = "SELECT avg(balance), COUNT(*) FROM pg.account WHERE firstname ~~ 'Su_an%'"

    # Ensure query is going to be aggregated on the foreign server
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
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
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_simple_grouping_clauses(snapshot, local_engine_empty):
    # Single column grouping
    query = "SELECT state FROM pg.account GROUP BY state"

    # Ensure grouping is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.state " "FROM public.account GROUP BY public.account.state"
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 51

    # Assert aggregation result
    snapshot.assert_match(yaml.dump(result), "account_states.yml")

    # Multi-column grouping
    query = "SELECT gender, age FROM pg.account GROUP BY age, gender"

    # Ensure grouping is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.gender, public.account.age "
        "FROM public.account GROUP BY public.account.gender, public.account.age"
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert len(result) == 42

    # Assert aggregation result
    snapshot.assert_match(yaml.dump(result), "account_genders_and_ages.yml")


@pytest.mark.mounting
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_simple_grouping_clauses_filtering(snapshot, local_engine_empty):
    # Single column grouping
    query = "SELECT state, gender FROM pg.account WHERE state IN ('TX', 'WA', 'CO') GROUP BY state, gender"

    # Ensure grouping is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.state, public.account.gender "
        "FROM public.account "
        "WHERE public.account.state IN ('TX', 'WA', 'CO') GROUP BY public.account.state, public.account.gender"
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert set(result) == {
        ("CO", "F"),
        ("CO", "M"),
        ("TX", "F"),
        ("TX", "M"),
        ("WA", "F"),
        ("WA", "M"),
    }


@pytest.mark.mounting
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_grouping_and_aggregations_bare(snapshot, local_engine_empty):
    # Aggregations functions and grouping bare combination
    query = "SELECT gender, avg(balance), avg(age) FROM pg.account GROUP BY gender"

    # Ensure query is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.gender, "
        'avg(public.account.balance) AS "avg.balance", '
        'avg(public.account.age) AS "avg.age" '
        "FROM public.account GROUP BY public.account.gender"
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert len(result) == 2
    assert result[0][0] == "M"
    assert math.isclose(result[0][1], 25803.8007, rel_tol=1e-05)
    assert math.isclose(result[0][2], 30.0276, rel_tol=1e-05)
    assert result[1][0] == "F"
    assert math.isclose(result[1][1], 25623.3468, rel_tol=1e-05)
    assert math.isclose(result[1][2], 30.3184, rel_tol=1e-05)

    # We support pushing down aggregation queries with sorting, with the caveat
    # that the sorting operation is performed on the PG side for now
    query = "SELECT age, COUNT(account_number), min(balance) FROM pg.account GROUP BY age ORDER BY age DESC"

    # Ensure query is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
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
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_grouping_and_aggregations_filtering(snapshot, local_engine_empty):
    # Aggregation functions and grouping with filtering
    query = """
    SELECT state, age, min(balance), COUNT(*)
    FROM pg.account
    WHERE gender = 'M' AND age = ANY(ARRAY[25, 35])
    GROUP BY state, age
    """

    # Ensure query is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
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
    query = """
    SELECT state, gender, avg(age)
    FROM pg.account
    GROUP BY gender, state
    HAVING gender IS NOT NULL AND state <> ALL(ARRAY['MA', 'ME', 'MI', 'MO'])
    """

    # Ensure query is going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
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
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_agg_subquery_pushdown(local_engine_empty):
    """
    Most of the magic in these examples is coming from PG, not our Multicorn code
    (i.e. discarding redundant targets from subqueries).
    Here we just make sure that we don't break that somehow.
    """

    # DISTINCT on a grouping clause from a subquery
    query = """
    SELECT DISTINCT gender FROM(
        SELECT state, gender, min(age), max(balance)
        FROM pg.account GROUP BY state, gender
    ) AS t
    """

    # Ensure only the relevant part is pushed down (i.e. no aggregations as they are redundant)
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.state, public.account.gender "
        "FROM public.account GROUP BY public.account.state, public.account.gender"
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 2

    # Assert aggregation result
    assert result == ["F", "M"]

    # DISTINCT on a aggregated column from a subquery
    query = """
    SELECT DISTINCT min FROM(
        SELECT state, gender, min(age), max(balance)
        FROM pg.account GROUP BY state, gender
    ) AS t
    """

    # Ensure only the relevant part is pushed down (no redundant aggregations, i.e. only min)
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
        'SELECT public.account.state, public.account.gender, min(public.account.age) AS "min.age" '
        "FROM public.account GROUP BY public.account.state, public.account.gender"
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.MANY_ONE)
    assert len(result) == 8

    # Assert aggregation result
    assert result == [20, 21, 22, 23, 24, 25, 26, 28]

    # Aggregation of the sub-aggregation through a CTE
    query = """
    WITH sub_agg AS (
        SELECT state, gender, min(age), max(balance) as max_balance
        FROM pg.account GROUP BY state, gender
    )
    SELECT min(max_balance), gender FROM sub_agg GROUP BY gender
    """

    # Only the subquery is pushed-down, with no redundant aggregations
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
        'SELECT public.account.state, public.account.gender, max(public.account.balance) AS "max.balance" '
        "FROM public.account GROUP BY public.account.state, public.account.gender"
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query)
    assert len(result) == 2

    # Assert aggregation result
    assert result == [(37358, "M"), (31968, "F")]


@pytest.mark.mounting
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_aggregations_join_combinations(snapshot, local_engine_empty):
    # Sub-aggregations in a join are pushed down
    query = """
    SELECT t1.*, t2.min FROM (
        SELECT age, max(balance) as max
        FROM pg.account
        GROUP BY age
    ) AS t1
    JOIN (
        SELECT age, min(balance) as min
        FROM pg.account
        GROUP BY age
    ) AS t2
    ON t1.age = t2.age
    """

    # Only the subqueries are pushed-down
    result = local_engine_empty.run_sql("EXPLAIN " + query)
    queries = _extract_queries_from_explain(result)

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
    result = local_engine_empty.run_sql(
        """
        EXPLAIN SELECT t.state, AVG(t.balance) FROM (
            SELECT l.state AS state, l.balance + r.balance AS balance
            FROM pg.account l
            JOIN pg.account r USING(state)
        ) t GROUP BY state
        """
    )
    queries = _extract_queries_from_explain(result)
    assert queries[0] == (
        "SELECT public.account.state, public.account.balance "
        "FROM public.account ORDER BY public.account.state"
    )
    assert queries[1] == (
        "SELECT public.account.balance, public.account.state "
        "FROM public.account ORDER BY public.account.state"
    )


@pytest.mark.mounting
@pytest.mark.usefixtures("pgorigin_sqlalchemy_fdw")
def test_not_pushed_down(local_engine_empty):
    # COUNT(1) not going to be pushed down, as 1 is treated like an expression (single T_Const node)
    query = "SELECT COUNT(1) FROM pg.account"

    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.account_number FROM public.account"
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.ONE_ONE)
    assert result == 1000

    # COUNT DISTINCT queries are not going to be pushed down
    query = "SELECT COUNT(DISTINCT state) FROM pg.account"

    result = local_engine_empty.run_sql("EXPLAIN " + query)
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.state FROM public.account"
    )

    # Ensure results are correct
    result = local_engine_empty.run_sql(query, return_shape=ResultShape.ONE_ONE)
    assert result == 51

    # SUM DISTINCT queries are not going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN SELECT SUM(DISTINCT age) FROM pg.account")
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.age FROM public.account"
    )

    # AVG DISTINCT queries are not going to be pushed down
    result = local_engine_empty.run_sql("EXPLAIN SELECT AVG(DISTINCT balance) FROM pg.account")
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.balance FROM public.account"
    )

    # Queries with proper HAVING are not goint to be pushed down
    result = local_engine_empty.run_sql(
        "EXPLAIN SELECT max(balance) FROM pg.account HAVING max(balance) > 30"
    )
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.balance FROM public.account "
        "WHERE public.account.balance IS NOT NULL ORDER BY public.account.balance DESC"
    )

    # Aggregation with a nested expression won't be pushed down
    result = local_engine_empty.run_sql(
        "EXPLAIN SELECT avg(age * balance) FROM pg.account GROUP BY state"
    )
    assert _extract_queries_from_explain(result)[0] == (
        "SELECT public.account.age, public.account.balance, public.account.state "
        "FROM public.account ORDER BY public.account.state"
    )
