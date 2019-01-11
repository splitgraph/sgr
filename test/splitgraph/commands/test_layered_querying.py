import pytest
from psycopg2 import ProgrammingError


@pytest.mark.xfail(raises=ProgrammingError, reason="Multicorn not installed on the driver")
@pytest.mark.parametrize("include_snap", [True, False])
@pytest.mark.parametrize("commit_after_every", [True, False])
@pytest.mark.parametrize("include_pk", [True, False])
def test_layered_querying(pg_repo_local, include_snap, commit_after_every, include_pk):
    OPS = ["INSERT INTO fruits VALUES (3, 'mayonnaise')",
           "DELETE FROM fruits WHERE name = 'apple'",
           "DELETE FROM vegetables WHERE vegetable_id = 1",
           "UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2"]

    pg_repo_local.run_sql("ALTER TABLE fruits ADD COLUMN number NUMERIC DEFAULT 1")
    if include_pk:
        pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
        pg_repo_local.run_sql("ALTER TABLE vegetables ADD PRIMARY KEY (vegetable_id)")

    for o in OPS:
        pg_repo_local.run_sql(o)
        print(o)

        if commit_after_every:
            pg_repo_local.commit(include_snap=include_snap)
    if not commit_after_every:
        pg_repo_local.commit(include_snap=include_snap)

    new_head = pg_repo_local.head
    # Discard the actual materialized table and query everything via FDW
    new_head.checkout(layered=True)

    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id = 3") == [(3, 'mayonnaise', 1)]
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id = 2") == [(2, 'guitar', 1)]
    assert pg_repo_local.run_sql("SELECT * FROM vegetables WHERE vegetable_id = 1") == []
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id = 1") == []

    # Test some more esoteric qualifiers
    # EQ on string
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE name = 'guitar'") == [(2, 'guitar', 1)]

    # IN ( converted to =ANY(array([...]))
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE name IN ('guitar', 'mayonnaise')") ==\
           [(2, 'guitar', 1), (3, 'mayonnaise', 1)]

    # LIKE (operator ~~)
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE name LIKE '%uitar'") == [(2, 'guitar', 1)]

    # Join between two FDWs
    assert pg_repo_local.run_sql("SELECT * FROM fruits JOIN vegetables ON fruits.fruit_id = vegetables.vegetable_id")\
           == [(2, 'guitar', 1, 2, 'carrot')]

    # Expression in terms of another column
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id = number") == []
    assert pg_repo_local.run_sql("SELECT * FROM fruits WHERE fruit_id = number + 1 ") == [(2, 'guitar', 1)]
