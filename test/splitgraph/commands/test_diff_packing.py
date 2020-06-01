import pytest

# Test cases: ops are a list of operations (with commit after each set);
#             diffs are expected diffs produced by each operation.
from splitgraph.core.fragment_manager import _conflate_changes

CASES = [
    [  # Insert + update changed into a single insert
        (
            """INSERT INTO fruits VALUES (3, 'mayonnaise');
        UPDATE fruits SET name = 'mustard' WHERE fruit_id = 3""",
            [(True, (3, "mustard"))],
        ),
        # Insert + update + delete did nothing (sequences not supported)
        (
            """INSERT INTO fruits VALUES (4, 'kumquat');
        UPDATE fruits SET name = 'mustard' WHERE fruit_id = 4;
        DELETE FROM fruits WHERE fruit_id = 4""",
            [],
        ),
        # delete + reinsert same results in nothing
        (
            """DELETE FROM fruits WHERE fruit_id = 1;
        INSERT INTO fruits VALUES (1, 'apple')""",
            [],
        ),
        # Two updates, but the PK changed back to the original one -- no diff.
        (
            """UPDATE fruits SET name = 'pineapple' WHERE fruit_id = 1;
        UPDATE fruits SET name = 'apple' WHERE fruit_id = 1""",
            [],
        ),
    ],
    [  # Now test this whole thing works with primary keys
        ("""ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)""", []),
        # Insert + update changed into a single insert (same pk, different value)
        (
            """INSERT INTO fruits VALUES (3, 'mayonnaise');
            UPDATE fruits SET name = 'mustard' WHERE fruit_id = 3""",
            [(True, (3, "mustard"))],
        ),
        # Insert + update + delete did nothing
        (
            """INSERT INTO fruits VALUES (4, 'kumquat');
            UPDATE fruits SET name = 'mustard' WHERE fruit_id = 4;
            DELETE FROM fruits WHERE fruit_id = 4""",
            [],
        ),
        # delete + reinsert same
        (
            """DELETE FROM fruits WHERE fruit_id = 1;
            INSERT INTO fruits VALUES (1, 'apple')""",
            [],
        ),
        # Two updates
        (
            """UPDATE fruits SET name = 'pineapple' WHERE fruit_id = 1;
            UPDATE fruits SET name = 'apple' WHERE fruit_id = 1""",
            # Same here
            [],
        ),
        # Update + delete gets turned into a delete
        (
            """UPDATE fruits SET name = 'pineapple' WHERE fruit_id = 1;
            DELETE FROM fruits WHERE fruit_id = 1""",
            [(False, (1, "apple"))],
        ),
    ],
    [
        # Test a table with 2 PKs and 2 non-PK columns
        (
            """DROP TABLE fruits; CREATE TABLE fruits (
            pk1 INTEGER,
            pk2 INTEGER,
            col1 VARCHAR,
            col2 VARCHAR,        
            PRIMARY KEY (pk1, pk2));
            INSERT INTO fruits VALUES (1, 1, 'val1', 'val2')""",
            [(False, (1, "apple")), (False, (2, "orange")), (True, (1, 1, "val1", "val2"))],
        ),
        # Test an update touching part of the PK and part of the contents
        (
            """UPDATE fruits SET pk2 = 2, col1 = 'val3' WHERE pk1 = 1""",
            [(False, (1, 1, "val1", "val2")), (True, (1, 2, "val3", "val2"))],
        ),
    ],
    [
        # Same as previous, but without PKs
        (
            """DROP TABLE fruits; CREATE TABLE fruits (
        pk1 INTEGER,
        pk2 INTEGER,
        col1 VARCHAR,
        col2 VARCHAR);
        INSERT INTO fruits VALUES (1, 1, 'val1', 'val2')""",
            # Same here for the diff produced by this DDL
            [(False, (1, "apple")), (False, (2, "orange")), (True, (1, 1, "val1", "val2"))],
        ),
        (
            """UPDATE fruits SET pk2 = 2, col1 = 'val3' WHERE pk1 = 1""",
            [(False, (1, 1, "val1", "val2")), (True, (1, 2, "val3", "val2"))],
        ),
    ],
]


# maybe move most things in test_commit_diff to here instead, since they basically seem to follow a similar
# flow (apply a change -- check the diff -- commit -- check the diff again)


@pytest.mark.parametrize("test_case", CASES)
def test_diff_conflation_on_commit(pg_repo_local, test_case):
    for operation, expected_diff in test_case:
        # Dump the operation we're running to stdout for easier debugging
        print("%r -> %r" % (operation, expected_diff))
        pg_repo_local.run_sql(operation)
        pg_repo_local.commit_engines()
        assert pg_repo_local.diff("fruits", pg_repo_local.head, None) == expected_diff
        head = pg_repo_local.commit()
        assert pg_repo_local.diff("fruits", pg_repo_local.head.parent_id, head) == expected_diff


def test_diff_conflation_insert_same(pg_repo_local):
    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    pg_repo_local.commit()

    pg_repo_local.run_sql("DELETE FROM fruits WHERE fruit_id = 1")
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (1, 'apple')")
    pg_repo_local.commit_engines()

    # check get_pending_changes returns old and new row values
    expected_changes = [
        ((1,), False, {"name": "apple", "fruit_id": 1}, {}),
        ((1,), True, {}, {"name": "apple", "fruit_id": 1}),
    ]
    assert pg_repo_local.engine.get_pending_changes("test/pg_mount", "fruits") == expected_changes

    # Check delete + insert same cancel each other out.
    assert _conflate_changes({}, expected_changes) == {}
    assert pg_repo_local.diff("fruits", pg_repo_local.head, None) == []
