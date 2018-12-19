import pytest

from splitgraph.commands import commit, diff
from splitgraph.commands.info import get_image
from test.splitgraph.conftest import PG_MNT

# Test cases: ops are a list of operations (with commit after each set);
#             diffs are expected diffs produced by each operation.

CASES = [
    [  # Insert + update changed into a single insert
        ("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise');
        UPDATE "test/pg_mount".fruits SET name = 'mustard' WHERE fruit_id = 3""",
         [((3, 'mustard'), 0, {'c': [], 'v': []})]),
        # Insert + update + delete did nothing (sequences not supported)
        ("""INSERT INTO "test/pg_mount".fruits VALUES (4, 'kumquat');
        UPDATE "test/pg_mount".fruits SET name = 'mustard' WHERE fruit_id = 4;
        DELETE FROM "test/pg_mount".fruits WHERE fruit_id = 4""",
         []),
        # delete + reinsert same results in nothing
        ("""DELETE FROM "test/pg_mount".fruits WHERE fruit_id = 1;
        INSERT INTO "test/pg_mount".fruits VALUES (1, 'apple')""",
         []),
        # Two updates, but the PK changed back to the original one -- no diff.
        ("""UPDATE "test/pg_mount".fruits SET name = 'pineapple' WHERE fruit_id = 1;
        UPDATE "test/pg_mount".fruits SET name = 'apple' WHERE fruit_id = 1""",
         [])
    ],
    [  # Now test this whole thing works with primary keys
        ("""ALTER TABLE "test/pg_mount".fruits ADD PRIMARY KEY (fruit_id)""",
         []),
        # Insert + update changed into a single insert (same pk, different value)
        ("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise');
            UPDATE "test/pg_mount".fruits SET name = 'mustard' WHERE fruit_id = 3""",
         [((3,), 0, {'c': ['name'], 'v': ['mustard']})]),
        # Insert + update + delete did nothing
        ("""INSERT INTO "test/pg_mount".fruits VALUES (4, 'kumquat');
            UPDATE "test/pg_mount".fruits SET name = 'mustard' WHERE fruit_id = 4;
            DELETE FROM "test/pg_mount".fruits WHERE fruit_id = 4""",
         []),
        # delete + reinsert same
        ("""DELETE FROM "test/pg_mount".fruits WHERE fruit_id = 1;
            INSERT INTO "test/pg_mount".fruits VALUES (1, 'apple')""",
         # Currently the packer isn't aware that we rewrote the same value
         [((1,), 2, {'c': ['name'], 'v': ['apple']})]),
        # Two updates
        ("""UPDATE "test/pg_mount".fruits SET name = 'pineapple' WHERE fruit_id = 1;
            UPDATE "test/pg_mount".fruits SET name = 'apple' WHERE fruit_id = 1""",
         # Same here
         [((1,), 2, {'c': ['name'], 'v': ['apple']})]),
    ],
    [
        # Test a table with 2 PKs and 2 non-PK columns
        ("""DROP TABLE "test/pg_mount".fruits; CREATE TABLE "test/pg_mount".fruits (
            pk1 INTEGER,
            pk2 INTEGER,
            col1 VARCHAR,
            col2 VARCHAR,        
            PRIMARY KEY (pk1, pk2));
            INSERT INTO "test/pg_mount".fruits VALUES (1, 1, 'val1', 'val2')""",
         # We don't really care about this diff but worth noting it's produced by comparing two tables
         # side-by-side since there's been a schema change. Hence it isn't aware that (1,1) is actually
         # the PK for the new table and instead just dumps the whole row as a diff.
         [((1, 'apple'), 1, None), ((2, 'orange'), 1, None), ((1, 1, 'val1', 'val2'), 0, {'c': [], 'v': []})]),
        # Test an update touching part of the PK and part of the contents
        ("""UPDATE "test/pg_mount".fruits SET pk2 = 2, col1 = 'val3' WHERE pk1 = 1""",
         # Since we delete one PK and insert another one, we need to reinsert the
         # full row (as opposed to just the values that were updated).
         [((1, 1), 1, None),
          ((1, 2), 0, {'c': ['col1', 'col2'], 'v': ['val3', 'val2']})]),
    ],
    [
        # Same as previous, but without PKs
        ("""DROP TABLE "test/pg_mount".fruits; CREATE TABLE "test/pg_mount".fruits (
        pk1 INTEGER,
        pk2 INTEGER,
        col1 VARCHAR,
        col2 VARCHAR);
        INSERT INTO "test/pg_mount".fruits VALUES (1, 1, 'val1', 'val2')""",
         # Same here for the diff produced by this DDL
         [((1, 'apple'), 1, None), ((2, 'orange'), 1, None), ((1, 1, 'val1', 'val2'), 0, {'c': [], 'v': []})]),
        ("""UPDATE "test/pg_mount".fruits SET pk2 = 2, col1 = 'val3' WHERE pk1 = 1""",
         # Full row is the PK, so we just enumerate the whole row for deletion/insertion.
         [((1, 1, 'val1', 'val2'), 1, None),
          ((1, 2, 'val3', 'val2'), 0, {'c': [], 'v': []})]),
    ]
]


@pytest.mark.parametrize("test_case", CASES)
def test_diff_conflation_on_commit(local_engine_with_pg, test_case):
    for operation, expected_diff in test_case:
        # Dump the operation we're running to stdout for easier debugging
        print("%r -> %r" % (operation, expected_diff))
        local_engine_with_pg.run_sql(operation, return_shape=None)
        local_engine_with_pg.commit()
        head = commit(PG_MNT)
        assert diff(PG_MNT, 'fruits', get_image(PG_MNT, head).parent_id, head) == expected_diff
