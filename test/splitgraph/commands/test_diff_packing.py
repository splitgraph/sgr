import pytest

from splitgraph.commands import commit, diff
from splitgraph.meta_handler import get_snap_parent
from test.splitgraph.conftest import PG_MNT

# Test cases: ops are a list of operations (with commit after each set);
#             diffs are expected diffs produced by each operation.

CASES = [
    [  # Insert + update changed into a single insert
        ("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise');
        UPDATE test_pg_mount.fruits SET name = 'mustard' WHERE fruit_id = 3""",
         [((3, 'mustard'), 0, {'c': [], 'v': []})]),
        # Insert + update + delete did nothing (todo what about sequences)
        ("""INSERT INTO test_pg_mount.fruits VALUES (4, 'kumquat');
        UPDATE test_pg_mount.fruits SET name = 'mustard' WHERE fruit_id = 4;
        DELETE FROM test_pg_mount.fruits WHERE fruit_id = 4""",
         []),
        # delete + reinsert same results in nothing
        ("""DELETE FROM test_pg_mount.fruits WHERE fruit_id = 1;
        INSERT INTO test_pg_mount.fruits VALUES (1, 'apple')""",
         []),
        # Two updates, but the PK changed back to the original one -- no diff.
        ("""UPDATE test_pg_mount.fruits SET name = 'pineapple' WHERE fruit_id = 1;
        UPDATE test_pg_mount.fruits SET name = 'apple' WHERE fruit_id = 1""",
         [])
    ]]

@pytest.mark.parametrize("test_case", CASES)
def test_diff_conflation_on_commit(sg_pg_conn, test_case):
    for operation, expected_diff in test_case:
        with sg_pg_conn.cursor() as cur:
            cur.execute(operation)
        sg_pg_conn.commit()
        head = commit(sg_pg_conn, PG_MNT)
        assert diff(sg_pg_conn, PG_MNT, 'fruits', get_snap_parent(sg_pg_conn, PG_MNT, head), head) == expected_diff
