import pytest

from splitgraph.commands import commit, diff, checkout
from splitgraph.meta_handler import get_snap_parent, get_current_head, get_table, get_table_with_format
from splitgraph.pg_utils import _get_full_table_schema, _get_primary_keys
from test.splitgraph.conftest import PG_MNT

TEST_CASES = [
    ("ALTER TABLE test_pg_mount.fruits DROP COLUMN name",
     [(1, 'fruit_id', 'integer', False)]),
    ("ALTER TABLE test_pg_mount.fruits ADD COLUMN test varchar",
     [(1, 'fruit_id', 'integer', False), (2, 'name', 'character varying', False),
      (3, 'test', 'character varying', False)]),
    ("ALTER TABLE test_pg_mount.fruits ADD PRIMARY KEY (fruit_id)",
     [(1, 'fruit_id', 'integer', True), (2, 'name', 'character varying', False)]),

    ("""ALTER TABLE test_pg_mount.fruits ADD COLUMN test_1 varchar, ADD COLUMN test_2 integer,
                                         DROP COLUMN name;
        ALTER TABLE test_pg_mount.fruits ADD PRIMARY KEY (fruit_id)""",
     [(1, 'fruit_id', 'integer', True), (3, 'test_1', 'character varying', False),
      (4, 'test_2', 'integer', False)]),
]

OLD_SCHEMA = [(1, 'fruit_id', 'integer', False), (2, 'name', 'character varying', False)]


def _reassign_ordinals(schema):
    # When a table is created anew, its ordinals are made consecutive again.
    return [(i+1, col[1], col[2], col[3]) for i, col in enumerate(schema)]


@pytest.mark.parametrize("test_case", TEST_CASES)
def test_schema_changes(sg_pg_conn, test_case):
    action, expected_new_schema = test_case

    assert _get_full_table_schema(sg_pg_conn, PG_MNT, 'fruits') == OLD_SCHEMA
    with sg_pg_conn.cursor() as cur:
        cur.execute(action)
    sg_pg_conn.commit()
    assert _get_full_table_schema(sg_pg_conn, PG_MNT, 'fruits') == expected_new_schema

    head = get_current_head(sg_pg_conn, PG_MNT)
    new_head = commit(sg_pg_conn, PG_MNT)

    # Test that the new image only has a snap and the object storing the snap has the expected new schema
    assert get_table_with_format(sg_pg_conn, PG_MNT, 'fruits', new_head, 'DIFF') is None
    new_snap = get_table_with_format(sg_pg_conn, PG_MNT, 'fruits', new_head, 'SNAP')
    assert new_snap is not None
    assert _get_full_table_schema(sg_pg_conn, PG_MNT, new_snap) == _reassign_ordinals(expected_new_schema)

    checkout(sg_pg_conn, PG_MNT, head)
    assert _get_full_table_schema(sg_pg_conn, PG_MNT, 'fruits') == OLD_SCHEMA
    checkout(sg_pg_conn, PG_MNT, new_head)

    assert _get_full_table_schema(sg_pg_conn, PG_MNT, 'fruits') == _reassign_ordinals(expected_new_schema)


def test_pk_preserved_on_checkout(sg_pg_conn):
    assert list(_get_primary_keys(sg_pg_conn, PG_MNT, 'fruits')) == []
    with sg_pg_conn.cursor() as cur:
        cur.execute("""ALTER TABLE test_pg_mount.fruits ADD PRIMARY KEY (fruit_id)""")
    assert list(_get_primary_keys(sg_pg_conn, PG_MNT, 'fruits')) == [('fruit_id', 'integer')]
    head = get_current_head(sg_pg_conn, PG_MNT)
    new_head = commit(sg_pg_conn, PG_MNT)
    assert list(_get_primary_keys(sg_pg_conn, PG_MNT, 'fruits')) == [('fruit_id', 'integer')]

    checkout(sg_pg_conn, PG_MNT, head)
    assert list(_get_primary_keys(sg_pg_conn, PG_MNT, 'fruits')) == []
    checkout(sg_pg_conn, PG_MNT, new_head)
    assert list(_get_primary_keys(sg_pg_conn, PG_MNT, 'fruits')) == [('fruit_id', 'integer')]
