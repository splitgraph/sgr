import pytest

from splitgraph._data.objects import get_object_for_table
from splitgraph.commands import commit, checkout
from splitgraph.commands.tagging import get_current_head
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.pg_utils import get_full_table_schema, get_primary_keys
from test.splitgraph.conftest import PG_MNT

TEST_CASES = [
    ("ALTER TABLE \"test/pg_mount\".fruits DROP COLUMN name",
     [(1, 'fruit_id', 'integer', False)]),
    ("ALTER TABLE \"test/pg_mount\".fruits ADD COLUMN test varchar",
     [(1, 'fruit_id', 'integer', False), (2, 'name', 'character varying', False),
      (3, 'test', 'character varying', False)]),
    ("ALTER TABLE \"test/pg_mount\".fruits ADD PRIMARY KEY (fruit_id)",
     [(1, 'fruit_id', 'integer', True), (2, 'name', 'character varying', False)]),

    ("""ALTER TABLE "test/pg_mount".fruits ADD COLUMN test_1 varchar, ADD COLUMN test_2 integer,
                                         DROP COLUMN name;
        ALTER TABLE "test/pg_mount".fruits ADD PRIMARY KEY (fruit_id)""",
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

    assert get_full_table_schema(sg_pg_conn, PG_MNT.to_schema(), 'fruits') == OLD_SCHEMA
    with sg_pg_conn.cursor() as cur:
        cur.execute(action)
    sg_pg_conn.commit()
    assert get_full_table_schema(sg_pg_conn, PG_MNT.to_schema(), 'fruits') == expected_new_schema

    head = get_current_head(PG_MNT)
    new_head = commit(PG_MNT)

    # Test that the new image only has a snap and the object storing the snap has the expected new schema
    assert get_object_for_table(PG_MNT, 'fruits', new_head, 'DIFF') is None
    new_snap = get_object_for_table(PG_MNT, 'fruits', new_head, 'SNAP')
    assert new_snap is not None
    assert get_full_table_schema(sg_pg_conn, SPLITGRAPH_META_SCHEMA, new_snap) == _reassign_ordinals(expected_new_schema)

    checkout(PG_MNT, head)
    assert get_full_table_schema(sg_pg_conn, PG_MNT.to_schema(), 'fruits') == OLD_SCHEMA
    checkout(PG_MNT, new_head)

    assert get_full_table_schema(sg_pg_conn, PG_MNT.to_schema(), 'fruits') == _reassign_ordinals(expected_new_schema)


def test_pk_preserved_on_checkout(sg_pg_conn):
    assert list(get_primary_keys(sg_pg_conn, PG_MNT.to_schema(), 'fruits')) == []
    with sg_pg_conn.cursor() as cur:
        cur.execute("""ALTER TABLE "test/pg_mount".fruits ADD PRIMARY KEY (fruit_id)""")
    assert list(get_primary_keys(sg_pg_conn, PG_MNT.to_schema(), 'fruits')) == [('fruit_id', 'integer')]
    head = get_current_head(PG_MNT)
    new_head = commit(PG_MNT)
    assert list(get_primary_keys(sg_pg_conn, PG_MNT.to_schema(), 'fruits')) == [('fruit_id', 'integer')]

    checkout(PG_MNT, head)
    assert list(get_primary_keys(sg_pg_conn, PG_MNT.to_schema(), 'fruits')) == []
    checkout(PG_MNT, new_head)
    assert list(get_primary_keys(sg_pg_conn, PG_MNT.to_schema(), 'fruits')) == [('fruit_id', 'integer')]
