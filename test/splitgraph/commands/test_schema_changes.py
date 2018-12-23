import pytest

from splitgraph._data.objects import get_object_for_table
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import get_engine

TEST_CASES = [
    ("ALTER TABLE fruits DROP COLUMN name",
     [(1, 'fruit_id', 'integer', False)]),
    ("ALTER TABLE fruits ADD COLUMN test varchar",
     [(1, 'fruit_id', 'integer', False), (2, 'name', 'character varying', False),
      (3, 'test', 'character varying', False)]),
    ("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)",
     [(1, 'fruit_id', 'integer', True), (2, 'name', 'character varying', False)]),

    ("""ALTER TABLE fruits ADD COLUMN test_1 varchar, ADD COLUMN test_2 integer,
                                         DROP COLUMN name;
        ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)""",
     [(1, 'fruit_id', 'integer', True), (3, 'test_1', 'character varying', False),
      (4, 'test_2', 'integer', False)]),
]

OLD_SCHEMA = [(1, 'fruit_id', 'integer', False), (2, 'name', 'character varying', False)]


def _reassign_ordinals(schema):
    # When a table is created anew, its ordinals are made consecutive again.
    return [(i + 1, col[1], col[2], col[3]) for i, col in enumerate(schema)]


@pytest.mark.parametrize("test_case", TEST_CASES)
def test_schema_changes(pg_repo_local, test_case):
    action, expected_new_schema = test_case
    engine = get_engine()

    assert pg_repo_local.engine.get_full_table_schema(pg_repo_local.to_schema(), 'fruits') == OLD_SCHEMA
    pg_repo_local.run_sql(action)
    pg_repo_local.engine.commit()
    assert pg_repo_local.engine.get_full_table_schema(pg_repo_local.to_schema(), 'fruits') == expected_new_schema

    head = pg_repo_local.get_head()
    new_head = pg_repo_local.commit()

    # Test that the new image only has a snap and the object storing the snap has the expected new schema
    assert get_object_for_table(pg_repo_local, 'fruits', new_head, 'DIFF') is None
    new_snap = get_object_for_table(pg_repo_local, 'fruits', new_head, 'SNAP')
    assert new_snap is not None
    assert pg_repo_local.engine.get_full_table_schema(SPLITGRAPH_META_SCHEMA, new_snap) == _reassign_ordinals(
        expected_new_schema)

    pg_repo_local.checkout(head)
    assert pg_repo_local.engine.get_full_table_schema(pg_repo_local.to_schema(), 'fruits') == OLD_SCHEMA

    pg_repo_local.checkout(new_head)
    assert pg_repo_local.engine.get_full_table_schema(pg_repo_local.to_schema(), 'fruits') == \
           _reassign_ordinals(expected_new_schema)


def test_pk_preserved_on_checkout(pg_repo_local):
    assert list(pg_repo_local.engine.get_primary_keys(pg_repo_local.to_schema(), 'fruits')) == []
    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    assert list(pg_repo_local.engine.get_primary_keys(pg_repo_local.to_schema(), 'fruits')) == [('fruit_id', 'integer')]
    head = pg_repo_local.get_head()
    new_head = pg_repo_local.commit()
    assert list(pg_repo_local.engine.get_primary_keys(pg_repo_local.to_schema(), 'fruits')) == [('fruit_id', 'integer')]

    pg_repo_local.checkout(head)
    assert list(pg_repo_local.engine.get_primary_keys(pg_repo_local.to_schema(), 'fruits')) == []
    pg_repo_local.checkout(new_head)
    assert list(pg_repo_local.engine.get_primary_keys(pg_repo_local.to_schema(), 'fruits')) == [('fruit_id', 'integer')]
