import pytest

from splitgraph.engine.postgres.engine import SG_UD_FLAG

TEST_CASES = [
    ("ALTER TABLE fruits DROP COLUMN name", [(1, "fruit_id", "integer", False)]),
    (
        "ALTER TABLE fruits ADD COLUMN test varchar",
        [
            (1, "fruit_id", "integer", False),
            (2, "name", "character varying", False),
            (3, "test", "character varying", False),
        ],
    ),
    (
        "ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)",
        [(1, "fruit_id", "integer", True), (2, "name", "character varying", False)],
    ),
    (
        """ALTER TABLE fruits ADD COLUMN test_1 varchar, ADD COLUMN test_2 integer,
                                         DROP COLUMN name;
        ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)""",
        [
            (1, "fruit_id", "integer", True),
            (3, "test_1", "character varying", False),
            (4, "test_2", "integer", False),
        ],
    ),
]

OLD_SCHEMA = [(1, "fruit_id", "integer", False), (2, "name", "character varying", False)]


def _reassign_ordinals(schema):
    # When a table is created anew, its ordinals are made consecutive again.
    return [(i + 1, col[1], col[2], col[3]) for i, col in enumerate(schema)]


@pytest.mark.parametrize("test_case", TEST_CASES)
def test_schema_changes(pg_repo_local, test_case):
    action, expected_new_schema = test_case

    assert (
        pg_repo_local.engine.get_full_table_schema(pg_repo_local.to_schema(), "fruits")
        == OLD_SCHEMA
    )
    pg_repo_local.run_sql(action)
    pg_repo_local.commit_engines()
    assert (
        pg_repo_local.engine.get_full_table_schema(pg_repo_local.to_schema(), "fruits")
        == expected_new_schema
    )

    head = pg_repo_local.head
    new_head = pg_repo_local.commit()

    # Test that the new image was stored as new object with the new schema.
    assert len(new_head.get_table("fruits").objects) == 1
    new_snap = new_head.get_table("fruits").objects[0]
    assert pg_repo_local.engine.get_object_schema(new_snap) == _reassign_ordinals(
        expected_new_schema
    ) + [(len(expected_new_schema) + 1, SG_UD_FLAG, "boolean", False)]

    head.checkout()
    assert (
        pg_repo_local.engine.get_full_table_schema(pg_repo_local.to_schema(), "fruits")
        == OLD_SCHEMA
    )

    new_head.checkout()
    assert pg_repo_local.engine.get_full_table_schema(
        pg_repo_local.to_schema(), "fruits"
    ) == _reassign_ordinals(expected_new_schema)


def test_pk_preserved_on_checkout(pg_repo_local):
    assert list(pg_repo_local.engine.get_primary_keys(pg_repo_local.to_schema(), "fruits")) == []
    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    assert list(pg_repo_local.engine.get_primary_keys(pg_repo_local.to_schema(), "fruits")) == [
        ("fruit_id", "integer")
    ]
    head = pg_repo_local.head
    new_head = pg_repo_local.commit()
    assert list(pg_repo_local.engine.get_primary_keys(pg_repo_local.to_schema(), "fruits")) == [
        ("fruit_id", "integer")
    ]

    head.checkout()
    assert list(pg_repo_local.engine.get_primary_keys(pg_repo_local.to_schema(), "fruits")) == []
    new_head.checkout()
    assert list(pg_repo_local.engine.get_primary_keys(pg_repo_local.to_schema(), "fruits")) == [
        ("fruit_id", "integer")
    ]
