import operator
from datetime import date, datetime as dt
from decimal import Decimal
from functools import reduce
from unittest.mock import patch

import pytest
from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core.sql import select
from splitgraph.core.fragment_manager import Digest
from splitgraph.core.metadata_manager import OBJECT_COLS
from splitgraph.core.repository import Repository
from splitgraph.core.types import TableColumn
from splitgraph.engine import ResultShape
from splitgraph.hooks.s3_server import delete_objects, list_objects
from test.splitgraph.commands.test_layered_querying import _prepare_fully_remote_repo
from test.splitgraph.conftest import OUTPUT, PG_DATA, SMALL_OBJECT_SIZE


def test_diff_head(pg_repo_local):
    pg_repo_local.run_sql(
        """INSERT INTO fruits VALUES (3, 'mayonnaise');
        DELETE FROM fruits WHERE name = 'apple'"""
    )
    pg_repo_local.commit_engines()  # otherwise the audit trigger won't see this
    assert pg_repo_local.engine.has_pending_changes(pg_repo_local.to_schema())
    change = pg_repo_local.diff("fruits", image_1=pg_repo_local.head.image_hash, image_2=None)
    # Added (3, mayonnaise); Deleted (1, 'apple')
    assert sorted(change) == [(False, (1, "apple")), (True, (3, "mayonnaise"))]


# Run some tests in multiple commit modes:
# * SNAP: refragment and store the table as a new object(s)
# * DIFF: delta-compress the changes and only store those
# * DIFF_SPLIT: split changes according to fragment boundaries
_COMMIT_MODES = ["SNAP", "DIFF", "DIFF_SPLIT"]


def _commit(repo, mode, **kwargs):
    if mode == "SNAP":
        return repo.commit(snap_only=True, split_changeset=False, **kwargs)
    if mode == "DIFF":
        return repo.commit(snap_only=False, split_changeset=False, **kwargs)
    if mode == "DIFF_SPLIT":
        return repo.commit(snap_only=False, split_changeset=True, **kwargs)


@pytest.mark.parametrize("mode", _COMMIT_MODES)
def test_commit_diff(mode, pg_repo_local):
    assert (pg_repo_local.to_schema(), "fruits") in pg_repo_local.engine.get_tracked_tables()

    pg_repo_local.run_sql(
        """INSERT INTO fruits VALUES (3, 'mayonnaise');
        DELETE FROM fruits WHERE name = 'apple';
        UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2;
        COMMENT ON COLUMN fruits.name IS 'Name of the fruit'"""
    )

    head = pg_repo_local.head.image_hash
    old_objects = pg_repo_local.head.get_table("fruits").objects

    new_head = _commit(pg_repo_local, mode, comment="test commit")

    # After commit, we should be switched to the new commit hash and there should be no differences.
    assert pg_repo_local.head == new_head
    assert pg_repo_local.diff("fruits", image_1=new_head, image_2=None) == []

    # Test object structure
    table = pg_repo_local.head.get_table("fruits")
    assert table.table_schema == [
        TableColumn(1, "fruit_id", "integer", False),
        TableColumn(2, "name", "character varying", False, "Name of the fruit"),
    ]

    obj = table.objects[0]
    obj_meta = pg_repo_local.objects.get_object_meta([obj])[obj]
    # Check object size has been written
    assert obj_meta.size >= 0

    if mode == "SNAP":
        assert all(o not in old_objects for o in table.objects)
    else:
        # Even though we've changed the comment on the fruits column, we still
        # delta compressed the table.
        assert len(table.objects) > 1
        assert obj == old_objects[0]

    assert new_head.comment == "test commit"
    change = pg_repo_local.diff("fruits", image_1=head, image_2=new_head)
    # pk (no PK here so the whole row) -- 0 for INS -- extra non-PK cols
    assert sorted(change) == [  # 1, apple deleted
        (False, (1, "apple")),
        # 2, orange deleted and 2, guitar added (PK (whole tuple) changed)
        (False, (2, "orange")),
        (True, (2, "guitar")),
        (True, (3, "mayonnaise")),
    ]

    assert pg_repo_local.diff("vegetables", image_1=head, image_2=new_head) == []


def _compare_object_meta(meta, expected, size_tolerance=150):
    """Check that the object metadata is as expected -- everything
    must be exactly equal apart from size which can vary within `size_tolerance`."""

    if len(meta) != len(expected):
        raise AssertionError

    size_index = OBJECT_COLS.index("size")
    for i in range(len(meta)):
        if i == size_index:
            assert abs(meta[i] - expected[i]) <= size_tolerance
        else:
            assert meta[i] == expected[i]


def test_commit_chunking(local_engine_empty):
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    for i in range(11):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord("a") + i), i * 2))
    with patch("splitgraph.core.fragment_manager.datetime") as dtp:
        dtp.now.return_value = dt(2019, 1, 1)
        head = OUTPUT.commit(chunk_size=5)

    # Should produce 3 objects: PK 1..5, PK 6..10, PK 11
    objects = head.get_table("test").objects
    assert len(objects) == 3
    object_meta = OUTPUT.objects.get_object_meta(objects)

    for i, obj in enumerate(objects):
        # Check object metadata
        min_key = i * 5 + 1
        max_key = min(i * 5 + 5, 11)

        _compare_object_meta(
            object_meta[obj],
            (
                obj,  # object ID
                "FRAG",  # fragment format
                "",  # no namespace
                SMALL_OBJECT_SIZE,
                dt(2019, 1, 1),
                # Don't check the insertion hash in this test
                object_meta[obj].insertion_hash,
                # Object didn't delete anything, so the deletion hash is zero.
                "0" * 64,
                # index
                {
                    "range": {
                        "key": [min_key, max_key],
                        "value_1": [chr(ord("a") + min_key - 1), chr(ord("a") + max_key - 1)],
                        "value_2": [(min_key - 1) * 2, (max_key - 1) * 2],
                    }
                },
            ),
        )

        # Check object contents
        assert local_engine_empty.run_sql(
            SQL("SELECT key FROM {}.{} ORDER BY key").format(
                Identifier(SPLITGRAPH_META_SCHEMA), Identifier(obj)
            ),
            return_shape=ResultShape.MANY_ONE,
        ) == list(range(min_key, max_key + 1))


def test_commit_chunking_order(local_engine_empty):
    # Same but make sure the chunks order by PK for more efficient indexing
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    for i in range(10, -1, -1):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord("a") + i), i * 2))
    head = OUTPUT.commit(chunk_size=5)

    objects = head.get_table("test").objects
    assert len(objects) == 3
    for i, obj in enumerate(objects):
        min_key = i * 5 + 1
        max_key = min(i * 5 + 5, 11)
        assert local_engine_empty.run_sql(
            SQL("SELECT key FROM {}.{} ORDER BY key").format(
                Identifier(SPLITGRAPH_META_SCHEMA), Identifier(obj)
            ),
            return_shape=ResultShape.MANY_ONE,
        ) == list(range(min_key, max_key + 1))


def test_commit_diff_splitting(local_engine_empty):
    # Similar setup to the chunking test
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    for i in range(11):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord("a") + i), i * 2))
    head = OUTPUT.commit(chunk_size=5)

    # Store the IDs of the base fragments
    base_objects = head.get_table("test").objects

    assert base_objects == [
        "oab9901c63e816f8e3d47366740a76323f168fbe5d5b25eed6b8f4755c37e10",
        "o899fa68202a31c0e10c98c446d9ffef8812d7bdce76ef9da92545172fbb47b",
        "oe1841cba2d9d9598f2cd3226bb60bb77dc77b98a05808e7976ed112bade86f",
    ]

    # Create a change that affects multiple base fragments:
    # INSERT PK 0 (goes before all fragments)
    OUTPUT.run_sql("INSERT INTO test VALUES (0, 'zero', -1)")

    # UPDATE the first fragment and delete a value from it
    OUTPUT.run_sql("DELETE FROM test WHERE key = 4")
    OUTPUT.run_sql("UPDATE test SET value_1 = 'UPDATED' WHERE key = 5")

    # Second fragment -- UPDATE then DELETE (test that gets conflated into a single change)
    OUTPUT.run_sql("UPDATE test SET value_1 = 'UPDATED' WHERE key = 6")
    OUTPUT.run_sql("DELETE FROM test WHERE key = 6")

    # INSERT a value after the last fragment (PK 12) -- should become a new fragment
    OUTPUT.run_sql("INSERT INTO test VALUES (12, 'l', 22)")

    # Now check the objects that were created.
    with patch("splitgraph.core.fragment_manager.datetime") as dtp:
        dtp.now.return_value = dt(2019, 1, 1)
        new_head = OUTPUT.commit(split_changeset=True)
    new_head.checkout()
    new_objects = new_head.get_table("test").objects

    # We expect 4 new objects at the end of the table meta:
    #   * one with PK 0
    #   * one with PKs 4 and 5 (partially overwriting the first base fragment)
    #   * one with PK 6 (updating the second base fragment)
    #   * one with PK 12 (end of the table)
    added_objects = [
        "o99852c525a1ce918bbfe1206d2d556f5a72f96c45b3e88efba88f853d2b963",
        "oe5bf159b64337a12623bb048e03c19406611ae8adf1ff60d7eb48817a45c18",
        "o1f065efbddb5004585b5c74cd614f1f66cfd9ceda622842ec0874be072def3",
        "o52e6b337372c63b8afddc52cab9ef16f899d836048692c42cf6fed2ebcf30a",
    ]
    assert new_objects == base_objects + added_objects

    # Check the contents of the newly created objects.
    assert OUTPUT.run_sql(select(added_objects[0])) == [
        (True, 0, "zero", -1)
    ]  # upserted=True, key, new_value_1, new_value_2
    assert OUTPUT.run_sql(select(added_objects[1])) == [
        (True, 5, "UPDATED", 8),  # upserted=True, key, new_value_1, new_value_2
        (False, 4, None, None),
    ]  # upserted=False, key, None, None
    assert OUTPUT.run_sql(select(added_objects[2])) == [
        (False, 6, None, None)
    ]  # upserted=False, key, None, None
    assert OUTPUT.run_sql(select(added_objects[3])) == [
        (True, 12, "l", 22)
    ]  # upserted=True, key, new_value_1, new_value_2

    object_meta = OUTPUT.objects.get_object_meta(added_objects)
    # The new fragments should be at the end of the table objects' list.
    expected_meta = [
        (
            added_objects[0],
            "FRAG",
            "",
            SMALL_OBJECT_SIZE,
            dt(2019, 1, 1),
            "3cfbe8fa6fc546264936e29f402d7263510481c88a8d27190d82b3d5830cbcbf",
            # no deletions in this fragment
            "0000000000000000000000000000000000000000000000000000000000000000",
            {"range": {"key": [0, 0], "value_1": ["zero", "zero"], "value_2": [-1, -1]}},
        ),
        (
            added_objects[1],
            "FRAG",
            "",
            SMALL_OBJECT_SIZE,
            dt(2019, 1, 1),
            "470425e8c107fff67264f9f812dfe211c7e625edf651947b8476f60392c57281",
            "e3eb6db305d889d3a69e3d8efa0931853c00fca75c0aeddd8f2fa2d6fd2443d6",
            # for value_1 we have old values for k=4,5 ('d', 'e') and new value
            # for k=5 ('UPDATED') included here; same for value_2.
            {"range": {"key": [4, 5], "value_1": ["UPDATED", "e"], "value_2": [6, 8]}},
        ),
        (
            added_objects[2],
            "FRAG",
            "",
            SMALL_OBJECT_SIZE,
            dt(2019, 1, 1),
            # UPD + DEL conflated so nothing gets inserted by this fragment
            "0000000000000000000000000000000000000000000000000000000000000000",
            "d7df15e62c1c8799ef3a3677e3eb7661cedf898d73449a80251b93c501b5bdeb",
            # Turned into one deletion, old values included here
            {"range": {"key": [6, 6], "value_1": ["f", "f"], "value_2": [10, 10]}},
        ),
        (
            added_objects[3],
            "FRAG",
            "",
            SMALL_OBJECT_SIZE,
            dt(2019, 1, 1),
            "96f0a7394f3839b048b492b789f7d57cf976345b04938a69d82b3512f72c3e9e",
            "0000000000000000000000000000000000000000000000000000000000000000",
            {"range": {"key": [12, 12], "value_1": ["l", "l"], "value_2": [22, 22]}},
        ),
    ]
    for added_object, expected in zip(added_objects, expected_meta):
        _compare_object_meta(object_meta[added_object], expected)

    # Also check that the insertion - deletion hashes of objects add up to the final content hash of the materialized
    # table.
    table_hash = OUTPUT.objects.calculate_content_hash(OUTPUT.to_schema(), "test")

    all_meta = OUTPUT.objects.get_object_meta(new_objects)
    hash_sum = reduce(
        operator.add,
        (
            Digest.from_hex(o.insertion_hash) - Digest.from_hex(o.deletion_hash)
            for o in all_meta.values()
        ),
    )
    assert (
        hash_sum.hex()
        == table_hash
        == "08598e27c62b61d308ed073c46b4473878aeae00d043a849365fbfe7cbc5a579"
    )


def test_commit_diff_splitting_composite(local_engine_empty):
    # Test splitting works correctly on composite PKs (see a big comment in _extract_min_max_pks for the explanation).
    # Also test timestamp PKs at the same time.

    OUTPUT.init()
    OUTPUT.run_sql(
        "CREATE TABLE test (key_1 TIMESTAMP, key_2 INTEGER, value VARCHAR, PRIMARY KEY (key_1, key_2))"
    )
    for i in range(10):
        # Set up something like
        # 1/1/2019, 1, '1'
        # 1/1/2019, 2, '2'
        # 1/1/2019, 3, '3'
        # 2/1/2019, 1, '4'
        # 2/1/2019, 2, '5'
        # --- chunk boundary
        # 2/1/2019, 3, '6'
        # ...
        # 4/1/2019, 1, '10'
        OUTPUT.run_sql(
            "INSERT INTO test VALUES (%s, %s, %s)", (dt(2019, 1, i // 3 + 1), i % 3 + 1, str(i))
        )

    head = OUTPUT.commit(chunk_size=5)
    base_objects = head.get_table("test").objects
    assert len(base_objects) == 2

    # INSERT at 1/1/2019, 4 -- should match first chunk even though it doesn't exist in it.
    OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (dt(2019, 1, 1), 4, "NEW"))

    # UPDATE 2/1/2019, 2 -- if we index by PK columns individually, it's ambiguous where
    # this should fit (as both chunks span key_1=2/1/2019 and key_2=2) but it should go into the first
    # chunk.
    OUTPUT.run_sql(
        "UPDATE test SET value = 'UPD' WHERE key_1 = %s AND key_2 = %s", (dt(2019, 1, 2), 2)
    )

    # INSERT a new value that comes after all chunks
    OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (dt(2019, 1, 4), 2, "NEW"))

    with patch("splitgraph.core.fragment_manager.datetime") as dtp:
        dtp.now.return_value = dt(2019, 1, 1)
        new_head = OUTPUT.commit(split_changeset=True)
    new_head.checkout()
    new_objects = new_head.get_table("test").objects

    assert len(new_objects) == 4
    assert new_objects[:2] == base_objects
    # First new chunk: overwrites the old first chunk, contains the INSERT (1/1/2019, 4) and the UPDATE (2/1/2019, 2)
    object_meta = OUTPUT.objects.get_object_meta(new_objects)
    _compare_object_meta(
        object_meta[new_objects[2]],
        (
            new_objects[2],
            "FRAG",
            "",
            SMALL_OBJECT_SIZE,
            dt(2019, 1, 1),
            # Hashes here just copypasted from the test output, see test_object_hashing for actual tests that
            # test each part of hash generation
            "00964b1b5db0d6e8d42b48462e9021ed626a773380345a1f4fee5c205d7d4beb",
            "0c8c07c66327f4493c716ceafd4bf70b692a1d6fe7cb1b88e1d683a4ea0bc4e8",
            {
                "range": {
                    "$pk": [["2019-01-01 00:00:00", 4], ["2019-01-02 00:00:00", 2]],
                    "key_1": ["2019-01-01 00:00:00", "2019-01-02 00:00:00"],
                    # 'value' spans the old value (was '5'), the inserted value ('4') and the new updated value ('UPD').
                    "key_2": [2, 4],
                    "value": ["4", "UPD"],
                }
            },
        ),
    )
    # The third chunk is new, contains (4/1/2019, 2, NEW)
    _compare_object_meta(
        object_meta[new_objects[3]],
        (
            new_objects[3],
            "FRAG",
            "",
            SMALL_OBJECT_SIZE,
            dt(2019, 1, 1),
            "8c92b3ea89234b06cf53c2bad9d6e1d49dc561dc8c0100ee63c0b9ce1eeb0750",
            # Nothing deleted, so the deletion hash is 0
            "0000000000000000000000000000000000000000000000000000000000000000",
            {
                "range": {
                    "$pk": [["2019-01-04 00:00:00", 2], ["2019-01-04 00:00:00", 2]],
                    "key_1": ["2019-01-04 00:00:00", "2019-01-04 00:00:00"],
                    "key_2": [2, 2],
                    "value": ["NEW", "NEW"],
                }
            },
        ),
    )


def test_commit_mode_change(pg_repo_local):
    # Test committing with splitting by fragment group boundaries after not doing so.
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    for i in range(5):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord("a") + i), i * 2))
    OUTPUT.commit(chunk_size=None)
    assert OUTPUT.head.get_table("test").objects == [
        "oab9901c63e816f8e3d47366740a76323f168fbe5d5b25eed6b8f4755c37e10"
    ]

    # Insert expanding the fragment both to the left and to the right
    for i in [-2, -1, 5, 6]:
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord("a") + i), i * 2))

    OUTPUT.commit()
    # Here, we commit with split_changeset=False, so the new object doesn't get split across the current
    # table object's boundaries. This means that the original table has one "chunk group" spanning 1..6 and
    # the new one has one "chunk group" spanning -1..7 (thus expanding it).

    assert OUTPUT.head.get_table("test").objects == [
        "oab9901c63e816f8e3d47366740a76323f168fbe5d5b25eed6b8f4755c37e10",
        "o20132b896017b209e9cbe82ef33a676435f9ead6b62c4aea53e6fbaad8502a",
    ]

    # Since the group was expanded, this changeset will be split to match the -1..7 expanded
    # group boundaries: the updates will go into one fragment, the insert will go into another.

    OUTPUT.run_sql("UPDATE test SET value_1 = 'UPDATED' WHERE key = -1")
    OUTPUT.run_sql("UPDATE test SET value_1 = 'UPDATED' WHERE key = 6")
    OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (8, "something", 42))
    OUTPUT.commit(split_changeset=True)

    table = OUTPUT.head.get_table("test")
    assert table.objects == [
        # The original two objects
        "oab9901c63e816f8e3d47366740a76323f168fbe5d5b25eed6b8f4755c37e10",
        "o20132b896017b209e9cbe82ef33a676435f9ead6b62c4aea53e6fbaad8502a",
        # The new object (spanning keys -1..6)
        "o9466c5547cf536b5ab6bebe3a1d9b78102976ddff512d02900bf11b7a7cb86",
        # The second new object (key = 8 insert)
        "o77674830056441f5215e56169e2646c935c302504f7b6b4cc2ddd319c1fc81",
    ]

    # Check object contents
    assert OUTPUT.run_sql(select(table.objects[2])) == [
        (True, -1, "UPDATED", -4),
        (True, 6, "UPDATED", 10),
    ]

    assert OUTPUT.run_sql(select(table.objects[3])) == [(True, 8, "something", 42)]


def test_drop_recreate(pg_repo_local):
    # Drops the fruits table and recreates it with the same schema -- check the same objects are recreated.
    old_objects = pg_repo_local.head.get_table("fruits").objects
    pg_repo_local.run_sql(PG_DATA)

    assert pg_repo_local.head.get_table("fruits").objects == old_objects


@pytest.mark.parametrize("mode", _COMMIT_MODES)
def test_commit_diff_remote(mode, pg_repo_remote):
    # Rerun the previous test against the remote fixture to make sure it works without
    # dependencies on the get_engine()
    test_commit_diff(mode, pg_repo_remote)


@pytest.mark.parametrize("snap_only", [True, False])
def test_commit_on_empty(snap_only, pg_repo_local):
    OUTPUT.init()
    OUTPUT.run_sql('CREATE TABLE test AS SELECT * FROM "test/pg_mount".fruits')

    # Make sure the pending changes get flushed anyway if we are only committing a snapshot.
    assert OUTPUT.diff("test", image_1=OUTPUT.head.image_hash, image_2=None) is True
    OUTPUT.commit(snap_only=snap_only)
    assert OUTPUT.diff("test", image_1=OUTPUT.head.image_hash, image_2=None) == []


@pytest.mark.mounting
@pytest.mark.parametrize("mode", _COMMIT_MODES)
def test_multiple_mountpoint_commit_diff(mode, pg_repo_local, mg_repo_local):
    pg_repo_local.run_sql(
        """INSERT INTO fruits VALUES (3, 'mayonnaise');
        DELETE FROM fruits WHERE name = 'apple';
        UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2;"""
    )
    mg_repo_local.run_sql("UPDATE stuff SET duration = 11 WHERE name = 'James'")
    # Both repositories have pending changes if we commit the PG connection.
    pg_repo_local.commit_engines()
    assert mg_repo_local.has_pending_changes()
    assert pg_repo_local.has_pending_changes()

    head = pg_repo_local.head.image_hash
    mongo_head = mg_repo_local.head
    new_head = _commit(pg_repo_local, mode)

    change = pg_repo_local.diff("fruits", image_1=head, image_2=new_head)
    assert sorted(change) == [
        (False, (1, "apple")),
        (False, (2, "orange")),
        (True, (2, "guitar")),
        (True, (3, "mayonnaise")),
    ]

    # PG has no pending changes, Mongo does
    assert mg_repo_local.head == mongo_head
    assert mg_repo_local.has_pending_changes()
    assert not pg_repo_local.has_pending_changes()

    # Discard the write to the mongodb
    mongo_head.checkout(force=True)
    assert not mg_repo_local.has_pending_changes()
    assert not pg_repo_local.has_pending_changes()
    assert mg_repo_local.run_sql("SELECT duration from stuff WHERE name = 'James'") == [
        (Decimal(2),)
    ]

    # Update and commit
    mg_repo_local.run_sql("UPDATE stuff SET duration = 15 WHERE name = 'James'")
    mg_repo_local.commit_engines()
    assert mg_repo_local.has_pending_changes()
    new_mongo_head = _commit(mg_repo_local, mode)
    assert not mg_repo_local.has_pending_changes()
    assert not pg_repo_local.has_pending_changes()

    mongo_head.checkout()
    assert mg_repo_local.run_sql("SELECT duration from stuff WHERE name = 'James'") == [
        (Decimal(2),)
    ]

    new_mongo_head.checkout()
    assert mg_repo_local.run_sql("SELECT duration from stuff WHERE name = 'James'") == [
        (Decimal(15),)
    ]
    assert not mg_repo_local.has_pending_changes()
    assert not pg_repo_local.has_pending_changes()


def test_delete_all_diff(pg_repo_local):
    pg_repo_local.run_sql("DELETE FROM fruits")
    pg_repo_local.commit_engines()
    assert pg_repo_local.has_pending_changes()
    expected_diff = [(False, (1, "apple")), (False, (2, "orange"))]

    actual_diff = pg_repo_local.diff("fruits", pg_repo_local.head.image_hash, None)
    print(actual_diff)
    assert actual_diff == expected_diff
    new_head = pg_repo_local.commit()
    assert not pg_repo_local.has_pending_changes()
    assert pg_repo_local.diff("fruits", new_head, None) == []
    assert pg_repo_local.diff("fruits", new_head.parent_id, new_head) == expected_diff


@pytest.mark.parametrize("mode", _COMMIT_MODES)
def test_diff_across_far_commits(mode, pg_repo_local):
    head = pg_repo_local.head.image_hash
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    _commit(pg_repo_local, mode)

    pg_repo_local.run_sql("DELETE FROM fruits WHERE name = 'apple'")
    _commit(pg_repo_local, mode)

    pg_repo_local.run_sql("UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2")
    new_head = _commit(pg_repo_local, mode)

    change = pg_repo_local.diff("fruits", head, new_head)
    assert sorted(change) == [
        (False, (1, "apple")),
        (False, (2, "orange")),
        (True, (2, "guitar")),
        (True, (3, "mayonnaise")),
    ]
    change_agg = pg_repo_local.diff("fruits", head, new_head, aggregate=True)
    assert change_agg == (2, 2, 0)


@pytest.mark.parametrize("snap_only", [True, False])
def test_non_ordered_inserts(snap_only, pg_repo_local):
    head = pg_repo_local.head.image_hash
    pg_repo_local.run_sql("INSERT INTO fruits (name, fruit_id) VALUES ('mayonnaise', 3)")
    new_head = pg_repo_local.commit(snap_only=snap_only)

    change = pg_repo_local.diff("fruits", head, new_head)
    assert change == [(True, (3, "mayonnaise"))]


@pytest.mark.parametrize("snap_only", [True, False])
def test_non_ordered_inserts_with_pk(snap_only, local_engine_empty):
    OUTPUT.init()
    OUTPUT.run_sql(
        """CREATE TABLE test
        (a int, 
         b int,
         c int,
         d varchar, PRIMARY KEY(a))"""
    )
    head = OUTPUT.commit()

    OUTPUT.run_sql("INSERT INTO test (a, d, b, c) VALUES (1, 'four', 2, 3)")
    new_head = OUTPUT.commit(snap_only=snap_only)
    head.checkout()
    new_head.checkout()
    assert OUTPUT.run_sql("SELECT * FROM test") == [(1, 2, 3, "four")]
    change = OUTPUT.diff("test", head, new_head)
    assert len(change) == 1
    assert change[0] == (True, (1, 2, 3, "four"))


def _write_multitype_dataset():
    # Schema/data copied from the wal2json tests
    OUTPUT.init()
    OUTPUT.run_sql(
        """CREATE TABLE test
        (a smallserial, 
         b smallint,
         c int,
         d bigint,
         e numeric(5,3),
         f real not null,
         g double precision,
         h char(10),
         i varchar(30),
         j text,
         k bit varying(20),
         l timestamp,
         m date,
         n boolean not null,
         o json,
         p tsvector, PRIMARY KEY(b, c, d));"""
    )

    OUTPUT.run_sql(
        """INSERT INTO test (b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
        VALUES(1, 2, 3, 3.54, 876.563452345, 1.23, 'test', 'testtesttesttest', 'testtesttesttesttesttesttest',
        B'001110010101010', '2013-11-02 17:30:52', '2013-02-04', true, '{ "a": 123 }',
        'Old Old Parr'::tsvector);"""
    )

    # Write another row to test min/max indexing on the resulting object
    OUTPUT.run_sql(
        """INSERT INTO test (b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
        VALUES(15, 22, 1, -1.23, 9.8811, 0.23, 'abcd', '0testtesttesttes', '0testtesttesttesttesttesttes',
        B'111110011111111', '2016-01-01 01:01:05', '2011-11-11', false, '{ "b": 456 }',
        'AAA AAA Bb '::tsvector);"""
    )

    new_head = OUTPUT.commit()
    # Check out the new image again to verify that writing the new image works
    new_head.checkout()

    return new_head


def test_various_types(local_engine_empty):
    new_head = _write_multitype_dataset()

    assert OUTPUT.run_sql("SELECT * FROM test") == [
        (
            1,
            1,
            2,
            3,
            Decimal("3.540"),
            876.563,
            1.23,
            "test",
            "testtesttesttest",
            "testtesttesttesttesttesttest",
            "001110010101010",
            dt(2013, 11, 2, 17, 30, 52),
            date(2013, 2, 4),
            True,
            {"a": 123},
            "'Old' 'Parr'",
        ),
        (
            (
                2,
                15,
                22,
                1,
                Decimal("-1.230"),
                9.8811,
                0.23,
                "abcd",
                "0testtesttesttes",
                "0testtesttesttesttesttesttes",
                "111110011111111",
                dt(2016, 1, 1, 1, 1, 5),
                date(2011, 11, 11),
                False,
                {"b": 456},
                "'AAA' 'Bb'",
            )
        ),
    ]

    object_id = new_head.get_table("test").objects[0]
    object_index = OUTPUT.objects.get_object_meta([object_id])[object_id].object_index
    expected = {
        "range": {
            # PK was (b, c, d) and we have two tuples: old (1, 2, 3)
            # and new (15, 22, 1). Even though the new d (3) is greater than old (1),
            # the composite PK isn't.
            "$pk": [[1, 2, 3], [15, 22, 1]],
            "a": [1, 2],
            "b": [1, 15],
            "c": [2, 22],
            "d": [1, 3],
            "e": ["-1.230", "3.540"],
            "f": [9.8811, 876.563],
            "g": [0.23, 1.23],
            "h": ["abcd", "test"],
            "i": ["0testtesttesttes", "testtesttesttest"],
            "j": ["0testtesttesttesttesttesttes", "testtesttesttesttesttesttest"],
            "l": ["2013-11-02 17:30:52", "2016-01-01 01:01:05"],
            "m": ["2011-11-11", "2013-02-04"],
        }
    }

    assert object_index == expected


def test_various_types_with_deletion_index(local_engine_empty):
    _write_multitype_dataset()
    # Insert a row, update a row and delete a row -- check the old rows are included in the index correctly.
    OUTPUT.run_sql(
        """INSERT INTO test (b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
        VALUES(16, 23, 2, 7.89, 10.01, 9.45, 'defg', '00esttesttesttes', '00esttesttesttesttesttesttes',
        B'111110011111111', '2016-02-01 01:01:05.123456', '2012-12-12', false, '{ "b": 789 }',
        'BBB BBB Cc '::tsvector);"""
    )
    OUTPUT.run_sql("DELETE FROM test WHERE b = 15")
    OUTPUT.run_sql("UPDATE test SET m = '2019-01-01' WHERE m = '2013-02-04'")
    new_head = OUTPUT.commit()

    # New object is the second object in the table objects' list
    object_id = new_head.get_table("test").objects[1]
    object_index = OUTPUT.objects.get_object_meta([object_id])[object_id].object_index
    # Deleted row for reference:
    # 15, 22, 1, -1.23, 9.8811, 0.23, 'abcd', '0testtesttesttes', '0testtesttesttesttesttesttes',
    #   B'111110011111111', '2016-01-01 01:01:05', '2011-11-11', false, '{ "b": 456 }',
    #   'AAA AAA Bb '::tsvector
    # Updated row for reference:
    # 1, 2, 3, 3.54, 876.563452345, 1.23, 'test', 'testtesttesttest', 'testtesttesttesttesttesttest',
    #   B'001110010101010', '2013-11-02 17:30:52', '2013-02-04', true, '{ "a": 123 }',
    #   'Old Old Parr'::tsvector

    # Make sure all rows are included since they were all affected + both old and new value for the update
    expected = {
        "range": {
            # Original PK (b, c, d): (1, 2, 3)
            # We inserted (16, 23, 2) and that's the largest PK tuple.
            "$pk": [[1, 2, 3], [16, 23, 2]],
            "a": [1, 2],  # this one is a sequence so is kinda broken
            "b": [1, 16],  # original values 1 (updated row), 15 (deleted row), 16 (inserted)
            "c": [2, 23],  # 2 (U), 22 (D), 23 (I)
            "d": [1, 3],  # 3 (U), 1 (D), 2 (I)
            "e": [-1.23, "7.89"],  # 3.54 (U), -1.23 (D), 7.89 (I) -- also wtf is up with types?
            "f": [9.8811, 876.563],  # 876.563 (U, numeric(5,3) so truncated), 9.8811 (D), 10.01 (I)
            "g": [0.23, 9.45],  # 1.23 (U), 0.23 (D), 9.45 (I)
            "h": ["abcd", "test"],  # test (U), abcd (D), defg (I)
            "i": ["00esttesttesttes", "testtesttesttest"],  # test... (U), 0tes... (D), 00te... (I)
            # same as previous here
            "j": ["00esttesttesttesttesttesttes", "testtesttesttesttesttesttest"],
            # 2013-11 (U), 2016-01 (D), 2016-02 (I)
            "l": ["2013-11-02 17:30:52", "2016-02-01 01:01:05.123456"],
            # 2013 (U, old value), 2016 (D), 2012 (I), 2019 (U, new value)
            "m": ["2011-11-11", "2019-01-01"],
        }
    }

    assert object_index == expected


def test_empty_diff_reuses_object(pg_repo_local):
    head_1 = pg_repo_local.commit()

    table_meta_1 = pg_repo_local.head.get_table("fruits").objects
    table_meta_2 = head_1.get_table("fruits").objects

    assert table_meta_1 == table_meta_2
    assert len(table_meta_2) == 1


def test_update_packing_applying(pg_repo_local):
    # Set fruit_id to be the PK first so that an UPDATE operation is stored in the patch.
    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    old_head = pg_repo_local.commit()

    pg_repo_local.run_sql("UPDATE fruits SET name = 'pineapple' WHERE fruit_id = 1")
    new_head = pg_repo_local.commit()

    pg_repo_local.run_sql("UPDATE fruits SET name = 'kumquat' WHERE fruit_id = 2")
    v_new_head = pg_repo_local.commit()

    old_head.checkout()
    assert pg_repo_local.run_sql("SELECT * FROM fruits ORDER BY fruit_id") == [
        (1, "apple"),
        (2, "orange"),
    ]

    new_head.checkout()
    assert pg_repo_local.run_sql("SELECT * FROM fruits ORDER BY fruit_id") == [
        (1, "pineapple"),
        (2, "orange"),
    ]

    v_new_head.checkout()
    assert pg_repo_local.run_sql("SELECT * FROM fruits ORDER BY fruit_id") == [
        (1, "pineapple"),
        (2, "kumquat"),
    ]


def test_diff_staging_aggregation(pg_repo_local):
    # Test diff from HEAD~1 to the current staging area (accumulate actual fragment with the pending changes)
    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    old_head = pg_repo_local.commit()

    pg_repo_local.run_sql("UPDATE fruits SET name = 'pineapple' WHERE fruit_id = 1")
    pg_repo_local.commit()
    pg_repo_local.run_sql("UPDATE fruits SET name = 'mustard' WHERE fruit_id = 2")

    assert pg_repo_local.diff("fruits", old_head, None, aggregate=True) == (2, 2, 0)
    assert pg_repo_local.diff("fruits", old_head, None, aggregate=False) == [
        (False, (1, "apple")),
        (False, (2, "orange")),
        (True, (1, "pineapple")),
        (True, (2, "mustard")),
    ]


def test_diff_schema_change(pg_repo_local):
    # Test diff when there's been a schema change and so we stored the table as a full snapshot.
    old_head = pg_repo_local.head.image_hash
    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    pg_repo_local.commit()

    pg_repo_local.run_sql("UPDATE fruits SET name = 'pineapple' WHERE fruit_id = 1")
    after_update = pg_repo_local.commit()
    pg_repo_local.run_sql("UPDATE fruits SET name = 'mustard' WHERE fruit_id = 2")

    # Can't detect an UPDATE since there's been a schema change (old table has no PK)
    assert pg_repo_local.diff("fruits", old_head, after_update, aggregate=True) == (1, 1, 0)
    assert pg_repo_local.diff("fruits", old_head, after_update, aggregate=False) == [
        (False, (1, "apple")),
        (True, (1, "pineapple")),
    ]

    # Again can't detect UPDATEs -- delete 2 rows, add two rows
    assert pg_repo_local.diff("fruits", old_head, None, aggregate=True) == (2, 2, 0)
    assert pg_repo_local.diff("fruits", old_head, None, aggregate=False) == [
        (False, (1, "apple")),
        (False, (2, "orange")),
        (True, (1, "pineapple")),
        (True, (2, "mustard")),
    ]


def test_multiengine_object_gets_recreated(local_engine_empty, pg_repo_remote, clean_minio):
    # Use the remote engine as a metadata store and the local engine as an object store -- make sure
    # the object gets re-registered on the remote engine if it gets deleted from it but not the
    # cache.
    _prepare_fully_remote_repo(local_engine_empty, pg_repo_remote)
    pg_repo_local = Repository.from_template(pg_repo_remote, object_engine=local_engine_empty)

    # Checkout to force objects to download
    pg_repo_local.images["latest"].checkout()
    pg_repo_local.uncheckout()
    parent = pg_repo_local.images[pg_repo_local.images["latest"].parent_id]

    old_objects = [
        "of22f20503d3bf17c7449b545d68ebcee887ed70089f0342c4bff38862c0dc5",
        "of0fb43e477311f82aa30055be303ff00599dfe155d737def0d00f06e07228b",
        "o23fe42d48d7545596d0fea1c48bcf7d64bde574d437c77cc5bb611e5f8849d",
        "o3f81f6c40ecc3366d691a2ce45f41f6f180053020607cbd0873baf0c4447dc",
    ]
    assert parent.get_table("fruits").objects == old_objects

    # Delete an image from the metadata engine and delete the corresponding object
    # from meta and Minio
    objects = pg_repo_local.images["latest"].get_table("fruits").objects
    assert objects == old_objects + [
        "oc27ee277aff108525a2df043d9efdaa1c3e26a4949a6cf6b53ee0c889c8559"
    ]
    object_to_delete = objects[-1]
    object_to_delete_meta = pg_repo_local.objects.get_object_meta([object_to_delete])[
        object_to_delete
    ]

    pg_repo_local.images.delete([pg_repo_local.images["latest"].image_hash])
    pg_repo_local.engine.run_sql(
        "DELETE FROM splitgraph_meta.object_locations WHERE object_id = %s", (object_to_delete,)
    )
    pg_repo_local.engine.run_sql(
        "DELETE FROM splitgraph_meta.objects WHERE object_id = %s", (object_to_delete,)
    )
    delete_objects(clean_minio, [object_to_delete])

    # Check the parent image out and unknowingly recreate the image we deleted, with the same object.
    # _prepare_fully_remote_repo has default values for columns 'number' and 'timestamp' that
    # don't get set after a checkout
    parent.checkout()
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'kumquat', 1, '2019-01-01T12:00:00')")
    assert pg_repo_local.head.get_table("fruits").objects == old_objects
    new_image = pg_repo_local.commit()

    # Make sure the object was reregistered
    assert new_image.get_table("fruits").objects == old_objects + [object_to_delete]
    assert object_to_delete in pg_repo_local.objects.get_all_objects()
    assert object_to_delete in pg_repo_local.objects.get_downloaded_objects()

    # New object has same metadata apart from creation timestamp (slightly in the future)
    actual_meta = pg_repo_local.objects.get_object_meta([object_to_delete])[object_to_delete]
    expected_meta = object_to_delete_meta._replace(created=actual_meta.created)
    assert expected_meta == actual_meta

    # Force a reupload and make sure the object exists in Minio.
    assert object_to_delete not in list_objects(clean_minio)
    pg_repo_local.objects.make_objects_external([object_to_delete], handler="S3", handler_params={})
    assert object_to_delete in list_objects(clean_minio)
