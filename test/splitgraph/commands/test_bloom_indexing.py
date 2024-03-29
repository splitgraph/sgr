from datetime import datetime as dt
from datetime import timedelta
from test.splitgraph.commands.test_layered_querying import _prepare_fully_remote_repo
from test.splitgraph.conftest import OUTPUT
from unittest import mock

import pytest

from splitgraph.core.indexing.bloom import (
    _prepare_bloom_quals,
    describe,
    filter_bloom_index,
)
from splitgraph.core.repository import Repository, clone
from splitgraph.engine import ResultShape
from splitgraph.exceptions import ObjectIndexingError


@pytest.mark.parametrize(
    "test_case",
    [
        # Basic case: equality gets left alone
        ([[("a", "=", 5)]], [[("a", mock.ANY, mock.ANY)]]),
        # a = 5 or b > 6: since b > 6 might be true and we don't know it, this collapses
        # into nothing
        ([[("a", "=", 5), ("b", ">", 6)]], []),
        # a = 5 and b > 6: no matter whether b > 6 is true, if a can never be = 5,
        # this statement can still be false
        ([[("a", "=", 5)], [("b", ">", 6)]], [[("a", mock.ANY, mock.ANY)]]),
    ],
)
def test_bloom_qual_preprocessing(test_case):
    case, expected = test_case
    assert _prepare_bloom_quals(case) == expected


def test_bloom_describe():
    # This filter was created with probability = 0.01 and 25 items -- see
    # if describe() reinfers that roughly correctly.
    assert (
        describe([7, "uSP6qzHHDqVq/qHMlqrAoHhpxEuZ08McrB0J6c9M"])
        == "k=7, size 40.00 B, approx. 23 item(s), false positive probability 0.7%"
    )


def test_bloom_index_structure(local_engine_empty):
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    # Insert 26 rows with value_1 spanning a-z
    for i in range(26):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord("a") + i), i * 2))
    head = OUTPUT.commit(extra_indexes={"test": {"bloom": {"value_1": {"size": 16}}}})

    objects = head.get_table("test").objects
    object_meta = OUTPUT.objects.get_object_meta(objects)

    index = object_meta[objects[0]].object_index
    assert "bloom" in index

    # The bloom index used k=4, note that the formula for optimal k is
    # m(ln2)/n = 16 * 8 (filter size in bits) * 0.693 / 26 (distinct items)
    # = 3.41 which rounds up to 4

    # The actual base64 value here is a canary: if the index fingerprint for some reason changes
    # with the same data, this is highly suspicious.
    assert index["bloom"] == {"value_1": [4, "T79jcHurra5T6d8Hk+djZA=="]}

    # Test indexing two columns with different parameters
    # Delete one row so that the object doesn't get reused and the index gets written again.
    OUTPUT.run_sql("DELETE FROM test WHERE key = 26")
    head = OUTPUT.commit(
        snap_only=True,
        extra_indexes={
            "test": {"bloom": {"value_1": {"size": 16}, "value_2": {"probability": 0.01}}}
        },
    )

    objects = head.get_table("test").objects
    object_meta = OUTPUT.objects.get_object_meta(objects)

    index = object_meta[objects[0]].object_index
    assert "bloom" in index

    # For fixed probability, we have k = -log2(p) = 6.64 (up to 7)
    # and filter size = -n * ln(p) / ln(2)**2 = 249.211 bits rounded up to 30 bytes
    # which in base64 is represented with ceil(30/3*4) = 40 bytes.
    assert index["bloom"] == {
        "value_1": [4, "D79jcGurra5T6d8Hk+djZA=="],
        "value_2": [7, "uSP6qzHHDqVq/qHMlqrAoHhpxEuZ08McrB0J6c9M"],
    }
    assert len(index["bloom"]["value_2"][1]) == 40


def test_bloom_index_querying(local_engine_empty):
    # Same dataset as the previous, but this time test querying the bloom index
    # by calling it directly (not as part of an LQ).

    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    for i in range(26):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord("a") + i), i * 2))

    # Make 3 chunks (value_1 = a-i, j-r, s-z, value_2 = 0-16, 18-34, 36-50).
    # These technically will get caught by the range index but we're not touching
    # it here.
    head = OUTPUT.commit(
        chunk_size=9,
        extra_indexes={
            "test": {"bloom": {"value_1": {"probability": 0.01}, "value_2": {"probability": 0.01}}}
        },
    )

    objects = head.get_table("test").objects
    assert len(objects) == 3

    def test_filter(quals, result):
        assert filter_bloom_index(OUTPUT.engine, objects, quals) == result

    # Basic test: check we get only one object matching values from each chunk.
    test_filter([[("value_1", "=", "a")]], [objects[0]])
    test_filter([[("value_1", "=", "k")]], [objects[1]])
    test_filter([[("value_1", "=", "u")]], [objects[2]])

    test_filter([[("value_2", "=", "10")]], [objects[0]])
    test_filter([[("value_2", "=", "20")]], [objects[1]])
    test_filter([[("value_2", "=", "40")]], [objects[2]])

    # This is fun: 37 isn't in the original table (it's only even numbers)
    # but gets caught here as a false positive.
    test_filter([[("value_2", "=", "37")]], [objects[2]])
    # 39 doesn't though.
    test_filter([[("value_2", "=", "39")]], [])

    # Check ORs on same column -- returns two fragments
    test_filter([[("value_1", "=", "b"), ("value_1", "=", "l")]], [objects[0], objects[1]])

    # Check ORs on different columns
    test_filter([[("value_1", "=", "b"), ("value_2", "=", "38")]], [objects[0], objects[2]])

    test_filter([[("value_1", "=", "b"), ("value_2", "=", "39")]], [objects[0]])

    # Check AND
    test_filter([[("value_1", "=", "c")], [("value_2", "=", "40")]], [])

    test_filter([[("value_1", "=", "x")], [("value_2", "=", "40")]], [objects[2]])

    # Check AND with an unsupported operator -- gets discarded
    test_filter([[("value_1", "=", "x")], [("value_2", ">", "32")]], [objects[2]])

    # OR with unsupported: unsupported evaluates to True, so we have to fetch all objects.
    test_filter([[("value_1", "=", "not_here"), ("value_2", ">", "32")]], objects)

    # Test a composite operator : ((False OR True) AND (True OR (unsupported -> True)))
    # First OR-block is only true for objects[1], second block is true for all objects
    # but it gets intersected, so the result is objects [1]
    test_filter(
        [
            [("value_1", "=", "not here"), ("value_2", "=", "32")],
            [("value_1", "=", "k"), ("value_2", ">", "100")],
        ],
        [objects[1]],
    )


def test_bloom_index_deletions(local_engine_empty):
    # Check the bloom index fingerprint includes both the old and the new values of deleted/added cells.

    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    # Insert 26 rows with value_1 spanning a-z
    for i in range(26):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord("a") + i), i * 2))
    OUTPUT.commit()

    # Delete and update some rows
    OUTPUT.run_sql("DELETE FROM test WHERE key = 5")  # ('e', 8)
    OUTPUT.run_sql("DELETE FROM test WHERE key = 10")  # ('j', 18)
    OUTPUT.run_sql("DELETE FROM test WHERE key = 15")  # ('o', 28)
    OUTPUT.run_sql("UPDATE test SET value_1 = 'G' WHERE key = 7")  # (g -> G)
    OUTPUT.run_sql("UPDATE test SET value_2 = 23 WHERE key = 12")  # (22 -> 23)

    head = OUTPUT.commit(
        extra_indexes={
            "test": {"bloom": {"value_1": {"probability": 0.01}, "value_2": {"probability": 0.01}}}
        }
    )
    objects = head.get_table("test").objects

    # Sanity check: 2 objects (original data + new with 3 deletions and 2 upserts)
    assert len(objects) == 2
    assert (
        local_engine_empty.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta." + objects[1], return_shape=ResultShape.ONE_ONE
        )
        == 5
    )

    # Check old/new values for value_1: 3 old values before a deletion,
    # 1 old value before update, 1 updated value
    value_1_vals = ["e", "j", "o", "g", "G"]

    # value_2, same
    value_2_vals = [8, 18, 28, 22, 23]

    for val in value_1_vals:
        assert filter_bloom_index(OUTPUT.engine, objects, [[("value_1", "=", val)]]) == objects

    for val in value_2_vals:
        assert filter_bloom_index(OUTPUT.engine, objects, [[("value_2", "=", val)]]) == objects


def test_bloom_index_datetime(local_engine_empty):
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 TIMESTAMP)")
    for i in range(50):
        OUTPUT.run_sql(
            "INSERT INTO test VALUES (%s, %s)", (i + 1, dt(2015, 1, 1) + timedelta(days=i))
        )
    head = OUTPUT.commit(extra_indexes={"test": {"bloom": {"value_1": {"probability": 0.01}}}})
    objects = head.get_table("test").objects

    # Datetimes are supported in the bloom index if they're passed in as actual datetime
    # objects (which Multicorn does).

    # Spot check some dates that exist...
    for i in range(0, 50, 5):
        assert (
            filter_bloom_index(
                OUTPUT.engine, objects, [[("value_1", "=", dt(2015, 1, 1) + timedelta(days=i))]]
            )
            == objects
        )

    # ...and some that don't.
    assert (
        filter_bloom_index(
            OUTPUT.engine, objects, [[("value_1", "=", dt(2015, 1, 1) + timedelta(days=55))]]
        )
        == []
    )

    # They also work if passed in as ISO strings with space as a separator.
    assert (
        filter_bloom_index(OUTPUT.engine, objects, [[("value_1", "=", "2015-01-01 00:00:00")]])
        == objects
    )


def test_bloom_index_post_factum(local_engine_empty):
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 TIMESTAMP)")
    for i in range(50):
        OUTPUT.run_sql(
            "INSERT INTO test VALUES (%s, %s)", (i + 1, dt(2015, 1, 1) + timedelta(days=i))
        )
    head = OUTPUT.commit()

    # Reindex the table after committing it and check that object metadata gets overwritten.
    head.get_table("test").reindex(extra_indexes={"bloom": {"value_1": {"probability": 0.01}}})

    obj = head.get_table("test").objects[0]
    object_index = OUTPUT.objects.get_object_meta([obj])[obj].object_index
    assert "bloom" in object_index

    # Run a snippet from the previous test to check the bloom metadata is valid.
    objects = head.get_table("test").objects
    for i in range(0, 50, 5):
        assert (
            filter_bloom_index(
                OUTPUT.engine, objects, [[("value_1", "=", dt(2015, 1, 1) + timedelta(days=i))]]
            )
            == objects
        )


def test_bloom_reindex_changed_table(local_engine_empty):
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    for i in range(26):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord("a") + i), i * 2))
    OUTPUT.commit(chunk_size=13)
    OUTPUT.run_sql("DELETE FROM test WHERE key = 5")  # ('e', 8)

    head = OUTPUT.commit()
    objects = head.get_table("test").objects
    assert len(objects) == 3  # original 2 fragments and one overwrite

    index_spec = {"bloom": {"value_1": {"probability": 0.01}, "value_2": {"probability": 0.01}}}
    # Since the patch object deletes the old value, we don't immediately know what it was and so
    # can't reindex that object.
    with pytest.raises(ObjectIndexingError) as e:
        head.get_table("test").reindex(extra_indexes=index_spec)
    assert "1 object" in str(e.value)

    reindexed = head.get_table("test").reindex(
        extra_indexes=index_spec, raise_on_patch_objects=False
    )
    assert objects[0] in reindexed
    assert objects[1] in reindexed
    assert objects[2] not in reindexed


@pytest.mark.registry
def test_bloom_reindex_remote(local_engine_empty, unprivileged_pg_repo, clean_minio):
    _prepare_fully_remote_repo(local_engine_empty, unprivileged_pg_repo)

    # Do a reindex using our local engine to query the object and the remote engine
    # to write metadata to.
    repo = Repository.from_template(unprivileged_pg_repo, object_engine=local_engine_empty)
    fruits = repo.images["latest"].get_table("fruits")

    # The repo used for LQ tests has 2 objects that overwrite data, so we ignore those.
    reindexed = fruits.reindex(
        extra_indexes={"bloom": {"name": {"probability": 0.01}}}, raise_on_patch_objects=False
    )
    repo.commit_engines()

    assert len(reindexed) == 3
    assert set(repo.objects.get_downloaded_objects()) == set(reindexed)

    # Check the index was written to the remote metadata engine.
    assert (
        "bloom"
        in unprivileged_pg_repo.objects.get_object_meta(reindexed)[reindexed[0]].object_index
    )


@pytest.mark.registry
def test_bloom_reindex_push(local_engine_empty, unprivileged_pg_repo, clean_minio):
    _prepare_fully_remote_repo(local_engine_empty, unprivileged_pg_repo)
    pg_repo_local = clone(unprivileged_pg_repo, download_all=False)

    # Do a reindex on the local engine and push the dataset back out.
    fruits = pg_repo_local.images["latest"].get_table("fruits")

    # The repo used for LQ tests has 2 objects that overwrite data, so we ignore those.
    reindexed = fruits.reindex(
        extra_indexes={"bloom": {"name": {"probability": 0.01}}}, raise_on_patch_objects=False
    )
    pg_repo_local.commit_engines()

    # Push back out overwriting object metadata
    pg_repo_local.push(overwrite_objects=True, single_image="latest")

    # Check the index was written to the registry.
    assert (
        "bloom"
        in unprivileged_pg_repo.objects.get_object_meta(reindexed)[reindexed[0]].object_index
    )
