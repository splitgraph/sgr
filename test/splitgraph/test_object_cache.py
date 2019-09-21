import itertools
from datetime import datetime as dt
from unittest import mock

import pytest

from splitgraph import SPLITGRAPH_META_SCHEMA
from splitgraph.core import clone, select
from splitgraph.core.indexing.range import _quals_to_clause
from splitgraph.engine import ResultShape
from splitgraph.exceptions import ObjectCacheError
from test.splitgraph.conftest import (
    OUTPUT,
    _cleanup_minio,
    SMALL_OBJECT_SIZE,
    _assert_cache_occupancy,
    prepare_lq_repo,
)


def _get_refcount(object_manager, object_id):
    return object_manager.object_engine.run_sql(
        select("object_cache_status", "refcount", "object_id = %s"),
        (object_id,),
        return_shape=ResultShape.ONE_ONE,
    )


def _get_last_used(object_manager, object_id):
    return object_manager.object_engine.run_sql(
        select("object_cache_status", "last_used", "object_id = %s"),
        (object_id,),
        return_shape=ResultShape.ONE_ONE,
    )


def _setup_object_cache_test(pg_repo_remote, longer_chain=False):
    pg_repo_local = clone(pg_repo_remote)
    pg_repo_local.images["latest"].checkout()
    prepare_lq_repo(pg_repo_local, commit_after_every=False, include_pk=True)
    if longer_chain:
        pg_repo_local.run_sql("INSERT INTO FRUITS VALUES (4, 'kumquat')")
        pg_repo_local.commit()

    # Same setup as the LQ test in the beginning: we clone a repo from upstream, don't download anything, all
    # objects are on Minio.
    remote = pg_repo_local.push(handler="S3", handler_options={})
    pg_repo_local.delete()
    pg_repo_remote.objects.delete_objects(remote.objects.get_downloaded_objects())
    pg_repo_remote.commit_engines()
    pg_repo_local.objects.cleanup()
    pg_repo_local = clone(pg_repo_remote, download_all=False)

    # 6 objects in the tree (original fragment, new base fragment and a patch on top of that fragment
    # for both tables)
    assert len(pg_repo_local.objects.get_all_objects()) == 6 if not longer_chain else 7
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0
    assert len(remote.objects.get_all_objects()) == 6 if not longer_chain else 7
    assert len(remote.objects.get_downloaded_objects()) == 0

    # Nothing has yet been downloaded (cache entries only for externally downloaded things)
    assert (
        len(pg_repo_local.engine.run_sql("SELECT * FROM splitgraph_meta.object_cache_status")) == 0
    )

    return pg_repo_local


def test_object_cache_loading(local_engine_empty, pg_repo_remote, clean_minio):
    # Test object caching, downloading etc.
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v3 = pg_repo_local.images["latest"].get_table("fruits")
    fruits_v2 = pg_repo_local.images[pg_repo_local.images["latest"].parent_id].get_table("fruits")

    # Quick assertions on the objects and table sizes recorded by the engine, since we'll rely on them
    # to test eviction.
    assert len(fruits_v3.objects) == 2
    fruit_diff = fruits_v3.objects[1]
    assert len(fruits_v2.objects) == 1
    fruit_snap = fruits_v2.objects[0]

    # Resolve and download the old version: only one fragment should be downloaded.
    with object_manager.ensure_objects(fruits_v2) as required_objects:
        assert required_objects == [fruit_snap]

        assert _get_refcount(object_manager, fruit_snap) == 1
        # fruit_diff not in the cache at all
        assert _get_refcount(object_manager, fruit_diff) is None

    # Exit from the manager: refcounts are 0 but the object is still in the cache since there's enough space.
    assert _get_refcount(object_manager, fruit_snap) == 0
    assert _get_refcount(object_manager, fruit_diff) is None
    assert len(object_manager.get_downloaded_objects()) == 1

    # Resolve and download the new version: will download the patch.
    with object_manager.ensure_objects(fruits_v3) as required_objects:
        assert required_objects == [fruit_snap, fruit_diff]

        assert _get_refcount(object_manager, fruit_snap) == 1
        assert _get_refcount(object_manager, fruit_diff) == 1
        assert len(object_manager.get_downloaded_objects()) == 2

    assert _get_refcount(object_manager, fruit_snap) == 0
    assert _get_refcount(object_manager, fruit_diff) == 0
    assert len(object_manager.get_downloaded_objects()) == 2


def test_object_cache_non_existing_objects(local_engine_empty, pg_repo_remote, clean_minio):
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)
    object_manager = pg_repo_local.objects

    fruits_v3 = pg_repo_local.images["latest"].get_table("fruits")
    # Unlink an object so that the manager tries to find it on the remote, make sure we get a friendly
    # exception rather than a psycopg ProgrammingError
    pg_repo_local.run_sql(
        "DELETE FROM splitgraph_meta.object_locations WHERE object_id = %s", (fruits_v3.objects[0],)
    )
    # Make sure to commit here -- otherwise the error rolls everything back.
    pg_repo_local.commit_engines()
    with pytest.raises(ObjectCacheError) as e:
        with object_manager.ensure_objects(fruits_v3):
            pass
    assert "Missing objects: " in str(e.value)
    assert fruits_v3.objects[0] in str(e.value)

    # Make sure the claims have been released on failure (not inserted into the table at all)
    assert _get_refcount(object_manager, fruits_v3.objects[0]) is None
    # Now, also delete objects from Minio and make sure it's detected at download time
    object_manager.run_eviction(keep_objects=[], required_space=None)

    assert len(object_manager.get_downloaded_objects()) == 0
    assert object_manager.get_cache_occupancy() == 0
    _cleanup_minio()

    with pytest.raises(ObjectCacheError) as e:
        with object_manager.ensure_objects(fruits_v3):
            pass
    assert "Missing objects: " in str(e.value)
    assert fruits_v3.objects[0] in str(e.value)


def test_object_cache_eviction(local_engine_empty, pg_repo_remote, clean_minio):
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v3 = pg_repo_local.images["latest"].get_table("fruits")
    fruits_v2 = pg_repo_local.images[pg_repo_local.images["latest"].parent_id].get_table("fruits")
    vegetables_v2 = pg_repo_local.images[pg_repo_local.images["latest"].parent_id].get_table(
        "vegetables"
    )
    vegetables_snap = vegetables_v2.objects[0]
    fruit_diff = fruits_v3.objects[1]
    fruit_snap = fruits_v2.objects[0]

    # Load the fruits objects into the cache
    with object_manager.ensure_objects(fruits_v3):
        assert len(object_manager.get_downloaded_objects()) == 2
        _assert_cache_occupancy(object_manager, 2)

    # Pretend that the cache has no space and try getting a different table
    # Free space is now 150 bytes (not enough for another object), so
    # we need to run eviction.
    object_manager.cache_size = object_manager.get_cache_occupancy() + 150
    with object_manager.ensure_objects(vegetables_v2) as required_objects:
        current_objects = object_manager.get_downloaded_objects()
        assert len(current_objects) == 2  # vegetables_v2 downloaded
        _assert_cache_occupancy(object_manager, 2)

        # One of fruits' fragments remains: since they were both used at the same time and have
        # the same size, it's arbitrary which one gets evicted.

        # This is the first time in my life I'm using xor for boolean logic rather than bit twiddling.
        assert (fruit_diff in current_objects) ^ (fruit_snap in current_objects)
        assert vegetables_snap in current_objects
        assert required_objects == [vegetables_snap]
        assert object_manager.object_engine.table_exists(SPLITGRAPH_META_SCHEMA, vegetables_snap)

    assert _get_refcount(object_manager, vegetables_snap) == 0

    # Delete all objects and re-load the fruits table
    object_manager.run_eviction([], None)
    assert len(object_manager.get_downloaded_objects()) == 0
    _assert_cache_occupancy(object_manager, 0)
    with object_manager.ensure_objects(fruits_v3):
        pass

    # Now, let's try squeezing the cache even more so that there's only space for one object.
    object_manager.cache_size = object_manager.get_object_meta([vegetables_snap])[
        vegetables_snap
    ].size
    with object_manager.ensure_objects(vegetables_v2):
        assert not object_manager.object_engine.table_exists(SPLITGRAPH_META_SCHEMA, fruit_snap)
        assert object_manager.object_engine.table_exists(SPLITGRAPH_META_SCHEMA, vegetables_snap)
        assert not object_manager.object_engine.table_exists(SPLITGRAPH_META_SCHEMA, fruit_diff)
        assert len(object_manager.get_downloaded_objects()) == 1

    # Loading the next version: not enough space for 2 objects.
    object_manager.run_eviction([], None)
    with pytest.raises(ObjectCacheError) as e:
        with object_manager.ensure_objects(fruits_v3):
            pass
    assert "Not enough space in the cache" in str(e.value)


def test_object_cache_eviction_fraction(local_engine_empty, pg_repo_remote, clean_minio):
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v3 = pg_repo_local.images["latest"].get_table("fruits")
    vegetables_v2 = pg_repo_local.images[pg_repo_local.images["latest"].parent_id].get_table(
        "vegetables"
    )

    # Load the fruits objects into the cache
    with object_manager.ensure_objects(fruits_v3):
        _assert_cache_occupancy(object_manager, 2)

    # Set the eviction fraction to 0.5 (clean out > half the cache in any case) and try downloading just one object.
    object_manager.cache_size = SMALL_OBJECT_SIZE * 2 + 300
    object_manager.eviction_min_fraction = 0.75

    with object_manager.ensure_objects(vegetables_v2):
        # Only one object should be in the cache since we evicted cache_size * 0.75 = 2 objects
        _assert_cache_occupancy(object_manager, 1)


def test_object_cache_locally_created_dont_get_evicted(
    local_engine_empty, pg_repo_remote, clean_minio
):
    # Test that the objects which were created locally are exempt from cache eviction/stats.
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v2 = pg_repo_local.images[pg_repo_local.images["latest"].parent_id].get_table("fruits")
    head = pg_repo_local.images["latest"]
    head.checkout()
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (5, 'banana')")
    new_head = pg_repo_local.commit()
    fruits_v4 = new_head.get_table("fruits")
    assert len(fruits_v4.objects) == 3

    head.checkout()
    new_head.checkout()

    # Despite that we have objects on the engine, they don't count towards the full cache occupancy.
    # 5 objects on the engine, 1 of them was created locally.
    assert len(object_manager.get_downloaded_objects()) == 5
    _assert_cache_occupancy(object_manager, 4)

    cache_occupancy = object_manager.get_cache_occupancy()
    assert object_manager.get_total_object_size() > cache_occupancy

    # Evict all objects -- check to see the one we created still exists.
    object_manager.run_eviction(keep_objects=[], required_space=None)
    downloaded = object_manager.get_downloaded_objects()
    assert downloaded == [fruits_v4.objects[-1]]


def test_object_cache_eviction_orphaned(local_engine_empty, pg_repo_remote, clean_minio):
    # Test that objects that become orphaned (no entry in the objects table) get evicted first.
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v3 = pg_repo_local.images["latest"].get_table("fruits")

    with object_manager.ensure_objects(fruits_v3):
        _assert_cache_occupancy(object_manager, 2)

    # Delete the image and objects that it's made out of (outside of the normal ObjectManager.cleanup())
    pg_repo_local.images.delete([pg_repo_local.images["latest"].image_hash])

    pg_repo_local.object_engine.run_sql(
        "DELETE FROM splitgraph_meta.object_locations WHERE object_id IN ("
        + ",".join(itertools.repeat("%s", len(fruits_v3.objects)))
        + ")",
        fruits_v3.objects,
    )
    pg_repo_local.object_engine.run_sql(
        "DELETE FROM splitgraph_meta.objects WHERE object_id IN ("
        + ",".join(itertools.repeat("%s", len(fruits_v3.objects)))
        + ")",
        fruits_v3.objects,
    )

    # Make sure the eviction works deleting orphaned objects as well.
    object_manager.run_eviction(keep_objects=[], required_space=0)

    _assert_cache_occupancy(object_manager, 0)


def test_object_cache_nested(local_engine_empty, pg_repo_remote, clean_minio):
    # Test that we can have multiple groups of objects loaded at the same time.
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v3 = pg_repo_local.images["latest"].get_table("fruits")
    fruits_v2 = pg_repo_local.images[pg_repo_local.images["latest"].parent_id].get_table("fruits")
    vegetables_v2 = pg_repo_local.images[pg_repo_local.images["latest"].parent_id].get_table(
        "vegetables"
    )
    vegetables_snap = vegetables_v2.objects[0]
    fruit_diff = fruits_v3.objects[0]
    fruit_snap = fruits_v2.objects[0]

    with object_manager.ensure_objects(fruits_v3):
        assert len(object_manager.get_downloaded_objects()) == 2
        _assert_cache_occupancy(object_manager, 2)
        with object_manager.ensure_objects(vegetables_v2):
            _assert_cache_occupancy(object_manager, 3)
            assert _get_refcount(object_manager, fruit_diff) == 1
            assert _get_refcount(object_manager, fruit_snap) == 1
            assert _get_refcount(object_manager, vegetables_snap) == 1
            assert len(object_manager.get_downloaded_objects()) == 3

    # Now evict everything from the cache.
    object_manager.run_eviction(keep_objects=[], required_space=None)
    assert len(object_manager.get_downloaded_objects()) == 0

    object_manager.cache_size = SMALL_OBJECT_SIZE * 2 + 300
    with object_manager.ensure_objects(fruits_v3):
        # Now the fruits objects are being used and so we can't reclaim that space and have to raise an error.
        with pytest.raises(ObjectCacheError) as e:
            with object_manager.ensure_objects(vegetables_v2):
                pass
        assert "Not enough space will be reclaimed" in str(e.value)


def test_object_cache_eviction_priority(local_engine_empty, pg_repo_remote, clean_minio):
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v2 = pg_repo_local.images[pg_repo_local.images["latest"].parent_id].get_table("fruits")
    fruits_v3 = pg_repo_local.images["latest"].get_table("fruits")
    fruit_snap = fruits_v2.objects[0]
    fruit_diff = fruits_v3.objects[1]
    vegetables_v2 = pg_repo_local.images[pg_repo_local.images["latest"].parent_id].get_table(
        "vegetables"
    )
    vegetables_v3 = pg_repo_local.images["latest"].get_table("vegetables")
    vegetables_snap = vegetables_v2.objects[0]
    vegetables_diff = vegetables_v3.objects[1]

    # Setup: the cache has enough space for 3 objects
    # (each object is within 100 bytes of SMALL_OBJECT_SIZE)
    object_manager.cache_size = SMALL_OBJECT_SIZE * 3 + 300

    # Can't use time-freezing methods (e.g. freezegun) here since we interact with the Minio server which timestamps
    # everything with the actual current time and kindly tells us to go away when we show up from the past and ask
    # to upload or download objects.

    with object_manager.ensure_objects(fruits_v3):
        lu_1 = _get_last_used(object_manager, fruit_snap)
        assert lu_1 == _get_last_used(object_manager, fruit_diff)

        current_objects = object_manager.get_downloaded_objects()
        assert fruit_snap in current_objects
        assert fruit_diff in current_objects
        assert vegetables_snap not in current_objects
        assert vegetables_diff not in current_objects

    # Slightly later: fetch just the old version (the original single fragment)
    with object_manager.ensure_objects(fruits_v2):
        # Make sure the timestamp for the fragment has been bumped.
        lu_2 = _get_last_used(object_manager, fruit_snap)
        assert _get_last_used(object_manager, fruit_diff) == lu_1
        assert lu_2 > lu_1

        # None of the fruits have been evicted yet.
        current_objects = object_manager.get_downloaded_objects()
        assert fruit_snap in current_objects
        assert fruit_diff in current_objects
        assert vegetables_snap not in current_objects
        assert vegetables_diff not in current_objects

    # Now, fetch the new vegetables version (2 objects). Since we already have
    # 2 objects in the cache and the limit is 3, one must be evicted.
    with object_manager.ensure_objects(vegetables_v3):
        assert _get_last_used(object_manager, fruit_snap) == lu_2
        assert _get_last_used(object_manager, fruit_diff) is None
        lu_3 = _get_last_used(object_manager, vegetables_snap)
        assert lu_3 > lu_2
        assert _get_last_used(object_manager, vegetables_diff) == lu_3

        # The first fruit fragment was used more recently than the patch and they both have the same size,
        # so the original fragment will stay.
        current_objects = object_manager.get_downloaded_objects()
        assert fruit_snap in current_objects
        assert fruit_diff not in current_objects
        assert vegetables_snap in current_objects
        assert vegetables_diff in current_objects

        with pytest.raises(ObjectCacheError) as e:
            # Try to load all 4 objects in the same time: should fail.
            with object_manager.ensure_objects(fruits_v3):
                pass
        assert "Not enough space will be reclaimed" in str(e.value)


def test_object_cache_deferred(local_engine_empty, pg_repo_remote, clean_minio):
    # Test object manager with deferred releases (we get given a callback to
    # release objects rather than it being done when we leave the context manager)
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v3 = pg_repo_local.images["latest"].get_table("fruits")
    vegetables_v2 = pg_repo_local.images[pg_repo_local.images["latest"].parent_id].get_table(
        "vegetables"
    )

    with object_manager.ensure_objects(fruits_v3, defer_release=True) as (
        objects,
        fruits_v3_callback,
    ):
        assert len(object_manager.get_downloaded_objects()) == 2
        _assert_cache_occupancy(object_manager, 2)

    assert (
        pg_repo_local.engine.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.object_cache_status WHERE refcount > 0",
            return_shape=ResultShape.ONE_ONE,
        )
        == 2
    )

    # Even though we left the context manager, we still haven't called the callback.
    for object_id in objects:
        assert _get_refcount(object_manager, object_id) == 1

    # Pretend that the cache has no space and try getting a different table
    # Free space is now 150 bytes (not enough for another object) but we haven't yet
    # released the original objects, so the download fails.
    object_manager.cache_size = object_manager.get_cache_occupancy() + 150
    with pytest.raises(ObjectCacheError) as e:
        with object_manager.ensure_objects(vegetables_v2):
            pass
        assert "Not enough space will be reclaimed" in str(e.value)

    # Now call the callback and check the objects are released
    fruits_v3_callback()
    for object_id in objects:
        assert _get_refcount(object_manager, object_id) == 0

    # Call it again to make sure we don't clean up twice
    fruits_v3_callback()
    for object_id in objects:
        assert _get_refcount(object_manager, object_id) == 0

    # Try loading the original table again, shouldn't fail
    with object_manager.ensure_objects(vegetables_v2):
        pass


def test_object_cache_make_external(pg_repo_local, clean_minio):
    # Test marking objects as external and uploading them to S3
    all_objects = list(sorted(pg_repo_local.objects.get_all_objects()))

    # Objects are local and don't exist externally: running eviction does nothing and local objects
    # don't count towards cache occupancy.
    pg_repo_local.objects.run_eviction(keep_objects=[], required_space=None)
    assert list(sorted(pg_repo_local.objects.get_downloaded_objects())) == all_objects
    assert pg_repo_local.objects.get_cache_occupancy() == 0

    # Mark objects as external and upload them
    pg_repo_local.objects.make_objects_external(all_objects, handler="S3", handler_params={})
    _assert_cache_occupancy(pg_repo_local.objects, 2)

    pg_repo_local.objects.run_eviction(keep_objects=[], required_space=None)
    _assert_cache_occupancy(pg_repo_local.objects, 0)
    assert not pg_repo_local.objects.get_downloaded_objects()

    # Download the objects again
    with pg_repo_local.objects.ensure_objects(
        pg_repo_local.images["latest"].get_table("fruits")
    ) as obs1:
        with pg_repo_local.objects.ensure_objects(
            pg_repo_local.images["latest"].get_table("vegetables")
        ) as obs2:
            _assert_cache_occupancy(pg_repo_local.objects, 2)
            assert list(sorted(pg_repo_local.objects.get_downloaded_objects())) == all_objects


def test_object_manager_index_clause_generation(pg_repo_local):
    column_types = {"a": "int", "b": "int"}

    def _assert_ic_result(quals, expected_clause, expected_args):
        qual, args = _quals_to_clause(quals, column_types)
        assert qual.as_string(pg_repo_local.engine.connection) == expected_clause
        assert args == expected_args

    # Test basics: single clause, comparison
    _assert_ic_result(
        [[("a", ">", 5)]],
        "((NOT (index -> 'range') ? %s " "OR (index #>> '{range,\"a\",1}')::int  > %s))",
        ("a", 5),
    )

    # Two clauses in an OR-block, (in)equality
    _assert_ic_result(
        [[("a", "=", 5), ("b", "<>", 3)]],
        "((NOT (index -> 'range') ? %s "
        "OR %s BETWEEN (index #>> '{range,\"a\",0}')::int "
        "AND (index #>> '{range,\"a\",1}')::int) OR (TRUE))",
        ("a", 5),
    )

    # Two clauses in an AND-block, check unknown operators
    _assert_ic_result(
        [[("a", "<", 3)], [("b", "~", 3)]],
        "((NOT (index -> 'range') ? %s "
        "OR (index #>> '{range,\"a\",0}')::int < %s)) AND ((TRUE))",
        ("a", 3),
    )


def _prepare_object_filtering_dataset(include_bloom=False):
    OUTPUT.init()
    OUTPUT.run_sql(
        """CREATE TABLE test
            (col1 int primary key,
             col2 int,
             col3 varchar,
             col4 timestamp,
             col5 json)"""
    )

    bloom_params = (
        {
            "test": {
                "bloom": {col: {"probability": 0.001} for col in ["col2", "col3", "col4", "col5"]}
            }
        }
        if include_bloom
        else None
    )

    # First object is kind of normal: incrementing PK, a random col2, some text, some timestamps
    OUTPUT.run_sql("INSERT INTO test VALUES (1, 5, 'aaaa', '2016-01-01 00:00:00', '{\"a\": 5}')")
    OUTPUT.run_sql("INSERT INTO test VALUES (5, 3, 'bbbb', '2016-01-02 00:00:00', '{\"a\": 10}')")
    OUTPUT.commit(extra_indexes=bloom_params)
    # Grab the newly created object (at the end of the table's objects list)
    obj_1 = OUTPUT.head.get_table("test").objects[-1]
    # Sanity check on index for reference + easier debugging
    assert OUTPUT.objects.get_object_meta([obj_1])[obj_1].object_index["range"] == {
        "col1": [1, 5],
        "col2": [3, 5],
        "col3": ["aaaa", "bbbb"],
        "col4": ["2016-01-01 00:00:00", "2016-01-02 00:00:00"],
    }
    if include_bloom:
        assert OUTPUT.objects.get_object_meta([obj_1])[obj_1].object_index["bloom"] == {
            "col2": [12, mock.ANY],
            "col3": [12, mock.ANY],
            "col4": [12, mock.ANY],
            "col5": [12, mock.ANY],
        }

    # Second object: PK increments, ranges for col2 and col3 overlap, col4 has the same timestamps everywhere
    OUTPUT.run_sql("INSERT INTO test VALUES (6, 1, 'abbb', '2015-12-30 00:00:00', '{\"a\": 5}')")
    OUTPUT.run_sql("INSERT INTO test VALUES (10, 4, 'cccc', '2015-12-30 00:00:00', '{\"a\": 10}')")
    OUTPUT.commit(extra_indexes=bloom_params)
    obj_2 = OUTPUT.head.get_table("test").objects[-1]
    assert OUTPUT.objects.get_object_meta([obj_2])[obj_2].object_index["range"] == {
        "col1": [6, 10],
        "col2": [1, 4],
        "col3": ["abbb", "cccc"],
        "col4": ["2015-12-30 00:00:00", "2015-12-30 00:00:00"],
    }
    if include_bloom:
        assert OUTPUT.objects.get_object_meta([obj_2])[obj_2].object_index["bloom"] == {
            "col2": [12, mock.ANY],
            "col3": [12, mock.ANY],
            "col4": [12, mock.ANY],
            "col5": [12, mock.ANY],
        }

    # Third object: just a single row
    OUTPUT.run_sql("INSERT INTO test VALUES (11, 10, 'dddd', '2016-01-05 00:00:00', '{\"a\": 5}')")
    OUTPUT.commit(extra_indexes=bloom_params)
    obj_3 = OUTPUT.head.get_table("test").objects[-1]
    assert OUTPUT.objects.get_object_meta([obj_3])[obj_3].object_index["range"] == {
        "col1": [11, 11],
        "col2": [10, 10],
        "col3": ["dddd", "dddd"],
        "col4": ["2016-01-05 00:00:00", "2016-01-05 00:00:00"],
    }
    if include_bloom:
        assert OUTPUT.objects.get_object_meta([obj_3])[obj_3].object_index["bloom"] == {
            "col2": [12, mock.ANY],
            "col3": [12, mock.ANY],
            "col4": [12, mock.ANY],
            "col5": [12, mock.ANY],
        }

    # Fourth object: PK increments, ranges for col2/col3 don't overlap, col4 spans obj_1's range, we have a NULL.
    OUTPUT.run_sql("INSERT INTO test VALUES (12, 11, 'eeee', '2015-12-31 00:00:00', '{\"a\": 5}')")
    OUTPUT.run_sql("INSERT INTO test VALUES (14, 13, 'ezzz', NULL, '{\"a\": 5}')")
    OUTPUT.run_sql("INSERT INTO test VALUES (16, 15, 'ffff', '2016-01-04 00:00:00', '{\"a\": 10}')")
    OUTPUT.commit(extra_indexes=bloom_params)
    obj_4 = OUTPUT.head.get_table("test").objects[-1]
    assert OUTPUT.objects.get_object_meta([obj_4])[obj_4].object_index["range"] == {
        "col1": [12, 16],
        "col2": [11, 15],
        "col3": ["eeee", "ffff"],
        "col4": ["2015-12-31 00:00:00", "2016-01-04 00:00:00"],
    }
    if include_bloom:
        assert OUTPUT.objects.get_object_meta([obj_4])[obj_4].object_index["bloom"] == {
            "col2": [12, mock.ANY],
            "col3": [12, mock.ANY],
            "col4": [12, mock.ANY],
            "col5": [12, mock.ANY],
        }

    return [obj_1, obj_2, obj_3, obj_4]


@pytest.mark.parametrize("include_bloom", [True, False])
def test_object_manager_object_filtering(local_engine_empty, include_bloom):
    objects = _prepare_object_filtering_dataset(include_bloom=include_bloom)
    obj_1, obj_2, obj_3, obj_4 = objects
    om = OUTPUT.objects
    table = OUTPUT.head.get_table("test")

    def _assert_filter_result(quals, expected):
        assert set(om.filter_fragments(objects, table, quals)) == set(expected)

    # Test single quals on PK
    _assert_filter_result([[("col1", "=", 3)]], [obj_1])
    # Even though obj_1 spans 3 (and might include 3 as a value), it might have values that aren't 3.
    _assert_filter_result([[("col1", "<>", 3)]], [obj_1, obj_2, obj_3, obj_4])
    _assert_filter_result([[("col1", ">", 5)]], [obj_2, obj_3, obj_4])
    _assert_filter_result([[("col1", ">=", 5)]], [obj_1, obj_2, obj_3, obj_4])
    _assert_filter_result([[("col1", "<", 11)]], [obj_1, obj_2])
    _assert_filter_result([[("col1", "<=", 11)]], [obj_1, obj_2, obj_3])

    # Test NULL: we don't filter anything down since they aren't included in the index and
    # we don't explicitly track down where they are.
    _assert_filter_result([[("col4", "IS", "NULL")]], [obj_1, obj_2, obj_3, obj_4])

    # Test datetime quals
    _assert_filter_result([[("col4", ">", "2015-12-31 00:00:00")]], [obj_1, obj_3, obj_4])
    _assert_filter_result([[("col4", ">", dt(2015, 12, 31))]], [obj_1, obj_3, obj_4])
    _assert_filter_result([[("col4", "<=", "2016-01-01 00:00:00")]], [obj_1, obj_2, obj_4])

    # Test text column
    # Unknown operator (that can't be pruned with the index) returns everything
    _assert_filter_result([[("col3", "~~", "eee%")]], [obj_1, obj_2, obj_3, obj_4])
    _assert_filter_result([[("col3", "=", "aaaa")]], [obj_1])

    if include_bloom:
        # With bloom filtering, we can see that "accc" isn't in any of the text fields.
        _assert_filter_result([[("col3", "=", "accc")]], [])
    else:
        _assert_filter_result([[("col3", "=", "accc")]], [obj_1, obj_2])

    # Test combining quals

    # (can't be pruned) OR (can be pruned) returns all since we don't know what will match the first clause
    _assert_filter_result(
        [[("col3", "~~", "eee%"), ("col1", "=", 3)]], [obj_1, obj_2, obj_3, obj_4]
    )

    # (can't be pruned) AND (can be pruned) returns the same result as the second clause since if something definitely
    # doesn't match the second clause, it won't match the AND.
    _assert_filter_result([[("col3", "~~", "eee%")], [("col1", "=", 3)]], [obj_1])

    # (col1 > 5 AND col1 < 2)
    _assert_filter_result([[("col1", ">", 5)], [("col1", "<", 2)]], [])

    # (col1 > 5 OR col1 < 2)
    _assert_filter_result([[("col1", ">", 5), ("col1", "<", 2)]], [obj_1, obj_2, obj_3, obj_4])

    # (col1 > 10 (selects obj_3 and 4) OR col4 == 2016-01-02 00:00:00 (selects obj_1 and 4))
    # AND col2 == 2 (selects obj_2)
    # the first clause selects objs 1, 3, 4; second clause selects 2; intersecting them results in []
    _assert_filter_result([[("col1", ">", 10)]], [obj_3, obj_4])
    _assert_filter_result([[("col2", "=", 1)]], [obj_2])

    # 2016-01-02 00:00:00 only exists in obj_1 (even though obj_4 also spans that date)
    # which the bloom filter can catch
    if include_bloom:
        _assert_filter_result([[("col4", "=", "2016-01-02 00:00:00")]], [obj_1])
    else:
        # Without the bloom filter, both obj_1 and obj_4 are returned.
        _assert_filter_result([[("col4", "=", "2016-01-02 00:00:00")]], [obj_1, obj_4])

    _assert_filter_result(
        [[("col1", ">", 10), ("col4", "=", "2016-01-02 00:00:00")]], [obj_1, obj_3, obj_4]
    )
    _assert_filter_result(
        [[("col1", ">", 10), ("col4", "=", "2016-01-02 00:00:00")], [("col2", "=", 1)]], []
    )

    # Same as previous but the second clause is col3 = 'dddd' (selects only obj_3),
    # hence the final result is just obj_3.
    _assert_filter_result(
        [[("col1", ">", 10), ("col4", "=", "2016-01-02 00:00:00")], [("col3", "=", "dddd")]],
        [obj_3],
    )


def test_object_manager_object_filtering_end_to_end(local_engine_empty):
    objects = _prepare_object_filtering_dataset()
    obj_1, obj_2, obj_3, obj_4 = objects
    om = OUTPUT.objects
    table = OUTPUT.head.get_table("test")

    with om.ensure_objects(table) as required_objects:
        assert set(required_objects) == set(objects)

    # Check the object manager filters the quals correctly
    for _ in range(5):
        with om.ensure_objects(
            table, quals=[[("col1", ">", 10), ("col4", "=", "2016-01-01 12:00:00")]]
        ) as required_objects:
            assert set(required_objects) == {obj_1, obj_3, obj_4}


def test_sync_object_mounts(pg_repo_local, clean_minio):
    # Test the engine discovering objects that were dropped into
    # its local storage and automatically mounting them.

    object_id = pg_repo_local.objects.get_all_objects()[0]
    assert object_id in pg_repo_local.engine.get_all_tables(SPLITGRAPH_META_SCHEMA)
    assert object_id in pg_repo_local.objects.get_downloaded_objects()
    pg_repo_local.objects.make_objects_external([object_id], handler="S3", handler_params={})

    # Simulate the object being in /var/lib/splitgraph/objects without actually
    # being mounted by first unmounting it (dropping the foreign table) -- pretend somebody
    # else put it there.
    pg_repo_local.engine.unmount_objects([object_id])

    # Unmounting the foreign table doesn't delete the object from the storage (/var/lib/splitgraph/objects)
    assert object_id not in pg_repo_local.engine.get_all_tables(SPLITGRAPH_META_SCHEMA)
    assert object_id in pg_repo_local.objects.get_downloaded_objects()

    # Check that sync_object_mounts remounts the object.
    pg_repo_local.engine.sync_object_mounts()
    assert object_id in pg_repo_local.objects.get_downloaded_objects()
    assert object_id in pg_repo_local.engine.get_all_tables(SPLITGRAPH_META_SCHEMA)
