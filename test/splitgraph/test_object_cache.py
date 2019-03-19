from datetime import datetime as dt, timedelta

import pytest
from splitgraph.core import clone, select, ResultShape, SPLITGRAPH_META_SCHEMA
from splitgraph.core.fragment_manager import _quals_to_clause
from splitgraph.exceptions import SplitGraphException
from test.splitgraph.commands.test_layered_querying import prepare_lq_repo
from test.splitgraph.conftest import OUTPUT, _cleanup_minio


def _get_refcount(object_manager, object_id):
    return object_manager.object_engine.run_sql(select("object_cache_status", "refcount", "object_id = %s"),
                                                (object_id,), return_shape=ResultShape.ONE_ONE)


def _get_last_used(object_manager, object_id):
    return object_manager.object_engine.run_sql(select("object_cache_status", "last_used", "object_id = %s"),
                                                (object_id,), return_shape=ResultShape.ONE_ONE)


def _setup_object_cache_test(pg_repo_remote, longer_chain=False):
    pg_repo_local = clone(pg_repo_remote)
    pg_repo_local.images['latest'].checkout()
    prepare_lq_repo(pg_repo_local, commit_after_every=False, include_pk=True)
    if longer_chain:
        pg_repo_local.run_sql("INSERT INTO FRUITS VALUES (4, 'kumquat')")
        pg_repo_local.commit()

    # Same setup as the LQ test in the beginning: we clone a repo from upstream, don't download anything, all
    # objects are on Minio.
    remote = pg_repo_local.push(handler='S3', handler_options={})
    pg_repo_local.delete()
    pg_repo_remote.objects.delete_objects(remote.objects.get_downloaded_objects())
    pg_repo_remote.engine.commit()
    pg_repo_local.objects.cleanup()
    pg_repo_local = clone(pg_repo_remote, download_all=False)

    # 6 objects in the tree (SNAP -> SNAP -> DIFF for both tables)
    assert len(pg_repo_local.objects.get_existing_objects()) == 6 if not longer_chain else 7
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0
    assert len(remote.objects.get_existing_objects()) == 6 if not longer_chain else 7
    assert len(remote.objects.get_downloaded_objects()) == 0

    # Nothing has yet been downloaded (cache entries only for externally downloaded things)
    assert len(pg_repo_local.engine.run_sql("SELECT * FROM splitgraph_meta.object_cache_status")) == 0

    return pg_repo_local


def test_object_cache_loading(local_engine_empty, pg_repo_remote):
    # Test object caching, downloading etc.
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    object_tree = object_manager.get_full_object_tree()
    fruits_v3 = pg_repo_local.images['latest'].get_table('fruits')
    fruits_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('fruits')

    # Quick assertions on the objects and table sizes recorded by the engine, since we'll rely on them
    # to test eviction.
    assert len(fruits_v3.objects) == 1
    fruit_diff = fruits_v3.objects[0]
    assert len(fruits_v2.objects) == 1
    fruit_snap = fruits_v2.objects[0]
    # Reported by Postgres itself and stored by the engine in object_tree. Might really backfire on us on different
    # Postgres versions.
    assert object_tree[fruit_diff] == (fruit_snap, 'DIFF', 8192)
    assert object_tree[fruit_snap] == (None, 'SNAP', 8192)

    # Resolve and download the old version: only one SNAP should be downloaded.
    with object_manager.ensure_objects(fruits_v2) as required_objects:
        assert required_objects == [fruit_snap]

        assert _get_refcount(object_manager, fruit_snap) == 1
        # fruit_diff not in the cache at all
        assert _get_refcount(object_manager, fruit_diff) is None

    # Exit from the manager: refcounts are 0 but the object is still in the cache since there's enough space.
    assert _get_refcount(object_manager, fruit_snap) == 0
    assert _get_refcount(object_manager, fruit_diff) is None
    assert len(object_manager.get_downloaded_objects()) == 1

    # Resolve and download the new version: will download the DIFF.
    with object_manager.ensure_objects(fruits_v3) as required_objects:
        assert required_objects == [fruit_snap, fruit_diff]

        assert _get_refcount(object_manager, fruit_snap) == 1
        assert _get_refcount(object_manager, fruit_diff) == 1
        assert len(object_manager.get_downloaded_objects()) == 2

    assert _get_refcount(object_manager, fruit_snap) == 0
    assert _get_refcount(object_manager, fruit_diff) == 0
    assert len(object_manager.get_downloaded_objects()) == 2


def test_object_cache_non_existing_objects(local_engine_empty, pg_repo_remote):
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)
    object_manager = pg_repo_local.objects

    fruits_v3 = pg_repo_local.images['latest'].get_table('fruits')
    # Unlink an object so that the manager tries to find it on the remote, make sure we get a friendly
    # exception rather than a psycopg ProgrammingError
    pg_repo_local.run_sql("DELETE FROM splitgraph_meta.object_locations WHERE object_id = %s", (fruits_v3.objects[-1],))
    # Make sure to commit here -- otherwise the error rolls everything back.
    pg_repo_local.engine.commit()
    with pytest.raises(SplitGraphException) as e:
        with object_manager.ensure_objects(fruits_v3):
            pass
    assert "Missing objects: " in str(e)
    assert fruits_v3.objects[-1] in str(e)

    # Now, also delete objects from Minio and make sure it's detected at download time
    object_manager.run_eviction(object_manager.get_full_object_tree(), keep_objects=[], required_space=None)
    assert len(object_manager.get_downloaded_objects()) == 0
    _cleanup_minio()

    with pytest.raises(SplitGraphException) as e:
        with object_manager.ensure_objects(fruits_v3):
            pass
    assert "Missing objects: " in str(e)
    assert fruits_v3.objects[0] in str(e)


def test_object_cache_eviction(local_engine_empty, pg_repo_remote):
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v3 = pg_repo_local.images['latest'].get_table('fruits')
    fruits_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('fruits')
    vegetables_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('vegetables')
    vegetables_snap = vegetables_v2.objects[0]
    fruit_diff = fruits_v3.objects[0]
    fruit_snap = fruits_v2.objects[0]

    # Check another test object has the same size
    assert object_manager.get_full_object_tree()[vegetables_snap][2] == 8192

    # Load the fruits objects into the cache
    object_manager.ensure_objects(fruits_v3)

    # Pretend that the cache has no space and try getting a different table
    # Free space is now 0, so we need to run eviction.
    object_manager.cache_size = 8192 * 2
    with object_manager.ensure_objects(vegetables_v2) as required_objects:
        current_objects = object_manager.get_downloaded_objects()
        assert len(current_objects) == 1
        assert fruit_snap not in current_objects
        assert fruit_diff not in current_objects
        assert vegetables_snap in current_objects
        assert required_objects == [vegetables_snap]
        assert object_manager.object_engine.table_exists(SPLITGRAPH_META_SCHEMA, vegetables_snap)

    assert _get_refcount(object_manager, vegetables_snap) == 0

    # Now, let's try squeezing the cache even more so that there's only space for one object.
    object_manager.cache_size = 8192
    with object_manager.ensure_objects(fruits_v2):
        # We only need to load the original SNAP here, so we're fine.
        assert object_manager.object_engine.table_exists(SPLITGRAPH_META_SCHEMA, fruit_snap)
        assert not object_manager.object_engine.table_exists(SPLITGRAPH_META_SCHEMA, vegetables_snap)
        assert not object_manager.object_engine.table_exists(SPLITGRAPH_META_SCHEMA, fruit_diff)
        assert len(object_manager.get_downloaded_objects()) == 1

    # Delete all objects
    object_manager.run_eviction(object_manager.get_full_object_tree(), [], None)
    assert len(object_manager.get_downloaded_objects()) == 0

    # Loading the next version (DIFF + SNAP) (not enough space for 2 objects).
    with pytest.raises(SplitGraphException) as ex:
        with object_manager.ensure_objects(fruits_v3):
            pass
    assert "Not enough space in the cache" in str(ex)


def test_object_cache_nested(local_engine_empty, pg_repo_remote):
    # Test that we can have multiple groups of objects loaded at the same time.
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v3 = pg_repo_local.images['latest'].get_table('fruits')
    fruits_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('fruits')
    vegetables_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('vegetables')
    vegetables_snap = vegetables_v2.objects[0]
    fruit_diff = fruits_v3.objects[0]
    fruit_snap = fruits_v2.objects[0]

    with object_manager.ensure_objects(fruits_v3):
        with object_manager.ensure_objects(vegetables_v2):
            assert _get_refcount(object_manager, fruit_diff) == 1
            assert _get_refcount(object_manager, fruit_snap) == 1
            assert _get_refcount(object_manager, vegetables_snap) == 1
            assert len(object_manager.get_downloaded_objects()) == 3

    # Now evict everything from the cache.
    object_manager.run_eviction(object_manager.get_full_object_tree(), keep_objects=[], required_space=None)
    assert len(object_manager.get_downloaded_objects()) == 0

    object_manager.cache_size = 8192 * 2
    with object_manager.ensure_objects(fruits_v3):
        # Now the fruits objects are being used and so we can't reclaim that space and have to raise an error.
        with pytest.raises(SplitGraphException) as ex:
            with object_manager.ensure_objects(vegetables_v2):
                pass
        assert "Not enough space will be reclaimed" in str(ex)


def test_object_cache_eviction_priority(local_engine_empty, pg_repo_remote):
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('fruits')
    fruits_v3 = pg_repo_local.images['latest'].get_table('fruits')
    fruit_snap = fruits_v2.objects[0]
    fruit_diff = fruits_v3.objects[0]
    vegetables_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('vegetables')
    vegetables_v3 = pg_repo_local.images['latest'].get_table('vegetables')
    vegetables_snap = vegetables_v2.objects[0]
    vegetables_diff = vegetables_v3.objects[0]

    # Setup: the cache has enough space for 3 objects
    object_manager.cache_size = 8192 * 3

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

    # Slightly later: fetch just the old version (the SNAP)
    with object_manager.ensure_objects(fruits_v2):
        # Make sure the timestamp for the SNAP has been bumped.
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

        # The fruit SNAP was used more recently than the DIFF and they both have the same size,
        # so the SNAP will stay.
        current_objects = object_manager.get_downloaded_objects()
        assert fruit_snap in current_objects
        assert fruit_diff not in current_objects
        assert vegetables_snap in current_objects
        assert vegetables_diff in current_objects

        with pytest.raises(SplitGraphException) as ex:
            # Try to load all 4 objects in the same time: should fail.
            with object_manager.ensure_objects(fruits_v3):
                pass
        assert "Not enough space will be reclaimed" in str(ex)


def test_object_cache_snaps(local_engine_empty, pg_repo_remote):
    # Test that asking the cache multiple times for a DIFF object eventually gets it to return SNAPs.
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('fruits')
    fruits_v3 = pg_repo_local.images['latest'].get_table('fruits')
    fruit_snap = fruits_v2.objects[0]
    fruit_diff = fruits_v3.objects[0]

    assert object_manager._get_snap_cache() == {}

    # First, ask the cache 4 times for a resolution (expect DIFF chain).
    for i in range(4):
        with object_manager.ensure_objects(fruits_v3) as required_objects:
            assert object_manager._recent_snap_cache_misses(
                fruit_diff, dt.utcnow() - timedelta(seconds=object_manager.cache_misses_lookback)) == i + 1
            assert required_objects == [fruit_snap, fruit_diff]

    # This time, the cache should give us a brand new SNAP.
    with object_manager.ensure_objects(fruits_v3) as required_objects:
        tmp_snap = required_objects[0]
        assert tmp_snap != fruit_snap
        assert len(required_objects) == 1
        assert tmp_snap in object_manager.get_downloaded_objects()
        # The cache now contains the new SNAP, mapped to the fruits_v3's DIFF chain; the snap has the size 8192.
        assert object_manager._get_snap_cache() == {tmp_snap: (fruit_diff, 8192)}

        # Check we're only counting the new SNAP and none of the old objects.
        assert _get_refcount(object_manager, tmp_snap) == 1
        assert _get_refcount(object_manager, fruit_diff) == 0
        assert _get_refcount(object_manager, fruit_snap) == 0

    # Check that if we ask for a chain again, the same cached SNAP is returned.
    with object_manager.ensure_objects(fruits_v3) as required_objects:
        tmp_snap = required_objects[0]
        assert tmp_snap != fruit_snap
        assert len(required_objects) == 1
        assert len(object_manager.get_downloaded_objects()) == 3

        # Make sure the cache hasn't changed.
        assert object_manager._get_snap_cache() == {tmp_snap: (fruit_diff, 8192)}


def test_object_cache_snaps_eviction(local_engine_empty, pg_repo_remote):
    # Test the temporarily materialized SNAPs get evicted when they're not needed.
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('fruits')
    fruits_v3 = pg_repo_local.images['latest'].get_table('fruits')
    fruit_snap = fruits_v2.objects[0]
    fruit_diff = fruits_v3.objects[0]
    vegetables_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('vegetables')
    vegetables_v3 = pg_repo_local.images['latest'].get_table('vegetables')
    vegetables_snap = vegetables_v2.objects[0]
    vegetables_diff = vegetables_v3.objects[0]

    assert object_manager._get_snap_cache() == {}

    # Poke the cache to get it to generate a SNAP
    for i in range(5):
        with object_manager.ensure_objects(fruits_v3):
            pass
    assert len(object_manager.get_downloaded_objects()) == 3

    # Only space for 2 objects (we currently have 3), so a future download will trigger an eviction.
    object_manager.cache_size = 8192 * 2

    # No download (cache occupancy only calculated at download time), so the old object still stays
    with object_manager.ensure_objects(fruits_v3) as required_objects:
        assert len(object_manager.get_downloaded_objects()) == 3
        assert required_objects[0] != fruit_snap

    # Now, load vegetables_v2 (only results in 1 object, the original SNAP, being downloaded).
    with object_manager.ensure_objects(vegetables_v2) as required_objects:
        # Since the cached SNAP was slightly more recently used, the original fruit SNAP and DIFF will be evicted
        # instead.
        assert required_objects[0] == vegetables_snap
        downloaded_objects = object_manager.get_downloaded_objects()
        assert fruit_snap not in downloaded_objects
        assert fruit_diff not in downloaded_objects
        assert vegetables_snap in downloaded_objects
        assert len(downloaded_objects) == 2

    # Now, load vegetables_v3: since both objects now need to be in the cache,
    # the temporary snap has to be evicted.
    with object_manager.ensure_objects(vegetables_v3):
        downloaded_objects = object_manager.get_downloaded_objects()
        assert vegetables_snap in downloaded_objects
        assert vegetables_diff in downloaded_objects
        assert len(downloaded_objects) == 2

        assert object_manager._get_snap_cache() == {}


def test_object_cache_snaps_cleanup_keeps(local_engine_empty, pg_repo_remote):
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)
    object_manager = pg_repo_local.objects
    fruits_v3 = pg_repo_local.images['latest'].get_table('fruits')
    # Poke the cache to get it to generate a SNAP
    for i in range(5):
        with object_manager.ensure_objects(fruits_v3):
            pass
    assert len(object_manager.get_downloaded_objects()) == 3

    # Run cleanup() and make sure that (since the cached SNAP is still linked to a DIFF that's still required)
    # nothing gets deleted.
    object_manager.cleanup()

    assert len(object_manager._get_snap_cache().items()) == 1
    assert len(object_manager.get_downloaded_objects()) == 3


def test_object_cache_snaps_cleanup_cleans(local_engine_empty, pg_repo_remote):
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)
    object_manager = pg_repo_local.objects
    fruits_v3 = pg_repo_local.images['latest'].get_table('fruits')
    # Poke the cache to get it to generate a SNAP
    for i in range(5):
        with object_manager.ensure_objects(fruits_v3):
            pass
    pg_repo_local.delete()
    # This time, since the DIFF that the cached SNAP is linked to doesn't exist, the cache entry
    # and the actual SNAP should get deleted too.

    object_manager.cleanup()
    assert len(object_manager._get_snap_cache().items()) == 0
    assert len(object_manager.get_downloaded_objects()) == 0


def test_object_cache_snaps_longer_chain(local_engine_empty, pg_repo_remote):
    # Test a longer DIFF chain
    pg_repo_local = _setup_object_cache_test(pg_repo_remote, longer_chain=True)

    object_manager = pg_repo_local.objects
    log = pg_repo_local.images['latest'].get_log()
    fruits_v2 = log[2].get_table('fruits')
    fruits_v3 = log[1].get_table('fruits')
    fruits_v4 = log[0].get_table('fruits')
    fruit_snap = fruits_v2.objects[0]
    fruit_diff = fruits_v3.objects[0]
    fruit_diff_2 = fruits_v4.objects[0]

    assert object_manager._get_snap_cache() == {}

    # First, test hitting the previous version (SNAP -> DIFF)
    for i in range(4):
        with object_manager.ensure_objects(fruits_v3) as required_objects:
            assert required_objects == [fruit_snap, fruit_diff]
    with object_manager.ensure_objects(fruits_v3) as required_objects:
        tmp_snap = required_objects[0]
        assert tmp_snap != fruit_snap
        assert len(required_objects) == 1
        assert _get_refcount(object_manager, tmp_snap) == 1
        assert _get_refcount(object_manager, fruit_diff) == 0
        assert _get_refcount(object_manager, fruit_snap) == 0
        assert object_manager._get_snap_cache() == {tmp_snap: (fruit_diff, 8192)}

    # Check the temporary snap is used in the resolution (even when it's not being created).
    with object_manager.ensure_objects(fruits_v3) as required_objects:
        assert required_objects == [tmp_snap]

    # Now test requesting the next version -- for the first 4 requests it should still return a DIFF
    # chain, but based on the cached SNAP (since then we only have 1 DIFF to apply instead of 2)
    for i in range(4):
        with object_manager.ensure_objects(fruits_v4) as required_objects:
            assert required_objects == [tmp_snap, fruit_diff_2]

    # At this point the SNAP should be cached.
    with object_manager.ensure_objects(fruits_v4) as required_objects:
        tmp_snap_2 = required_objects[0]
        assert tmp_snap_2 != fruit_snap
        assert tmp_snap_2 != fruit_diff
        assert len(required_objects) == 1
        assert _get_refcount(object_manager, tmp_snap_2) == 1
        assert _get_refcount(object_manager, tmp_snap) == 0
        assert _get_refcount(object_manager, fruit_diff) == 0
        assert _get_refcount(object_manager, fruit_diff_2) == 0
        assert _get_refcount(object_manager, fruit_snap) == 0
        assert object_manager._get_snap_cache() == {
            tmp_snap: (fruit_diff, 8192),
            tmp_snap_2: (fruit_diff_2, 8192)
        }
        assert len(object_manager.get_downloaded_objects()) == 5  # Original SNAP, 2 DIFFs and 2 derived SNAPs.

    # Test again this all still works after the cache has been populated
    with object_manager.ensure_objects(fruits_v3) as required_objects:
        assert required_objects == [tmp_snap]
    with object_manager.ensure_objects(fruits_v4) as required_objects:
        assert required_objects == [tmp_snap_2]

    # Test eviction as well -- make sure to get the original version of vegetables with just the SNAP
    # (the third image from the top).
    # Cache has space for 3 objects and since the 2 derived SNAPs were most recently used, they get to stay.
    object_manager.cache_size = 8192 * 3
    vegetables_v2 = pg_repo_local.images['latest'].get_log()[2].get_table('vegetables')
    with object_manager.ensure_objects(vegetables_v2) as required_objects:
        assert len(required_objects) == 1
        downloaded_objects = object_manager.get_downloaded_objects()
        assert len(downloaded_objects) == 3
        assert tmp_snap in downloaded_objects
        assert tmp_snap_2 in downloaded_objects
        assert required_objects[0] in downloaded_objects


def test_object_cache_resolution_with_snaps(pg_repo_local):
    # Test that if there's a SNAP in the object's history, it gets chosen by the object manager correctly
    pg_repo_local.images['latest'].checkout()
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'kumquat')")
    img_1 = pg_repo_local.commit(snap_only=True)
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (5, 'zebra')")
    img_2 = pg_repo_local.commit()
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (6, 'ketchup')")
    img_3 = pg_repo_local.commit()

    with pg_repo_local.objects.ensure_objects(pg_repo_local.images['latest'].get_table('fruits')) as required_objects:
        assert required_objects == [
            img_1.get_table('fruits').objects[0],
            img_2.get_table('fruits').objects[0],
            img_3.get_table('fruits').objects[0]]


def test_object_manager_index_clause_generation(pg_repo_local):
    column_types = {'a': 'int', 'b': 'int'}

    def _assert_ic_result(quals, expected_clause, expected_args):
        qual, args = _quals_to_clause(quals, column_types)
        assert qual.as_string(pg_repo_local.engine.connection) == expected_clause
        assert args == expected_args

    # Test basics: single clause, comparison
    _assert_ic_result([[('a', '>', 5)]], '((NOT index ? %s OR (index #>> \'{"a",1}\')::int  > %s))', ('a', 5,))

    # Two clauses in an OR-block, (in)equality
    _assert_ic_result([[('a', '=', 5), ('b', '<>', 3)]],
                      '((NOT index ? %s OR %s BETWEEN (index #>> \'{"a",0}\')::int '
                      'AND (index #>> \'{"a",1}\')::int) OR (TRUE))',
                      ('a', 5))

    # Two clauses in an AND-block, check unknown operators
    _assert_ic_result([[('a', '<', 3)], [('b', '~', 3)]],
                      '((NOT index ? %s OR (index #>> \'{"a",0}\')::int < %s)) AND ((TRUE))',
                      ('a', 3))


def _prepare_object_filtering_dataset():
    OUTPUT.init()
    OUTPUT.run_sql("""CREATE TABLE test
            (col1 int primary key,
             col2 int,
             col3 varchar,
             col4 timestamp,
             col5 json)""")

    # First object is kind of normal: incrementing PK, a random col2, some text, some timestamps
    OUTPUT.run_sql("INSERT INTO test VALUES (1, 5, 'aaaa', '2016-01-01 00:00:00', '{\"a\": 5}')")
    OUTPUT.run_sql("INSERT INTO test VALUES (5, 3, 'bbbb', '2016-01-02 00:00:00', '{\"a\": 10}')")
    OUTPUT.commit()
    obj_1 = OUTPUT.head.get_table('test').objects[0]
    # Sanity check on index for reference + easier debugging
    assert OUTPUT.objects.get_object_meta([obj_1])[0][5] == \
           {'col1': [1, 5],
            'col2': [3, 5],
            'col3': ['aaaa', 'bbbb'],
            'col4': ['2016-01-01T00:00:00', '2016-01-02T00:00:00']}

    # Second object: PK increments, ranges for col2 and col3 overlap, col4 has the same timestamps everywhere
    OUTPUT.run_sql("INSERT INTO test VALUES (6, 1, 'abbb', '2015-12-30 00:00:00', '{\"a\": 5}')")
    OUTPUT.run_sql("INSERT INTO test VALUES (10, 4, 'cccc', '2015-12-30 00:00:00', '{\"a\": 10}')")
    OUTPUT.commit()
    obj_2 = OUTPUT.head.get_table('test').objects[0]
    assert OUTPUT.objects.get_object_meta([obj_2])[0][5] == {
        'col1': [6, 10],
        'col2': [1, 4],
        'col3': ['abbb', 'cccc'],
        'col4': ['2015-12-30T00:00:00', '2015-12-30T00:00:00']}

    # Third object: just a single row
    OUTPUT.run_sql("INSERT INTO test VALUES (11, 10, 'dddd', '2016-01-05 00:00:00', '{\"a\": 5}')")
    OUTPUT.commit()
    obj_3 = OUTPUT.head.get_table('test').objects[0]
    assert OUTPUT.objects.get_object_meta([obj_3])[0][5] == {
        'col1': [11, 11],
        'col2': [10, 10],
        'col3': ['dddd', 'dddd'],
        'col4': ['2016-01-05T00:00:00', '2016-01-05T00:00:00']}

    # Fourth object: PK increments, ranges for col2/col3 don't overlap, col4 spans obj_1's range, we have a NULL.
    OUTPUT.run_sql("INSERT INTO test VALUES (12, 11, 'eeee', '2015-12-31 00:00:00', '{\"a\": 5}')")
    OUTPUT.run_sql("INSERT INTO test VALUES (14, 13, 'ezzz', NULL, '{\"a\": 5}')")
    OUTPUT.run_sql("INSERT INTO test VALUES (16, 15, 'ffff', '2016-01-04 00:00:00', '{\"a\": 10}')")
    OUTPUT.commit()
    obj_4 = OUTPUT.head.get_table('test').objects[0]
    assert OUTPUT.objects.get_object_meta([obj_4])[0][5] == {
        'col1': [12, 16],
        'col2': [11, 15],
        'col3': ['eeee', 'ffff'],
        'col4': ['2015-12-31T00:00:00', '2016-01-04T00:00:00']}

    return [obj_1, obj_2, obj_3, obj_4]


def test_object_manager_object_filtering(local_engine_empty):
    objects = _prepare_object_filtering_dataset()
    obj_1, obj_2, obj_3, obj_4 = objects
    om = OUTPUT.objects
    column_types = {c[1]: c[2] for c in OUTPUT.engine.get_full_table_schema(SPLITGRAPH_META_SCHEMA, obj_1)}

    def _assert_filter_result(quals, expected):
        assert set(om.filter_fragments(objects, quals, column_types)) == set(expected)

    # Test single quals on PK
    _assert_filter_result([[('col1', '=', 3)]], [obj_1])
    # Even though obj_1 spans 3 (and might include 3 as a value), it might have values that aren't 3.
    _assert_filter_result([[('col1', '<>', 3)]], [obj_1, obj_2, obj_3, obj_4])
    _assert_filter_result([[('col1', '>', 5)]], [obj_2, obj_3, obj_4])
    _assert_filter_result([[('col1', '>=', 5)]], [obj_1, obj_2, obj_3, obj_4])
    _assert_filter_result([[('col1', '<', 11)]], [obj_1, obj_2])
    _assert_filter_result([[('col1', '<=', 11)]], [obj_1, obj_2, obj_3])

    # Test NULL: we don't filter anything down since they aren't included in the index and
    # we don't explicitly track down where they are.
    _assert_filter_result([[('col4', 'IS', 'NULL')]], [obj_1, obj_2, obj_3, obj_4])

    # Test datetime quals
    _assert_filter_result([[('col4', '>', '2015-12-31 00:00:00')]], [obj_1, obj_3, obj_4])
    _assert_filter_result([[('col4', '>', dt(2015, 12, 31))]], [obj_1, obj_3, obj_4])
    _assert_filter_result([[('col4', '<=', '2016-01-01 00:00:00')]], [obj_1, obj_2, obj_4])

    # Test text column
    # Unknown operator (that can't be pruned with the index) returns everything
    _assert_filter_result([[('col3', '~~', 'eee%')]], [obj_1, obj_2, obj_3, obj_4])
    _assert_filter_result([[('col3', '=', 'aaaa')]], [obj_1])
    _assert_filter_result([[('col3', '=', 'accc')]], [obj_1, obj_2])

    # Test combining quals

    # (can't be pruned) OR (can be pruned) returns all since we don't know what will match the first clause
    _assert_filter_result([[('col3', '~~', 'eee%'), ('col1', '=', 3)]], [obj_1, obj_2, obj_3, obj_4])

    # (can't be pruned) AND (can be pruned) returns the same result as the second clause since if something definitely
    # doesn't match the second clause, it won't match the AND.
    _assert_filter_result([[('col3', '~~', 'eee%')], [('col1', '=', 3)]], [obj_1])

    # (col1 > 5 AND col1 < 2)
    _assert_filter_result([[('col1', '>', 5)], [('col1', '<', 2)]], [])

    # (col1 > 5 OR col1 < 2)
    _assert_filter_result([[('col1', '>', 5), ('col1', '<', 2)]], [obj_1, obj_2, obj_3, obj_4])

    # (col1 > 10 (selects obj_3 and 4) OR col4 == 2016-01-01 12:00:00 (selects obj_1 and 4))
    # AND col2 == 2 (selects obj_2)
    # the first clause selects objs 1, 3, 4; second clause selects 2; intersecting them results in []
    _assert_filter_result([[('col1', '>', 10)]], [obj_3, obj_4])
    _assert_filter_result([[('col4', '=', '2016-01-01 12:00:00')]], [obj_1, obj_4])
    _assert_filter_result([[('col2', '=', 2)]], [obj_2])

    _assert_filter_result([[('col1', '>', 10), ('col4', '=', '2016-01-01 12:00:00')]], [obj_1, obj_3, obj_4])
    _assert_filter_result([[('col1', '>', 10), ('col4', '=', '2016-01-01 12:00:00')],
                           [('col2', '=', 2)]], [])

    # Same as previous but the second clause is col3 = 'dddd' (selects only obj_3),
    # hence the final result is just obj_3.
    _assert_filter_result([[('col1', '>', 10), ('col4', '=', '2016-01-01 12:00:00')],
                           [('col3', '=', 'dddd')]], [obj_3])


def test_object_manager_object_filtering_end_to_end(local_engine_empty):
    objects = _prepare_object_filtering_dataset()
    obj_1, obj_2, obj_3, obj_4 = objects
    om = OUTPUT.objects
    table = OUTPUT.head.get_table('test')

    with om.ensure_objects(table) as required_objects:
        assert set(required_objects) == set(objects)

    # Check the object manager filters the quals correctly and doesn't generate a temporary SNAP if not all objects
    # in the chain are used.
    for _ in range(5):
        with om.ensure_objects(table, quals=[[('col1', '>', 10),
                                              ('col4', '=', '2016-01-01 12:00:00')]]) as required_objects:
            assert set(required_objects) == {obj_1, obj_3, obj_4}

    # Check the object manager does create a temporary snap if all objects were used
    # Since we've already has a DIFF cache miss in the beginning, we only need 4
    # misses to generate a SNAP.
    for _ in range(3):
        with om.ensure_objects(table, quals=[[('col1', '>', 1)]]) as required_objects:
            assert set(required_objects) == set(objects)

    with om.ensure_objects(table, quals=[[('col1', '>', 1)]]) as required_objects:
        assert len(required_objects) == 1
        assert required_objects[0] not in objects
