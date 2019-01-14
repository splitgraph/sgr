import pytest

from splitgraph.core import clone, select, ResultShape, SPLITGRAPH_META_SCHEMA
from splitgraph.exceptions import SplitGraphException
from test.splitgraph.commands.test_layered_querying import prepare_lq_repo


def _get_refcount(object_manager, object_id):
    return object_manager.object_engine.run_sql(select("object_cache_status", "refcount", "object_id = %s"),
                                                (object_id,), return_shape=ResultShape.ONE_ONE)


def _setup_object_cache_test(pg_repo_remote):
    pg_repo_local = clone(pg_repo_remote)
    pg_repo_local.images['latest'].checkout()
    prepare_lq_repo(pg_repo_local, include_snap=False, commit_after_every=False, include_pk=True)

    # Same setup as the LQ test in the beginning: we clone a repo from upstream, don't download anything, all
    # objects are on Minio.
    remote = pg_repo_local.push(handler='S3', handler_options={})
    pg_repo_local.rm()
    pg_repo_remote.objects.delete_objects(remote.objects.get_downloaded_objects())
    pg_repo_remote.engine.commit()
    pg_repo_local.objects.cleanup()
    pg_repo_local = clone(pg_repo_remote, download_all=False)

    # 6 objects in the tree (SNAP -> SNAP -> DIFF for both tables)
    assert len(pg_repo_local.objects.get_existing_objects()) == 6
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0
    assert len(remote.objects.get_existing_objects()) == 6
    assert len(remote.objects.get_downloaded_objects()) == 0

    assert len(pg_repo_local.engine.run_sql("SELECT * FROM splitgraph_meta.object_cache_status")) == 6

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
    fruit_diff = fruits_v3.get_object('DIFF')
    assert len(fruits_v2.objects) == 1
    fruit_snap = fruits_v2.get_object('SNAP')
    # Reported by Postgres itself and stored by the engine in object_tree. Might really backfire on us on different
    # Postgres versions.
    assert object_tree[fruit_diff] == ([fruit_snap], 'DIFF', 8192)
    assert object_tree[fruit_snap] == ([], 'SNAP', 8192)

    # Resolve and download the old version: only one SNAP should be downloaded.
    with object_manager.ensure_objects(fruits_v2) as (snap, diffs):
        assert snap == fruit_snap
        assert diffs == []

        assert _get_refcount(object_manager, fruit_snap) == 1
        assert _get_refcount(object_manager, fruit_diff) == 0

    # Exit from the manager: refcounts are 0 but the object is still in the cache since there's enough space.
    assert _get_refcount(object_manager, fruit_snap) == 0
    assert _get_refcount(object_manager, fruit_diff) == 0
    assert len(object_manager.get_downloaded_objects()) == 1

    # Resolve and download the new version: will download the DIFF.
    with object_manager.ensure_objects(fruits_v3) as (snap, diffs):
        assert snap == fruit_snap
        assert diffs == [fruit_diff]

        assert _get_refcount(object_manager, fruit_snap) == 1
        assert _get_refcount(object_manager, fruit_diff) == 1
        assert len(object_manager.get_downloaded_objects()) == 2

    assert _get_refcount(object_manager, fruit_snap) == 0
    assert _get_refcount(object_manager, fruit_diff) == 0
    assert len(object_manager.get_downloaded_objects()) == 2


def test_object_cache_eviction(local_engine_empty, pg_repo_remote):
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v3 = pg_repo_local.images['latest'].get_table('fruits')
    fruits_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('fruits')
    vegetables_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('vegetables')
    vegetables_snap = vegetables_v2.get_object('SNAP')
    fruit_diff = fruits_v3.get_object('DIFF')
    fruit_snap = fruits_v2.get_object('SNAP')

    # Check another test object has the same size
    assert object_manager.get_full_object_tree()[vegetables_snap][2] == 8192

    # Load the fruits objects into the cache
    object_manager.ensure_objects(fruits_v3)

    # Pretend that the cache has no space and try getting a different table
    # Free space is now 0, so we need to run eviction.
    object_manager.cache_size = 8192 * 2
    with object_manager.ensure_objects(vegetables_v2) as (snap, diffs):
        current_objects = object_manager.get_downloaded_objects()
        assert len(current_objects) == 1
        assert fruit_snap not in current_objects
        assert fruit_diff not in current_objects
        assert vegetables_snap in current_objects
        assert vegetables_snap == snap
        assert diffs == []
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

    # However, loading the next version will fail (not enough space for 2 objects).
    with pytest.raises(SplitGraphException) as ex:
        with object_manager.ensure_objects(fruits_v3):
            pass
    assert "Not enough space in the cache" in str(ex.message)


def test_object_cache_nested(local_engine_empty, pg_repo_remote):
    # Test that we can have multiple groups of objects loaded at the same time.
    pg_repo_local = _setup_object_cache_test(pg_repo_remote)

    object_manager = pg_repo_local.objects
    fruits_v3 = pg_repo_local.images['latest'].get_table('fruits')
    fruits_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('fruits')
    vegetables_v2 = pg_repo_local.images[pg_repo_local.images['latest'].parent_id].get_table('vegetables')
    vegetables_snap = vegetables_v2.get_object('SNAP')
    fruit_diff = fruits_v3.get_object('DIFF')
    fruit_snap = fruits_v2.get_object('SNAP')

    with object_manager.ensure_objects(fruits_v3):
        with object_manager.ensure_objects(vegetables_v2):
            assert _get_refcount(object_manager, fruit_diff) == 1
            assert _get_refcount(object_manager, fruit_snap) == 1
            assert _get_refcount(object_manager, vegetables_snap) == 1
            assert len(object_manager.get_downloaded_objects()) == 3

    # Now evict everything from the cache (don't specify any reclaimed space requirements, it'll evict
    # everything with refcount 0 anyway).
    object_manager.run_eviction(object_manager.get_full_object_tree(), keep_objects=[], required_space=0)
    assert len(object_manager.get_downloaded_objects()) == 0

    object_manager.cache_size = 8192 * 2
    with object_manager.ensure_objects(fruits_v3):
        # Now the fruits objects are being used and so we can't reclaim that space and have to raise an error.
        with pytest.raises(SplitGraphException) as ex:
            object_manager.ensure_objects(vegetables_v2)
        assert "Not enough space will be reclaimed" in str(ex.message)
