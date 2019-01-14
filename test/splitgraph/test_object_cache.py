from splitgraph.core import clone, select, ResultShape
from test.splitgraph.commands.test_layered_querying import prepare_lq_repo


def _get_refcount(object_manager, object_id):
    return object_manager.object_engine.run_sql(select("object_cache_status", "refcount", "object_id = %s"),
                                                (object_id,), return_shape=ResultShape.ONE_ONE)


def test_object_cache(local_engine_empty, pg_repo_remote):
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

    # Test object caching, downloading etc.

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
