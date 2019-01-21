import os

from benchmarking.multiple_repo_object_cache_test import lq_cleanup_everything

os.environ['SG_CONFIG_FILE'] = 'test/resources/.sgconfig'

from benchmarking.commit_chain_test import _cleanup_minio
from benchmarking.object_cache_test import create_repo
from splitgraph import get_engine, Repository

from splitgraph.core.fdw_checkout import QueryingForeignDataWrapper
from multicorn import Qual

MOUNTPOINT = Repository("splitgraph", "benchmark")
REMOTE = Repository.from_template(MOUNTPOINT, engine=get_engine('remote_engine'))
MOUNTPOINT.set_upstream(REMOTE)


def cleanup_cache(repository):
    repository.engine.run_sql("DELETE FROM splitgraph_meta.snap_cache_misses")
    repository.engine.run_sql("DELETE FROM splitgraph_meta.snap_cache")
    repository.objects.cleanup()
    repository.objects.run_eviction(object_tree=repository.objects.get_full_object_tree(), keep_objects=[],
                                    required_space=None)
    repository.engine.commit()


def setup_repo(table_size, rows_added, rows_deleted, rows_updated, number_of_commits):
    _cleanup_minio()
    REMOTE.objects.cleanup()
    REMOTE.objects.run_eviction(object_tree=REMOTE.objects.get_full_object_tree(), keep_objects=[], required_space=None)
    REMOTE.engine.commit()

    repo = Repository("splitgraph", "benchmark_0")
    cleanup_cache(repo)
    print("Generating %s" % repo.to_schema())
    create_repo(repo, number_of_commits, rows_added, rows_deleted, rows_updated, table_size)
    repo.uncheckout()
    return repo


if __name__ == "__main__":
    table_size = 10000
    rows_added = 1000
    rows_deleted = 1000
    rows_updated = 0
    number_of_commits = 1000

    lq_cleanup_everything(MOUNTPOINT.engine)
    MOUNTPOINT.engine.commit()
    repo = setup_repo(table_size, rows_added, rows_deleted, rows_updated, number_of_commits)
    repo.engine.commit()

    cleanup_cache(repo)
    fdw = QueryingForeignDataWrapper(fdw_options={'namespace': repo.namespace,
                                                  'repository': repo.repository,
                                                  'image_hash': repo.images['latest'].image_hash,
                                                  'table': 'test'},
                                     fdw_columns={})  # columns not used
    quals = [Qual(field_name="key",
                  operator=("=", "ANY"),
                  value=(42, 534, 123, 999, 1))]
    columns = ['value']
    # warm up the cache to load the objects in
    list(fdw.execute(quals=quals, columns=columns, sortkeys=[]))
    # del fdw

    from vprof import runner

    runner.run(fdw.execute, options='cpm', kwargs={'quals': quals, 'columns': columns, 'sortkeys': None},
               host='localhost', port=8000)

    # cProfile.run("list(fdw.execute(quals=quals, columns=columns, sortkeys=None))", filename="lq_fdw_.cProfile")

    # stats = pstats.Stats("lq_fdw.cProfile")
    # stats.sort_stats('cumtime').print_stats(20)
