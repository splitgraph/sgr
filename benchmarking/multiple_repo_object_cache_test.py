import os
import random
import sys

import math
from datetime import datetime
from multiprocessing.pool import Pool

os.environ['SG_CONFIG_FILE'] = 'test/resources/.sgconfig'

from psycopg2.sql import Identifier

from benchmarking.commit_chain_test import _cleanup_minio
from benchmarking.object_cache_test import create_repo
from splitgraph import get_engine, Repository, select, SQL
from splitgraph.core import ResultShape, get_current_repositories
from splitgraph.core._common import gather_sync_metadata
from splitgraph.core.object_manager import get_random_object_id, ObjectManager

MOUNTPOINT = Repository("splitgraph", "benchmark")
REMOTE = Repository.from_template(MOUNTPOINT, engine=get_engine('remote_engine'))
MOUNTPOINT.set_upstream(REMOTE)


def setup_multiple_repositories(table_size, rows_added, rows_deleted, rows_updated,
                                number_of_commits, number_of_repos):
    _cleanup_minio()
    REMOTE.objects.cleanup()
    REMOTE.objects.run_eviction(object_tree=REMOTE.objects.get_full_object_tree(), keep_objects=[], required_space=None)
    REMOTE.engine.commit()
    MOUNTPOINT.engine.run_sql("DELETE FROM splitgraph_meta.snap_cache_misses")
    MOUNTPOINT.engine.run_sql("DELETE FROM splitgraph_meta.snap_cache")
    MOUNTPOINT.objects.cleanup()
    MOUNTPOINT.objects.run_eviction(object_tree=MOUNTPOINT.objects.get_full_object_tree(), keep_objects=[],
                                    required_space=None)
    MOUNTPOINT.engine.commit()

    repo = Repository("splitgraph", "benchmark_0")
    print("Generating %s" % repo.to_schema())
    create_repo(repo, number_of_commits, rows_added, rows_deleted, rows_updated, table_size)
    repo.uncheckout()
    object_meta = repo.engine.run_sql(select("objects", "object_id, format, parent_id, namespace, size"))
    object_locations = repo.objects.get_external_object_locations([o[0] for o in object_meta])

    # Instead of generating a bunch of random data, we generate one repository and then copy it into a bunch of new
    # ones with different object IDs (that still map to the same locations).
    result = [repo]
    for i in range(number_of_repos - 1):
        new_repo = Repository("splitgraph", "benchmark_%d" % (i + 1))
        print("Generating %s" % new_repo.to_schema())
        new_images, table_meta, _, _, _ = \
            gather_sync_metadata(new_repo, repo)

        fake_object_map = {o[0]: get_random_object_id() for o in object_meta}
        table_meta = [(t[0], t[1], t[2], fake_object_map[t[3]]) for t in table_meta]
        new_object_locations = [(fake_object_map[o[0]], o[1], o[2]) for o in object_locations]
        new_object_meta = [(fake_object_map[o[0]], o[1], fake_object_map[o[2]] if o[2] else None,
                            o[3], o[4]) for o in object_meta]
        new_repo.objects.register_objects(new_object_meta)
        new_repo.objects.register_object_locations(new_object_locations)
        new_repo.objects.register_tables(new_repo, table_meta)
        new_repo.set_tags({'HEAD': None}, force=True)

        new_repo.engine.commit()
        result.append(repo)
    return result


def lq_mount_everything(engine):
    # LQ mount all versions of all repos (splitgraph/benchmark_17_v1 ...)
    for repository, _ in get_current_repositories(engine):
        log = repository.images['latest'].get_log()[:-1]
        for i, image in enumerate(reversed(log)):
            target_schema = repository.to_schema() + "_v%d" % i
            image.engine.create_schema(target_schema)
            image._lq_checkout(target_schema)
        engine.commit()


def get_all_demo_schemas(engine):
    return engine.run_sql("SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'splitgraph/%'",
                          return_shape=ResultShape.MANY_ONE)


def lq_cleanup_everything(engine):
    schemas = get_all_demo_schemas(engine)
    for s in schemas:
        print("Deleting %s" % s)
        engine.delete_schema(s)
        engine.run_sql(SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(s + "_lq_checkout_server")))
        engine.commit()


def generate_query_set(engine, number, repo_bias=1., version_bias=1.):
    # Future: access patterns (on repos and versions)
    # repo_bias: Nth most popular repo will be accessed 1/N^repo_bias times as often as the most popular repo.
    # version_bias: Nth most recent version will be accessed 1/N^version_bias times as often
    # as the most popular version.

    repos = [r[0] for r in get_current_repositories(engine)]
    repo_distribution = [1. / math.pow(i + 1, repo_bias) for i in range(len(repos))]
    images = list(range(len(repos[0].images['latest'].get_log()) - 1))
    image_distribution = list(reversed([1. / math.pow(i + 1, version_bias) for i in range(len(images))]))

    result = []
    for _ in range(number):
        schema = "%s_v%d" % (random.choices(repos, repo_distribution)[0].to_schema(),
                             random.choices(images, image_distribution)[0])
        query = SQL("SELECT value FROM {}.test WHERE key IN (42, 534, 123, 999, 1)").format(Identifier(schema))
        result.append(query)
    return result


def get_stats(object_engine):
    stats = {}
    stats['cache_occupancy'] = object_engine.get_cache_occupancy()
    stats['objects_in_cache'] = len(object_engine.get_downloaded_objects())
    stats['objects_in_snap_cache'] = len(object_engine._get_snap_cache())
    return stats


def bench_query(task):
    query_id, query = task
    engine = get_engine()
    start = datetime.now()
    engine.run_sql(query)
    end = datetime.now()
    stats = get_stats(ObjectManager(engine))
    stats['query_execution_time'] = (end - start).total_seconds()
    return query_id, stats


def _purge_engine():
    import splitgraph.engine
    splitgraph.engine._ENGINES = {}


def run_query_set_bench(engine, queries, threads=1):
    results = []

    pool = Pool(threads, initializer=_purge_engine())
    start = datetime.now()
    for i, result in enumerate(pool.imap(bench_query, enumerate(queries), chunksize=100)):
        # for i, result in enumerate(map(bench_query, enumerate(queries))):
        sys.stdout.write("\r(%d/%d)..." % (i, len(queries)))
        results.append(result)
    end = datetime.now()
    results = [r[1] for r in sorted(results)]
    return results, (end - start).total_seconds()


if __name__ == "__main__":
    for repo, head in get_current_repositories(get_engine()):
        repo.rm()
        MOUNTPOINT.engine.run_sql("DELETE FROM splitgraph_meta.snap_cache_misses")
        MOUNTPOINT.engine.run_sql("DELETE FROM splitgraph_meta.snap_cache")
    MOUNTPOINT.objects.cleanup()
    MOUNTPOINT.objects.run_eviction(object_tree=MOUNTPOINT.objects.get_full_object_tree(), keep_objects=[],
                                    required_space=None)
    MOUNTPOINT.engine.commit()

    table_size = 100000
    rows_added = 1000
    rows_deleted = 0
    rows_updated = 10
    number_of_commits = 100
    number_of_repos = 5
    number_of_queries = 1000
    repo_bias = 1.5
    version_bias = 1.5
    parallel_processes = 4

    repos = setup_multiple_repositories(table_size=table_size, rows_added=rows_added, rows_deleted=rows_deleted,
                                        rows_updated=rows_updated, number_of_commits=number_of_commits,
                                        number_of_repos=number_of_repos)
    total_size = sum(v[2] for v in MOUNTPOINT.objects.get_full_object_tree().values())
    # Rough sizes:
    # 10000 table size, 10 DIFFs (1000 A, 100 U, 100 D): about 2.9MB per diff chain.

    lq_cleanup_everything(MOUNTPOINT.engine)
    lq_mount_everything(MOUNTPOINT.engine)
    MOUNTPOINT.engine.commit()

    queries = generate_query_set(MOUNTPOINT.engine, number=number_of_queries, version_bias=version_bias,
                                 repo_bias=repo_bias)
    stats, wall_clock = run_query_set_bench(MOUNTPOINT.engine, queries, threads=parallel_processes)

    from matplotlib import pyplot as plt
    import pandas as pd

    stats_df = pd.DataFrame(stats)
    # ax = (stats_df['cache_occupancy'] / 1024 / 1024).plot()
    # ax2 = ax.twinx()
    # stats_df['objects_in_snap_cache'].plot()
    # stats_df['query_execution_time'].rolling(10).mean().plot(ax=ax2)

    fig_text = "Dataset: %d repo(s), SNAP size %d, %d DIFFs (%d A, %d D, %d U)" % (number_of_repos,
                                                                                   table_size,
                                                                                   number_of_commits,
                                                                                   rows_added,
                                                                                   rows_deleted,
                                                                                   rows_updated)
    fig_text += "\nTotal dataset object size: %.2f MB" % (total_size / 1024. / 1024)
    fig_text += "\nCache: size %d MB, SNAP after %d hit(s)" % (MOUNTPOINT.objects.cache_size / 1024. / 1024.,
                                                               MOUNTPOINT.objects.cache_misses_for_snap)
    fig_text += "\nQueries: %d (bias towards one repository %.2f, bias towards most recent images %.2f)" % \
                (number_of_queries, repo_bias, version_bias)
    fig_text += "\nExecution: %d process(es), total time %.2fs (CPU %.2fs, avg %.2fs)" % (
    parallel_processes, wall_clock,
    stats_df[
        'query_execution_time'].sum(),
    stats_df[
        'query_execution_time'].mean())
    from matplotlib import pyplot as plt

    fig = plt.figure(figsize=(8, 12))
    plt.suptitle(fig_text)
    ax1 = plt.subplot(311)
    stats_df['query_execution_time'].rolling(10).mean().plot(ax=ax1,
                                                             title='Query execution time, s (average over 10 queries)')
    ax2 = plt.subplot(312, sharex=ax1)
    (stats_df['cache_occupancy'] / 1024 / 1024).plot(ax=ax2,
                                                     title="Cache occupancy, MB")
    ax3 = plt.subplot(313, sharex=ax1)
    stats_df['objects_in_cache'].plot(ax=ax3, title="Objects in cache")
    stats_df['objects_in_snap_cache'].plot(ax=ax3)
    plt.legend(["Total", "Temporary SNAPs"])
    plt.subplots_adjust(top=0.87, bottom=0.05)

    i = 0
    while True:
        fname = "cache_stress_test_%03d.png" % i
        if not os.path.exists(fname):
            plt.savefig(fname)
            break
        i += 1
    plt.close()
