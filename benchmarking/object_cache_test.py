from contextlib import contextmanager
from datetime import datetime

from benchmarking.commit_chain_test import _cleanup_minio
from splitgraph import get_engine, Repository
from splitgraph.commandline.example import generate_random_table, alter_random_table
from splitgraph.core import clone, ResultShape

MOUNTPOINT = Repository("splitgraph", "benchmark")
REMOTE = Repository.from_template(MOUNTPOINT, engine=get_engine('remote_engine'))
MOUNTPOINT.set_upstream(REMOTE)

TIME = None


@contextmanager
def timeit():
    start = datetime.now()
    try:
        yield
    finally:
        end = datetime.now()
        global TIME
        TIME = (end - start).total_seconds()
        print("Start %s, end %s, total: %.3f" % (start, end, (end - start).total_seconds()))


def _run_sub_bench(bench_name, repository, query, query_args=None):
    results = []

    print("Initial fetch...")
    with timeit():
        repository.run_sql(query, query_args)
    initial_fetch_and_lq = TIME

    print("LQ...")
    for _ in range(3):
        with timeit():
            repository.run_sql(query, query_args)
    lq_against_diff_chain = TIME
    assert repository.objects._get_snap_cache() == {}

    print("LQ + SNAP materialization...")
    with timeit():
        repository.run_sql(query, query_args)
    lq_with_snap_materialization = TIME
    assert repository.objects._get_snap_cache() != {}

    print("LQ from SNAP...")
    with timeit():
        repository.run_sql(query, query_args)
    lq_against_snap = TIME

    results.append((bench_name + ": Minio to Postgres", initial_fetch_and_lq - lq_against_diff_chain))
    results.append((bench_name + ": LQ against DIFF chain", lq_against_diff_chain))
    results.append((bench_name + ": SNAP materialization", lq_with_snap_materialization - lq_against_snap))
    results.append((bench_name + ": LQ against SNAP", lq_against_snap))
    return results


def run_bench(repository):
    results = []
    print("Running SELECT all benchmarks...")
    results.extend(_run_sub_bench("SELECT all", repository, "SELECT key FROM test"))
    keys = repository.run_sql("SELECT * FROM test", return_shape=ResultShape.MANY_ONE)
    one_key = keys[0]

    MOUNTPOINT.objects.cleanup()
    MOUNTPOINT.objects.run_eviction(repository.objects.get_full_object_tree(), keep_objects=[], required_space=None)
    # yeah lol
    MOUNTPOINT.engine.run_sql("DELETE FROM splitgraph_meta.snap_cache_misses")
    MOUNTPOINT.engine.run_sql("DELETE FROM splitgraph_meta.snap_cache")
    MOUNTPOINT.objects.cleanup()
    MOUNTPOINT.engine.commit()

    print("Running SELECT one benchmarks...")
    results.extend(_run_sub_bench("SELECT one key", repository, "SELECT value FROM test WHERE key = %s",
                                  (one_key,)))

    return results


def setup_dataset(table_size, rows_added, rows_deleted, rows_updated, number_of_commits):
    _cleanup_minio()
    REMOTE.rm()
    REMOTE.objects.cleanup()
    REMOTE.objects.run_eviction(object_tree=REMOTE.objects.get_full_object_tree(), keep_objects=[], required_space=None)
    REMOTE.engine.commit()
    MOUNTPOINT.rm()
    MOUNTPOINT.init()

    generate_random_table(MOUNTPOINT, "test", table_size)
    MOUNTPOINT.commit()
    for i in range(number_of_commits):
        alter_random_table(MOUNTPOINT, "test", rows_added=rows_added,
                           rows_deleted=rows_deleted, rows_updated=rows_updated)
        MOUNTPOINT.commit()
    MOUNTPOINT.set_upstream(REMOTE)
    MOUNTPOINT.push(handler='S3', handler_options={})

    MOUNTPOINT.rm()
    MOUNTPOINT.objects.cleanup()
    MOUNTPOINT.objects.run_eviction(MOUNTPOINT.objects.get_full_object_tree(), [], None)
    clone(REMOTE, local_repository=MOUNTPOINT, download_all=False)
    MOUNTPOINT.images['latest'].checkout(layered=True)
    return MOUNTPOINT


if __name__ == "__main__":
    # table_size, rows_added, rows_deleted, rows_updated, number_of_commits
    benchmark_matrix = [(100, 10, 10, 10, 10),
                        (10000, 1000, 1000, 1000, 10),
                        (10000, 1500, 1500, 0, 10),
                        (10000, 100, 100, 0, 1000),
                        (100000, 3000, 3000, 3000, 10),
                        (100000, 4500, 4500, 0, 10),
                        ]

    results = {}

    for case in benchmark_matrix:
        print("Setting up dataset for %r" % (case,))
        repo = setup_dataset(*case)
        print("Dataset set up, running benchmark...")

        results[case] = run_bench(repo)

    output = ""
    for case in benchmark_matrix:
        output += "### Rows in SNAP: %d, DIFFs: %d (%d A, %d D, %d U)\n\n" % (
        case[0], case[4], case[1], case[2], case[3])
        result = results[case]
        output += "\n".join("  * %s: %.2fs" % r for r in result) + "\n\n\n"
    print(output)
