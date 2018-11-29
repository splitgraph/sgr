from random import getrandbits, sample
from timeit import timeit

from psycopg2.extras import execute_batch

from splitgraph.commands import *
from splitgraph.commands._pg_audit import dump_pending_changes
from splitgraph.connection import get_connection

MOUNTPOINT = "splitgraph_benchmark"


def create_random_table(mountpoint, table, N=1000):
    fq_table = mountpoint + '.' + table

    with get_connection().cursor() as cur:
        cur.execute("""CREATE TABLE %s (id numeric, value varchar, primary key (id))""" % fq_table)
        to_insert = []
        for i in range(N):
            to_insert.append((i, "%0.2x" % getrandbits(256)))
            if len(to_insert) >= 10000:
                execute_batch(cur, """INSERT INTO %s VALUES (%%s, %%s)""" % fq_table, to_insert)
                to_insert = []
                print(i)
    get_connection().commit()


def delete_random_rows(mountpoint, table, N=200):
    fq_table = mountpoint + '.' + table
    with get_connection().cursor() as cur:
        cur.execute("""SELECT id FROM %s""" % fq_table)
        ids = [c[0] for c in cur.fetchall()]

    to_delete = [(int(i),) for i in sample(ids, N)]
    with get_connection().cursor() as cur:
        execute_batch(cur, "DELETE FROM %s WHERE id = %%s" % fq_table, to_delete)
    get_connection().commit()


def bench_delete_checkout(N):
    unmount(MOUNTPOINT)
    init(MOUNTPOINT)
    create_random_table(MOUNTPOINT, "test", N)
    commit(MOUNTPOINT)
    delete_random_rows(MOUNTPOINT, "test", N // 5)
    print(dump_pending_changes(MOUNTPOINT, "test", aggregate=True))
    rev = commit(MOUNTPOINT)

    print(timeit("checkout(MOUNTPOINT, '%s')" % rev, "from __main__ import MOUNTPOINT, checkout", number=3))


if __name__ == '__main__':
    # for N in [10, 100, 1000, 5000, 10000, 20000]:
    #     print(N)
    #     bench_delete_checkout(N)
    N = 1000000
    unmount(MOUNTPOINT)
    init(MOUNTPOINT)
    create_random_table(MOUNTPOINT, "test", N)
    commit(MOUNTPOINT)
