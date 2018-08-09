from random import getrandbits, sample
from timeit import timeit

from splitgraph.commands import *
from splitgraph.commandline import _conn
conn = _conn()


def create_random_table(conn, mountpoint, table, N=1000):
    fq_table = mountpoint + '.' + table

    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE %s (id numeric, value varchar)""" % fq_table)
        for i in range(N):
            cur.execute("""INSERT INTO %s VALUES (%%s, %%s)""" % fq_table, (i, "%0.2x" % getrandbits(256)))

    conn.commit()


def delete_random_rows(conn, mountpoint, table, N=200):
    fq_table = mountpoint + '.' + table
    with conn.cursor() as cur:
        cur.execute("""SELECT id FROM %s""" % fq_table)
        ids = [c[0] for c in cur.fetchall()]

    to_delete = sample(ids, N)
    with conn.cursor() as cur:
        cur.execute("""DELETE FROM %s WHERE id IN (""" % fq_table + (','.join('%s' for _ in to_delete) + ')'), to_delete)
    conn.commit()


MOUNTPOINT = "splitgraph_benchmark"


def bench_delete_checkout(N):
    unmount(conn, MOUNTPOINT)
    init(conn, MOUNTPOINT)
    create_random_table(conn, MOUNTPOINT, "test", N)
    delete_random_rows(conn, MOUNTPOINT, "test", N/5)
    rev = commit(conn, MOUNTPOINT)

    print(timeit("checkout(conn, MOUNTPOINT, '%s')" % rev, "from __main__ import *", number=3))

if __name__ == '__main__':
    for N in [10, 100, 1000, 5000, 10000, 20000, 50000, 100000]:
        print(N)
        bench_delete_checkout(N)