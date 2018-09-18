from random import getrandbits, sample
from timeit import timeit

from psycopg2.extras import execute_batch

from splitgraph.commandline import _conn
from splitgraph.commands import *
from splitgraph.pg_replication import dump_pending_changes

conn = _conn()
MOUNTPOINT = "splitgraph_benchmark"


def create_random_table(conn, mountpoint, table, N=1000):
    fq_table = mountpoint + '.' + table

    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE %s (id numeric, value varchar, primary key (id))""" % fq_table)
        to_insert = []
        for i in range(N):
            to_insert.append((i, "%0.2x" % getrandbits(256)))
            if len(to_insert) >= 10000:
                execute_batch(cur, """INSERT INTO %s VALUES (%%s, %%s)""" % fq_table, to_insert)
                to_insert = []
                print(i)

    conn.commit()


def delete_random_rows(conn, mountpoint, table, N=200):
    fq_table = mountpoint + '.' + table
    with conn.cursor() as cur:
        cur.execute("""SELECT id FROM %s""" % fq_table)
        ids = [c[0] for c in cur.fetchall()]

    to_delete = [(int(i),) for i in sample(ids, N)]
    with conn.cursor() as cur:
        execute_batch(cur, "DELETE FROM %s WHERE id = %%s" % fq_table, to_delete)




def bench_delete_checkout(N):
    unmount(conn, MOUNTPOINT)
    init(conn, MOUNTPOINT)
    create_random_table(conn, MOUNTPOINT, "test", N)
    commit(conn, MOUNTPOINT)
    delete_random_rows(conn, MOUNTPOINT, "test", N//5)
    conn.commit()
    print(dump_pending_changes(conn, MOUNTPOINT, "test", aggregate=True))
    with conn.cursor() as cur:
        cur.execute("select count(1) from splitgraph_meta.pending_changes")
        print(cur.fetchall())
    rev = commit(conn, MOUNTPOINT)
    conn.commit()

    print(timeit("checkout(conn, MOUNTPOINT, '%s')" % rev, "from __main__ import conn, MOUNTPOINT, checkout", number=3))

if __name__ == '__main__':
    # for N in [10, 100, 1000, 5000, 10000, 20000]:
    #     print(N)
    #     bench_delete_checkout(N)
    N = 1000000
    unmount(conn, MOUNTPOINT)
    init(conn, MOUNTPOINT)
    create_random_table(conn, MOUNTPOINT, "test", N)
    commit(conn, MOUNTPOINT)