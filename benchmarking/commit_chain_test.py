from random import getrandbits, randrange
from timeit import timeit

from psycopg2.extras import execute_batch

from splitgraph.commandline import _conn
from splitgraph.commands import *
from splitgraph.meta_handler import get_current_head

conn = _conn()
MOUNTPOINT = "splitgraph_benchmark"


def create_random_table(conn, mountpoint, table, N=1000):
    fq_table = mountpoint + '.' + table

    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE %s (id numeric, value varchar, primary key (id))""" % fq_table)
        to_insert = []
        for i in range(N):
            to_insert.append((i, "%0.2x" % getrandbits(256)))
        execute_batch(cur, """INSERT INTO %s VALUES (%%s, %%s)""" % fq_table, to_insert)
    conn.commit()


def alter_random_row(conn, mountpoint, table, N=1000):
    fq_table = mountpoint + '.' + table
    row_id = randrange(0, N)
    with conn.cursor() as cur:
        print(row_id)
        cur.execute("UPDATE %s SET value=%%s WHERE id=%%s" % fq_table, ("%0.2x" % getrandbits(256), row_id))
    conn.commit()
    commit(conn, mountpoint)


def bench_commit_chain_checkout(N):
    unmount(conn, MOUNTPOINT)
    init(conn, MOUNTPOINT)
    create_random_table(conn, MOUNTPOINT, "test", 1000)
    for i in range(N):
        alter_random_row(conn, MOUNTPOINT, "test", 1000)
    rev = get_current_head(conn, MOUNTPOINT)
    print(timeit("checkout(conn, MOUNTPOINT, '%s')" % rev, "from __main__ import conn, MOUNTPOINT, checkout", number=3))

if __name__ == '__main__':
    for N in [10, 100, 1000, 5000, 10000, 20000]:
        print(N)
        bench_commit_chain_checkout(N)
    # N = 1000
    # unmount(conn, MOUNTPOINT)
    # init(conn, MOUNTPOINT)
    # create_random_table(conn, MOUNTPOINT, "test", N)
    # commit(conn, MOUNTPOINT)
    #
    #
    # import cProfile
    # import pstats
    # cProfile.run("checkout(conn, MOUNTPOINT, '8792466ac755ac65a384563db889aa6e616f48cddb1f35b3e216ca0bad786fe1')", 'checkout_fast.cprofile')
    #
    # ps = pstats.Stats('checkout_fast.cprofile')
    # ps_2 = pstats.Stats('checkout.cprofile')