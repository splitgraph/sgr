from datetime import datetime
from random import getrandbits, randrange

from psycopg2.extras import execute_batch

from splitgraph.commandline import _conn
from splitgraph.commands import *
from splitgraph.meta_handler.tags import get_current_head

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


def alter_random_row(conn, mountpoint, table, table_size, update_size):
    fq_table = mountpoint + '.' + table
    with conn.cursor() as cur:
        to_update = []
        for _ in range(update_size):
            row_id = randrange(0, table_size)
            to_update.append(("%0.2x" % getrandbits(256), row_id))
        execute_batch(cur, "UPDATE %s SET value=%%s WHERE id=%%s" % fq_table, to_update)
    conn.commit()
    commit(conn, mountpoint)


def bench_commit_chain_checkout(commits, table_size, update_size):
    unmount(conn, MOUNTPOINT)
    init(conn, MOUNTPOINT)
    print("START")
    print(datetime.now())
    create_random_table(conn, MOUNTPOINT, "test", table_size)
    print("SNAP CREATED")
    print(datetime.now())
    for i in range(commits):
        alter_random_row(conn, MOUNTPOINT, "test", table_size, update_size)
    print("COMMITS MADE")
    print(datetime.now())
    rev = get_current_head(conn, MOUNTPOINT)
    #print(timeit("checkout(conn, MOUNTPOINT, '%s')" % rev, "from __main__ import conn, MOUNTPOINT, checkout", number=3))

if __name__ == '__main__':
    conn = _conn()
    for _ in range(3):
        table_size = 100000
        update_size = 1000
        commits = 100

        unmount(conn, MOUNTPOINT)
        init(conn, MOUNTPOINT)
        print("START")
        print(datetime.now())
        create_random_table(conn, MOUNTPOINT, "test", table_size)
        print("SNAP CREATED")
        print(datetime.now())
        for i in range(commits):
            alter_random_row(conn, MOUNTPOINT, "test", table_size, update_size)
        print("COMMITS MADE")
        print(datetime.now())
        rev = get_current_head(conn, MOUNTPOINT)
        #print(timeit("checkout(conn, MOUNTPOINT, '%s')" % rev, "from __main__ import conn, MOUNTPOINT, checkout", number=3))


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
