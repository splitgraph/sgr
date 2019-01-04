from random import getrandbits, sample
from timeit import timeit

from splitgraph import get_engine
from splitgraph.engine import ResultShape

MOUNTPOINT = "splitgraph_benchmark"


def create_random_table(mountpoint, table, N=1000):
    fq_table = mountpoint + '.' + table
    get_engine().run_sql("""CREATE TABLE %s (id numeric, value varchar, primary key (id))""" % fq_table,
                         return_shape=None)
    to_insert = []
    for i in range(N):
        to_insert.append((i, "%0.2x" % getrandbits(256)))
        if len(to_insert) >= 10000:
            get_engine().run_sql_batch("""INSERT INTO %s VALUES (%%s, %%s)""" % fq_table, to_insert)
            to_insert = []
            print(i)
    get_engine().commit()


def delete_random_rows(mountpoint, table, N=200):
    fq_table = mountpoint + '.' + table
    ids = get_engine().run_sql("""SELECT id FROM %s""" % fq_table, return_shape=ResultShape.MANY_ONE)

    to_delete = [(int(i),) for i in sample(ids, N)]
    get_engine().run_sql_batch("DELETE FROM %s WHERE id = %%s" % fq_table, to_delete)
    get_engine().commit()


def bench_delete_checkout(N):
    rm()
    init(MOUNTPOINT)
    create_random_table(MOUNTPOINT, "test", N)
    MOUNTPOINT.commit()
    delete_random_rows(MOUNTPOINT, "test", N // 5)
    print(get_engine().dump_pending_changes(MOUNTPOINT, "test", aggregate=True))
    rev = MOUNTPOINT.commit()

    print(timeit("checkout(MOUNTPOINT, '%s')" % rev, "from __main__ import MOUNTPOINT, checkout", number=3))


if __name__ == '__main__':
    # for N in [10, 100, 1000, 5000, 10000, 20000]:
    #     print(N)
    #     bench_delete_checkout(N)
    N = 1000000
    MOUNTPOINT.rm()
    MOUNTPOINT.init()
    create_random_table(MOUNTPOINT, "test", N)
    MOUNTPOINT.commit()
