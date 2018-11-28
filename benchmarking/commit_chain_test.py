import os
os.environ['SG_CONFIG_FILE'] = 'test/resources/.sgconfig'

from splitgraph.connection import get_connection, serialize_connection_string, make_conn
from minio import Minio
from splitgraph.commands.misc import cleanup_objects
from splitgraph.config.repo_lookups import get_remote_connection_params


from datetime import datetime
from random import getrandbits, randrange

from psycopg2.extras import execute_batch

from splitgraph.commands import *
from splitgraph.constants import to_repository, S3_HOST, S3_PORT, S3_ACCESS_KEY, \
    S3_SECRET_KEY
from splitgraph.meta_handler.tags import get_current_head

MOUNTPOINT = to_repository("splitgraph_benchmark")
PG_MNT = to_repository('test/pg_mount')


def _cleanup_minio():
    client = Minio('%s:%s' % (S3_HOST, S3_PORT),
                   access_key=S3_ACCESS_KEY,
                   secret_key=S3_SECRET_KEY,
                   secure=False)
    if client.bucket_exists(S3_ACCESS_KEY):
        objects = [o.object_name for o in client.list_objects(bucket_name=S3_ACCESS_KEY)]
        # remove_objects is an iterator, so we force evaluate it
        list(client.remove_objects(bucket_name=S3_ACCESS_KEY, objects_iter=objects))


def create_random_table(mountpoint, table, N=1000):
    fq_table = mountpoint.to_schema() + '.' + table
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE %s (id numeric, value varchar, primary key (id))""" % fq_table)
        to_insert = []
        for i in range(N):
            to_insert.append((i, "%0.2x" % getrandbits(256)))
        execute_batch(cur, """INSERT INTO %s VALUES (%%s, %%s)""" % fq_table, to_insert)
    conn.commit()


def alter_random_row(mountpoint, table, table_size, update_size):
    fq_table = mountpoint.to_schema() + '.' + table
    conn = get_connection()
    with conn.cursor() as cur:
        to_update = []
        for _ in range(update_size):
            row_id = randrange(0, table_size)
            to_update.append(("%0.2x" % getrandbits(256), row_id))
        execute_batch(cur, "UPDATE %s SET value=%%s WHERE id=%%s" % fq_table, to_update)
    conn.commit()
    commit(mountpoint)


def bench_commit_chain_checkout(commits, table_size, update_size):
    unmount(MOUNTPOINT)
    init(MOUNTPOINT)
    print("START")
    print(datetime.now())
    create_random_table(MOUNTPOINT, "test", table_size)
    print("SNAP CREATED")
    print(datetime.now())
    for i in range(commits):
        alter_random_row(MOUNTPOINT, "test", table_size, update_size)
    print("COMMITS MADE")
    print(datetime.now())
    rev = get_current_head(MOUNTPOINT)
    #print(timeit("checkout(conn, MOUNTPOINT, '%s')" % rev, "from __main__ import conn, MOUNTPOINT, checkout", number=3))

if __name__ == '__main__':
    for _ in range(1):
        table_size = 1000000
        update_size = 1000
        commits = 10

        unmount(MOUNTPOINT)
        init(MOUNTPOINT)
        print("START")
        print(datetime.now())
        create_random_table(MOUNTPOINT, "test", table_size)
        print("SNAP CREATED")
        print(datetime.now())
        for i in range(commits):
            alter_random_row(MOUNTPOINT, "test", table_size, update_size)
        print("COMMITS MADE")
        print(datetime.now())
        rev = get_current_head(MOUNTPOINT)
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

    _cleanup_minio()
    remote_conn = make_conn(*get_remote_connection_params('remote_driver'))
    unmount(PG_MNT)
    cleanup_objects(include_external=True)
    remote_conn.commit()
    remote_conn.close()

    print(datetime.now())
    push(MOUNTPOINT, remote_conn_string=serialize_connection_string(*get_remote_connection_params('remote_driver')),
         remote_repository=PG_MNT, handler='S3', handler_options={})
    print("UPLOADED")
    print(datetime.now())

    unmount(MOUNTPOINT)
    cleanup_objects()

    print(datetime.now())
    print("STARTING CLONE + DOWNLOAD")
    clone(PG_MNT, local_repository=MOUNTPOINT, download_all=True)
    print(datetime.now())
    print("STARTING CHECKOUT")
    checkout(MOUNTPOINT, tag='latest')
    print("CHECKOUT DONE")
    print(datetime.now())


