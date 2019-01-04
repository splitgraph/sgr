import os

from splitgraph.core.repository import Repository, clone

os.environ['SG_CONFIG_FILE'] = 'test/resources/.sgconfig'

from splitgraph.engine import get_engine, switch_engine
from minio import Minio

from datetime import datetime
from random import getrandbits, randrange

from splitgraph.hooks.s3 import S3_HOST, S3_PORT, S3_ACCESS_KEY, S3_SECRET_KEY

MOUNTPOINT = Repository.from_schema("splitgraph_benchmark")
PG_MNT = Repository.from_schema('test/pg_mount')


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
    mountpoint.run_sql("""CREATE TABLE %s (id numeric, value varchar, primary key (id))""" % table)
    to_insert = []
    for i in range(N):
        to_insert.append((i, "%0.2x" % getrandbits(256)))
    get_engine().run_sql_batch("""INSERT INTO %s VALUES (%%s, %%s)""" % fq_table, to_insert)
    get_engine().commit()


def alter_random_row(mountpoint, table, table_size, update_size):
    fq_table = mountpoint.to_schema() + '.' + table
    to_update = []
    for _ in range(update_size):
        row_id = randrange(0, table_size)
        to_update.append(("%0.2x" % getrandbits(256), row_id))
    get_engine().run_sql_batch("UPDATE %s SET value=%%s WHERE id=%%s" % fq_table, to_update)
    get_engine().commit()
    mountpoint.commit()


def bench_commit_chain_checkout(commits, table_size, update_size):
    MOUNTPOINT.rm()
    MOUNTPOINT.init()
    print("START")
    print(datetime.now())
    create_random_table(MOUNTPOINT, "test", table_size)
    print("SNAP CREATED")
    print(datetime.now())
    for i in range(commits):
        alter_random_row(MOUNTPOINT, "test", table_size, update_size)
    print("COMMITS MADE")
    print(datetime.now())
    rev = MOUNTPOINT.get_head()
    # print(timeit("checkout(conn, MOUNTPOINT, '%s')" % rev, "from __main__ import conn, MOUNTPOINT, checkout", number=3))


if __name__ == '__main__':
    for _ in range(1):
        table_size = 1000000
        update_size = 1000
        commits = 10

        MOUNTPOINT.rm()
        MOUNTPOINT.init()
        print("START")
        print(datetime.now())
        create_random_table(MOUNTPOINT, "test", table_size)
        print("SNAP CREATED")
        print(datetime.now())
        for i in range(commits):
            alter_random_row(MOUNTPOINT, "test", table_size, update_size)
        print("COMMITS MADE")
        print(datetime.now())
        rev = MOUNTPOINT.get_head()
        # print(timeit("checkout(conn, MOUNTPOINT, '%s')" % rev, "from __main__ import conn, MOUNTPOINT, checkout", number=3))

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
    remote_driver = get_engine('remote_driver')
    with switch_engine('remote_driver'):
        MOUNTPOINT.rm()
        MOUNTPOINT.objects.cleanup(include_external=True)
    remote_driver.commit()
    remote_driver.close()

    print(datetime.now())
    MOUNTPOINT.push(remote_engine='remote_driver', remote_repository=PG_MNT, handler='S3', handler_options={})
    print("UPLOADED")
    print(datetime.now())

    MOUNTPOINT.rm()
    MOUNTPOINT.objects.cleanup()

    print(datetime.now())
    print("STARTING CLONE + DOWNLOAD")
    clone(PG_MNT, local_repository=MOUNTPOINT, download_all=True)
    print(datetime.now())
    print("STARTING CHECKOUT")
    MOUNTPOINT.checkout(tag='latest')
    print("CHECKOUT DONE")
    print(datetime.now())
