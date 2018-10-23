import pytest

from splitgraph.commandline import _conn
from splitgraph.commands import *
from splitgraph.commands import unmount
from splitgraph.commands.misc import make_conn, cleanup_objects
from splitgraph.constants import PG_USER, PG_PWD, PG_DB, serialize_connection_string, to_repository as R
from splitgraph.meta_handler.misc import get_current_repositories
from splitgraph.pg_utils import get_all_foreign_tables
from splitgraph.registry_meta_handler import ensure_registry_schema, unpublish_repository

PG_MNT = R('test/pg_mount')
PG_MNT_PULL = R('test_pg_mount_pull')
MG_MNT = R('test_mg_mount')
OUTPUT = R('output')


def _mount_postgres(conn, repository):
    mount(conn, 'tmp', "postgres_fdw",
          dict(server='pgorigin', port=5432, username='originro', password='originpass', dbname="origindb",
               remote_schema="public"))
    import_tables(conn, R('tmp'), get_all_foreign_tables(conn, 'tmp'), repository, [], foreign_tables=True,
                  do_checkout=True)
    unmount(conn, R('tmp'))


def _mount_mongo(conn, repository):
    mount(conn, 'tmp', "mongo_fdw", dict(server='mongoorigin', port=27017,
                                         username='originro', password='originpass',
                                         stuff={
                                             "db": "origindb",
                                             "coll": "stuff",
                                             "schema": {
                                                 "name": "text",
                                                 "duration": "numeric",
                                                 "happy": "boolean"
                                             }}))
    import_tables(conn, R('tmp'), get_all_foreign_tables(conn, 'tmp'), repository, [], foreign_tables=True,
                  do_checkout=True)
    unmount(conn, R('tmp'))


TEST_MOUNTPOINTS = [PG_MNT, PG_MNT_PULL, OUTPUT, MG_MNT, R('output_stage_2')]


def healthcheck():
    # A pre-flight check before we run the tests to make sure the test architecture has been brought up:
    # the local_driver and the two origins (tested by mounting). There's still an implicit race condition
    # here since we don't touch the remote_driver but we don't run any tests against it until later on,
    # so it should have enough time to start up.
    conn = _conn()
    for mountpoint in [PG_MNT, MG_MNT]:
        unmount(conn, mountpoint)
    _mount_postgres(conn, PG_MNT)
    _mount_mongo(conn, MG_MNT)
    with conn.cursor() as cur:
        cur.execute('SELECT COUNT(*) FROM "test_pg_mount".fruits')
        assert cur.fetchone()[0] > 0
        cur.execute('SELECT COUNT(*) FROM "test_mg_mount".stuff')
        assert cur.fetchone()[0] > 0
    for mountpoint in [PG_MNT, MG_MNT]:
        unmount(conn, mountpoint)
    conn.close()


@pytest.fixture
def sg_pg_conn():
    # SG connection with a mounted Postgres db
    conn = _conn()
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(conn, mountpoint)
    _mount_postgres(conn, PG_MNT)
    yield conn
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(conn, mountpoint)
    conn.close()


@pytest.fixture
def sg_pg_mg_conn():
    # SG connection with a mounted Mongo + Postgres db
    conn = _conn()
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(conn, mountpoint)
    cleanup_objects(conn)
    _mount_postgres(conn, PG_MNT)
    _mount_mongo(conn, MG_MNT)
    yield conn
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(conn, mountpoint)
    cleanup_objects(conn)
    conn.close()


REMOTE_HOST = 'remote_driver'  # On the host, mapped into localhost; on the local driver works as intended.
REMOTE_PORT = 5431


@pytest.fixture
def remote_driver_conn():
    # For these, we'll use both the cachedb (original postgres for integration tests) as well as the remote_driver.
    # Mount and snapshot the two origin DBs (mongo/pg) with the test data.
    conn = make_conn(REMOTE_HOST, REMOTE_PORT, PG_USER, PG_PWD, PG_DB)
    ensure_registry_schema(conn)
    unpublish_repository(conn, OUTPUT)
    unpublish_repository(conn, PG_MNT)
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(conn, mountpoint)
    cleanup_objects(conn)
    conn.commit()
    _mount_postgres(conn, PG_MNT)
    _mount_mongo(conn, MG_MNT)
    yield conn
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(conn, mountpoint)
    cleanup_objects(conn)
    conn.commit()
    conn.close()


@pytest.fixture
def empty_pg_conn():
    # A connection to the local driver that has nothing mounted on it.
    conn = _conn()
    for mountpoint, _ in get_current_repositories(conn):
        unmount(conn, mountpoint)
    cleanup_objects(conn)
    conn.commit()
    yield conn
    for mountpoint, _ in get_current_repositories(conn):
        unmount(conn, mountpoint)
    cleanup_objects(conn)
    conn.commit()
    conn.close()


REMOTE_CONN_STRING = serialize_connection_string(REMOTE_HOST, REMOTE_PORT, PG_USER, PG_PWD, PG_DB)
