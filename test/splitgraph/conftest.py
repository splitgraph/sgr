import pytest

from splitgraph.commandline import _conn
from splitgraph.commands import *
from splitgraph.commands import unmount
from splitgraph.commands.misc import make_conn, cleanup_objects
from splitgraph.constants import PG_PORT, PG_USER, PG_PWD, PG_DB

PG_MNT = 'test_pg_mount'
MG_MNT = 'test_mg_mount'


def _mount_postgres(conn, mountpoint):
    mount(conn, server='pgorigin', port=5432, username='originro', password='originpass', mountpoint=mountpoint,
          mount_handler='postgres_fdw', extra_options={"dbname": "origindb", "remote_schema": "public"})


def _mount_mongo(conn, mountpoint):
    mount(conn, server='mongoorigin', port=27017, username='originro', password='originpass', mountpoint=mountpoint,
          mount_handler='mongo_fdw', extra_options={"stuff": {
            "db": "origindb",
            "coll": "stuff",
            "schema": {
                "name": "text",
                "duration": "numeric",
                "happy": "boolean"
            }}})


TEST_MOUNTPOINTS = [PG_MNT, PG_MNT + '_pull', 'output', MG_MNT, 'output_stage_2']


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
    _mount_postgres(conn, PG_MNT)
    _mount_mongo(conn, MG_MNT)
    yield conn
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(conn, mountpoint)
    conn.close()


SNAPPER_HOST = '172.18.0.7'  # temporary until I figure out how to docker (oh god it also changes between executions)


@pytest.fixture
def snapper_conn():
    # For these, we'll use both the cachedb (original postgres for integration tests) as well as the
    # snapper (currently the Dockerfile just creates 2 mountpoints: mongoorigin and pgorigin, snapshotting the two
    # origin databases)
    # We still create the test_pg_mount and output mountpoints there just so that we don't clash with them.
    conn = make_conn(SNAPPER_HOST, PG_PORT, PG_USER, PG_PWD, PG_DB)
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
    # A connection to the pgcache that has nothing mounted on it.
    conn = _conn()
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(conn, mountpoint)
    cleanup_objects(conn)
    conn.commit()
    yield conn
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(conn, mountpoint)
    cleanup_objects(conn)
    conn.commit()
    conn.close()