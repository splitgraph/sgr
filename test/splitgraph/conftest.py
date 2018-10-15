import pytest

from splitgraph.commandline import _conn
from splitgraph.commands import *
from splitgraph.commands import unmount
from splitgraph.commands.misc import make_conn, cleanup_objects
from splitgraph.constants import PG_USER, PG_PWD, PG_DB, serialize_connection_string
from splitgraph.meta_handler.misc import get_current_mountpoints_hashes, get_all_foreign_tables
from splitgraph.registry_meta_handler import ensure_registry_schema, unpublish_repository

PG_MNT = 'test_pg_mount'
MG_MNT = 'test_mg_mount'


def _mount_postgres(conn, mountpoint):
    mount(conn, 'tmp', "postgres_fdw",
          dict(server='pgorigin', port=5432, username='originro', password='originpass', dbname="origindb",
               remote_schema="public"))
    import_tables(conn, 'tmp', get_all_foreign_tables(conn, 'tmp'), mountpoint,
                  [], foreign_tables=True, do_checkout=True)
    unmount(conn, 'tmp')


def _mount_mongo(conn, mountpoint):
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
    import_tables(conn, 'tmp', get_all_foreign_tables(conn, 'tmp'), mountpoint,
                  [], foreign_tables=True, do_checkout=True)
    unmount(conn, 'tmp')

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
    cleanup_objects(conn)
    _mount_postgres(conn, PG_MNT)
    _mount_mongo(conn, MG_MNT)
    yield conn
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(conn, mountpoint)
    cleanup_objects(conn)
    conn.close()


SNAPPER_HOST = 'snapper'  # On the host, mapped by /etc/hosts into localhost; on the pgcache box works as intended.
SNAPPER_PORT = 5431


@pytest.fixture
def snapper_conn():
    # For these, we'll use both the cachedb (original postgres for integration tests) as well as the
    # snapper (currently the Dockerfile just creates 2 mountpoints: mongoorigin and pgorigin, snapshotting the two
    # origin databases)
    # We still create the test_pg_mount and output mountpoints there just so that we don't clash with them.
    conn = make_conn(SNAPPER_HOST, SNAPPER_PORT, PG_USER, PG_PWD, PG_DB)
    ensure_registry_schema(conn)
    unpublish_repository(conn, 'output')
    unpublish_repository(conn, 'test_pg_mount')
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
    for mountpoint, _ in get_current_mountpoints_hashes(conn):
        unmount(conn, mountpoint)
    cleanup_objects(conn)
    conn.commit()
    yield conn
    for mountpoint, _ in get_current_mountpoints_hashes(conn):
        unmount(conn, mountpoint)
    cleanup_objects(conn)
    conn.commit()
    conn.close()


SNAPPER_CONN_STRING = serialize_connection_string(SNAPPER_HOST, SNAPPER_PORT, PG_USER, PG_PWD, PG_DB)
