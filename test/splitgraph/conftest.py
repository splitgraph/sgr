import os

import pytest

from splitgraph.commands import *
from splitgraph.commands import unmount, commit
from splitgraph.commands.misc import make_conn, cleanup_objects
from splitgraph.connection import get_connection, override_driver_connection
from splitgraph.constants import PG_USER, PG_PWD, PG_DB, serialize_connection_string, to_repository as R, Repository
from splitgraph.meta_handler.common import setup_registry_mode, ensure_metadata_schema, toggle_registry_rls
from splitgraph.meta_handler.misc import get_current_repositories
from splitgraph.meta_handler.tags import set_tag, get_current_head
from splitgraph.pg_utils import get_all_foreign_tables
from splitgraph.registry_meta_handler import ensure_registry_schema, unpublish_repository

PG_MNT = R('test/pg_mount')
PG_MNT_PULL = R('test_pg_mount_pull')
MG_MNT = R('test_mg_mount')
MYSQL_MNT = R('test/mysql_mount')
OUTPUT = R('output')


def _mount_postgres(repository):
    mount('tmp', "postgres_fdw",
          dict(server='pgorigin', port=5432, username='originro', password='originpass', dbname="origindb",
               remote_schema="public"))
    import_tables(R('tmp'), get_all_foreign_tables(get_connection(), 'tmp'),
                  repository, [], foreign_tables=True, do_checkout=True)
    unmount(R('tmp'))


def _mount_mongo(repository):
    mount('tmp', "mongo_fdw", dict(server='mongoorigin', port=27017,
                                   username='originro', password='originpass',
                                   stuff={
                                       "db": "origindb",
                                       "coll": "stuff",
                                       "schema": {
                                           "name": "text",
                                           "duration": "numeric",
                                           "happy": "boolean"
                                       }}))
    import_tables(R('tmp'), get_all_foreign_tables(get_connection(), 'tmp'),
                  repository, [], foreign_tables=True, do_checkout=True)
    unmount(R('tmp'))


def _mount_mysql(repository):
    # We don't use this one in tests beyond basic mounting, so no point importing it.
    mount(repository.to_schema(), "mysql_fdw", dict(
        dict(server='mysqlorigin', port=3306, username='originuser', password='originpass',
             remote_schema="mysqlschema")))


TEST_MOUNTPOINTS = [PG_MNT, PG_MNT_PULL, OUTPUT, MG_MNT,
                    R('output_stage_2'), R('testuser/pg_mount'), MYSQL_MNT]


def healthcheck():
    # A pre-flight check before we run the tests to make sure the test architecture has been brought up:
    # the local_driver and the two origins (tested by mounting). There's still an implicit race condition
    # here since we don't touch the remote_driver but we don't run any tests against it until later on,
    # so it should have enough time to start up.
    for mountpoint in [PG_MNT, MG_MNT, MYSQL_MNT]:
        unmount(mountpoint)
    _mount_postgres(PG_MNT)
    _mount_mongo(MG_MNT)
    _mount_mysql(MYSQL_MNT)
    try:
        with get_connection().cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "test/pg_mount".fruits')
            assert cur.fetchone()[0] > 0
            cur.execute('SELECT COUNT(*) FROM "test_mg_mount".stuff')
            assert cur.fetchone()[0] > 0
            cur.execute('SELECT COUNT(*) FROM "test/mysql_mount".mushrooms')
            assert cur.fetchone()[0] > 0
    finally:
        for mountpoint in [PG_MNT, MG_MNT, MYSQL_MNT]:
            unmount(mountpoint)


@pytest.fixture
def sg_pg_conn():
    # SG connection with a mounted Postgres db
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(mountpoint)
    _mount_postgres(PG_MNT)
    yield get_connection()
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(mountpoint)


@pytest.fixture
def sg_pg_mg_conn():
    # SG connection with a mounted Mongo + Postgres db
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(mountpoint)
    cleanup_objects()
    _mount_postgres(PG_MNT)
    _mount_mongo(MG_MNT)
    yield get_connection()
    get_connection().rollback()
    for mountpoint in TEST_MOUNTPOINTS:
        unmount(mountpoint)
    cleanup_objects()


REMOTE_HOST = 'remote_driver'  # On the host, mapped into localhost; on the local driver works as intended.
REMOTE_PORT = 5431


@pytest.fixture
def remote_driver_conn():
    # For these, we'll use both the cachedb (original postgres for integration tests) as well as the remote_driver.
    # Mount and snapshot the two origin DBs (mongo/pg) with the test data.
    conn = make_conn(REMOTE_HOST, REMOTE_PORT, PG_USER, PG_PWD, PG_DB)
    with override_driver_connection(conn):
        ensure_metadata_schema()
        ensure_registry_schema()
        setup_registry_mode()
        toggle_registry_rls('DISABLE')
        unpublish_repository(OUTPUT)
        unpublish_repository(PG_MNT)
        unpublish_repository(Repository('testuser', 'pg_mount'))
        for mountpoint in TEST_MOUNTPOINTS:
            unmount(mountpoint)
        cleanup_objects()
        conn.commit()
        _mount_postgres(PG_MNT)
        _mount_mongo(MG_MNT)
    try:
        yield conn
    finally:
        conn.rollback()
        with override_driver_connection(conn):
            for mountpoint in TEST_MOUNTPOINTS:
                unmount(mountpoint)
            cleanup_objects()
        conn.commit()


@pytest.fixture
def empty_pg_conn():
    # A connection to the local driver that has nothing mounted on it.
    conn = get_connection()
    for mountpoint, _ in get_current_repositories():
        unmount(mountpoint)
    cleanup_objects()
    conn.commit()
    try:
        yield conn
    finally:
        conn.rollback()
        for mountpoint, _ in get_current_repositories():
            unmount(mountpoint)
        cleanup_objects()
        conn.commit()


REMOTE_CONN_STRING = serialize_connection_string(REMOTE_HOST, REMOTE_PORT, PG_USER, PG_PWD, PG_DB)


def add_multitag_dataset_to_remote_driver(remote_driver_conn):
    with override_driver_connection(remote_driver_conn):
        set_tag(PG_MNT, get_current_head(PG_MNT), 'v1')
        with remote_driver_conn.cursor() as cur:
            cur.execute("DELETE FROM \"test/pg_mount\".fruits WHERE fruit_id = 1")
        new_head = commit(PG_MNT)
        set_tag(PG_MNT, new_head, 'v2')
        remote_driver_conn.commit()
        return new_head


SGFILE_ROOT = os.path.join(os.path.dirname(__file__), '../resources/')


def load_sgfile(name):
    with open(SGFILE_ROOT + name, 'r') as f:
        return f.read()
