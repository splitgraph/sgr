import os

import pytest

from splitgraph._data.common import ensure_metadata_schema
from splitgraph._data.registry import _ensure_registry_schema, unpublish_repository, setup_registry_mode, \
    toggle_registry_rls
from splitgraph.commands import *
from splitgraph.commands.repository import get_current_repositories
from splitgraph.core.repository import to_repository as R, Repository
from splitgraph.engine import get_engine, ResultShape, switch_engine

PG_MNT = R('test/pg_mount')
PG_MNT_PULL = R('test_pg_mount_pull')
MG_MNT = R('test_mg_mount')
MYSQL_MNT = R('test/mysql_mount')
OUTPUT = R('output')


def _mount_postgres(repository):
    mount('tmp', "postgres_fdw",
          dict(server='pgorigin', port=5432, username='originro', password='originpass', dbname="origindb",
               remote_schema="public"))
    R('tmp').import_tables(get_engine().get_all_tables('tmp'),
                           repository, [], foreign_tables=True, do_checkout=True)
    rm(R('tmp'))


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
    R('tmp').import_tables(get_engine().get_all_tables('tmp'),
                           repository, [], foreign_tables=True, do_checkout=True)
    rm(R('tmp'))


def _mount_mysql(repository):
    # We don't use this one in tests beyond basic mounting, so no point importing it.
    mount(repository.to_schema(), "mysql_fdw", dict(
        dict(server='mysqlorigin', port=3306, username='originuser', password='originpass',
             remote_schema="mysqlschema")))


TEST_MOUNTPOINTS = [PG_MNT, PG_MNT_PULL, OUTPUT, MG_MNT,
                    R('output_stage_2'), R('testuser/pg_mount'), MYSQL_MNT]


def healthcheck():
    # A pre-flight check before we run the tests to make sure the test architecture has been brought up:
    # the local_engine and the two origins (tested by mounting). There's still an implicit race condition
    # here since we don't touch the remote_engine but we don't run any tests against it until later on,
    # so it should have enough time to start up.
    for mountpoint in [PG_MNT, MG_MNT, MYSQL_MNT]:
        rm(mountpoint)
    _mount_postgres(PG_MNT)
    _mount_mongo(MG_MNT)
    _mount_mysql(MYSQL_MNT)
    try:
        assert get_engine().run_sql('SELECT COUNT(*) FROM "test/pg_mount".fruits',
                                    return_shape=ResultShape.ONE_ONE) is not None
        assert get_engine().run_sql('SELECT COUNT(*) FROM "test_mg_mount".stuff',
                                    return_shape=ResultShape.ONE_ONE) is not None
        assert get_engine().run_sql('SELECT COUNT(*) FROM "test/mysql_mount".mushrooms',
                                    return_shape=ResultShape.ONE_ONE) is not None
    finally:
        for mountpoint in [PG_MNT, MG_MNT, MYSQL_MNT]:
            rm(mountpoint)


@pytest.fixture
def local_engine_with_pg():
    # SG connection with a mounted Postgres db
    for mountpoint in TEST_MOUNTPOINTS:
        rm(mountpoint)
    _mount_postgres(PG_MNT)
    try:
        yield get_engine()
    finally:
        get_engine().rollback()
        for mountpoint in TEST_MOUNTPOINTS:
            rm(mountpoint)


@pytest.fixture
def local_engine_with_pg_and_mg():
    # SG connection with a mounted Mongo + Postgres db
    for mountpoint in TEST_MOUNTPOINTS:
        rm(mountpoint)
    cleanup_objects()
    _mount_postgres(PG_MNT)
    _mount_mongo(MG_MNT)
    try:
        yield get_engine()
    finally:
        get_engine().rollback()
        for mountpoint in TEST_MOUNTPOINTS:
            rm(mountpoint)
        cleanup_objects()


REMOTE_ENGINE = 'remote_engine'  # On the host, mapped into localhost; on the local engine works as intended.


@pytest.fixture
def remote_engine():
    # For these, we'll use both the cachedb (original postgres for integration tests) as well as the remote_engine.
    # Mount and snapshot the two origin DBs (mongo/pg) with the test data.
    with switch_engine(REMOTE_ENGINE):
        ensure_metadata_schema()
        _ensure_registry_schema()
        setup_registry_mode()
        toggle_registry_rls('DISABLE')
        unpublish_repository(OUTPUT)
        unpublish_repository(PG_MNT)
        unpublish_repository(Repository('testuser', 'pg_mount'))
        for mountpoint in TEST_MOUNTPOINTS:
            rm(mountpoint)
        cleanup_objects()
        get_engine().commit()
        _mount_postgres(PG_MNT)
        _mount_mongo(MG_MNT)
    try:
        yield get_engine(REMOTE_ENGINE)
    finally:
        with switch_engine(REMOTE_ENGINE):
            e = get_engine()
            e.rollback()
            for mountpoint in TEST_MOUNTPOINTS:
                rm(mountpoint)
            cleanup_objects()
            e.commit()
            e.close()


@pytest.fixture
def local_engine_empty():
    # A connection to the local engine that has nothing mounted on it.
    for mountpoint, _ in get_current_repositories():
        rm(mountpoint)
    cleanup_objects()
    get_engine().commit()
    try:
        yield get_engine()
    finally:
        get_engine().rollback()
        for mountpoint, _ in get_current_repositories():
            rm(mountpoint)
        cleanup_objects()
        get_engine().commit()


def add_multitag_dataset_to_engine(engine):
    with switch_engine(engine):
        PG_MNT.get_image(PG_MNT.get_head()).tag('v1')
        engine.run_sql("DELETE FROM \"test/pg_mount\".fruits WHERE fruit_id = 1")
        new_head = PG_MNT.commit()
        PG_MNT.get_image(new_head).tag('v2')
        get_engine().commit()
        return new_head


SPLITFILE_ROOT = os.path.join(os.path.dirname(__file__), '../resources/')


def load_splitfile(name):
    with open(SPLITFILE_ROOT + name, 'r') as f:
        return f.read()
