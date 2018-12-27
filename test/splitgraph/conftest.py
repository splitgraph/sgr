import os

import pytest
from minio import Minio

from splitgraph._data.registry import _ensure_registry_schema, unpublish_repository, setup_registry_mode, \
    toggle_registry_rls, set_info_key
from splitgraph.core._common import ensure_metadata_schema
from splitgraph.core.engine import get_current_repositories
from splitgraph.core.object_manager import ObjectManager
from splitgraph.core.repository import to_repository as R, Repository
from splitgraph.engine import get_engine, ResultShape, switch_engine
from splitgraph.hooks.mount_handlers import mount
from splitgraph.hooks.s3 import S3_HOST, S3_PORT, S3_ACCESS_KEY, S3_SECRET_KEY

PG_MNT = R('test/pg_mount')
PG_MNT_PULL = R('test_pg_mount_pull')
MG_MNT = R('test_mg_mount')
MYSQL_MNT = R('test/mysql_mount')
OUTPUT = R('output')


def _mount_postgres(repository):
    mount('tmp', "postgres_fdw",
          dict(server='pgorigin', port=5432, username='originro', password='originpass', dbname="origindb",
               remote_schema="public"))
    R('tmp').import_tables([], repository, [], foreign_tables=True, do_checkout=True)
    R('tmp').rm()


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
    R('tmp').import_tables([], repository, [], foreign_tables=True, do_checkout=True)
    R('tmp').rm()


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
        mountpoint.rm()
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
            mountpoint.rm()


@pytest.fixture
def local_engine_empty():
    engine = get_engine()
    # A connection to the local engine that has nothing mounted on it.
    for mountpoint, _ in get_current_repositories(engine):
        mountpoint.rm()
    for mountpoint in TEST_MOUNTPOINTS:
        mountpoint.rm()
    ObjectManager(engine).cleanup()
    engine.commit()
    try:
        yield engine
    finally:
        engine.rollback()
        for mountpoint, _ in get_current_repositories(engine):
            mountpoint.rm()
        for mountpoint in TEST_MOUNTPOINTS:
            mountpoint.rm()
        ObjectManager(engine).cleanup()
        engine.commit()


with open(os.path.join(os.path.dirname(__file__), '../architecture/data/pgorigin/setup.sql'), 'r') as f:
    PG_DATA = f.read()


def make_pg_repo(engine):
    # Used to be a mount, trying to just write the data directly to speed tests up
    result = Repository("test", "pg_mount", engine=engine)
    result.init()
    result.run_sql(PG_DATA)
    assert result.has_pending_changes()
    result.commit()
    return result


def make_multitag_pg_repo(pg_repo):
    pg_repo.get_image(pg_repo.get_head()).tag('v1')
    pg_repo.run_sql("DELETE FROM fruits WHERE fruit_id = 1")
    new_head = pg_repo.commit()
    pg_repo.get_image(new_head).tag('v2')
    pg_repo.engine.commit()
    return pg_repo


@pytest.fixture
def pg_repo_local(local_engine_empty):
    yield make_pg_repo(local_engine_empty)


@pytest.fixture
def pg_repo_remote(remote_engine):
    yield make_pg_repo(remote_engine)


@pytest.fixture
def pg_repo_remote_multitag(pg_repo_remote):
    yield make_multitag_pg_repo(pg_repo_remote)


@pytest.fixture
def pg_repo_local_multitag(pg_repo_local):
    yield make_multitag_pg_repo(pg_repo_local)


@pytest.fixture
def mg_repo_local(local_engine_empty):
    result = Repository("", "test_mg_mount", engine=local_engine_empty)
    _mount_mongo(result)
    yield result


@pytest.fixture
def mg_repo_remote(remote_engine):
    result = Repository("", "test_mg_mount", engine=remote_engine)
    with switch_engine(remote_engine):
        _mount_mongo(result)
    yield result


REMOTE_ENGINE = 'remote_engine'  # On the host, mapped into localhost; on the local engine works as intended.


@pytest.fixture
def remote_engine():
    engine = get_engine(REMOTE_ENGINE)
    ensure_metadata_schema(engine)
    _ensure_registry_schema(engine)
    set_info_key(engine, 'registry_mode', False)
    setup_registry_mode(engine)
    toggle_registry_rls(engine, 'DISABLE')
    unpublish_repository(Repository('', 'output', engine))
    unpublish_repository(Repository('test', 'pg_mount', engine))
    unpublish_repository(Repository('testuser', 'pg_mount', engine))
    for mountpoint, _ in get_current_repositories(engine):
        mountpoint.rm()
    ObjectManager(engine).cleanup()
    engine.commit()
    try:
        yield engine
    finally:
        engine.rollback()
        for mountpoint, _ in get_current_repositories(engine):
            mountpoint.rm()
        ObjectManager(engine).cleanup()
        engine.commit()
        engine.close()


SPLITFILE_ROOT = os.path.join(os.path.dirname(__file__), '../resources/')


def load_splitfile(name):
    with open(SPLITFILE_ROOT + name, 'r') as f:
        return f.read()


def _cleanup_minio():
    client = Minio('%s:%s' % (S3_HOST, S3_PORT),
                   access_key=S3_ACCESS_KEY,
                   secret_key=S3_SECRET_KEY,
                   secure=False)
    if client.bucket_exists(S3_ACCESS_KEY):
        objects = [o.object_name for o in client.list_objects(bucket_name=S3_ACCESS_KEY)]
        # remove_objects is an iterator, so we force evaluate it
        list(client.remove_objects(bucket_name=S3_ACCESS_KEY, objects_iter=objects))


@pytest.fixture
def clean_minio():
    # Make sure to delete extra objects in the remote Minio bucket
    _cleanup_minio()
    yield
    # Comment this out if tests fail and you want to see what the hell went on in the bucket.
    _cleanup_minio()
