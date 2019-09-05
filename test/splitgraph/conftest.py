import os

import pytest
from minio.error import BucketAlreadyExists, BucketAlreadyOwnedByYou

from splitgraph.core._common import ensure_metadata_schema
from splitgraph.core.engine import get_current_repositories
from splitgraph.core.object_manager import ObjectManager
from splitgraph.core.registry import (
    _ensure_registry_schema,
    setup_registry_mode,
    toggle_registry_rls,
    set_info_key,
)
from splitgraph.core.repository import Repository, clone
from splitgraph.engine import get_engine, ResultShape, switch_engine
from splitgraph.hooks.mount_handlers import mount
from splitgraph.hooks.s3_server import MINIO, S3_BUCKET

R = Repository.from_schema

PG_MNT = R("test/pg_mount")
PG_MNT_PULL = R("test_pg_mount_pull")
MG_MNT = R("test_mg_mount")
MYSQL_MNT = R("test/mysql_mount")
OUTPUT = R("output")

# On the host, mapped into localhost; on the local engine works as intended.
REMOTE_ENGINE = "remote_engine"

# Namespace to push to on the remote engine that the user owns
REMOTE_NAMESPACE = get_engine("unprivileged_remote_engine").conn_params["SG_NAMESPACE"]

# Namespace that the user can read from but can't write to
READONLY_NAMESPACE = "otheruser"

SPLITFILE_ROOT = os.path.join(os.path.dirname(__file__), "../resources/")

INGESTION_RESOURCES = os.path.join(os.path.dirname(__file__), "../resources/ingestion")

# Rough on-disk size taken up by a small (<10 rows) object that we
# use in tests. Includes the actual CStore file, the footer and the
# object's schema (serialized into JSON).
SMALL_OBJECT_SIZE = 350


def _mount_postgres(repository, tables=None):
    mount(
        "tmp",
        "postgres_fdw",
        dict(
            server="pgorigin",
            port=5432,
            username="originro",
            password="originpass",
            dbname="origindb",
            remote_schema="public",
            tables=tables,
        ),
    )
    repository.import_tables([], R("tmp"), [], foreign_tables=True, do_checkout=True)
    R("tmp").delete()


def _mount_mongo(repository):
    mount(
        "tmp",
        "mongo_fdw",
        dict(
            server="mongoorigin",
            port=27017,
            username="originro",
            password="originpass",
            stuff={
                "db": "origindb",
                "coll": "stuff",
                "schema": {"name": "text", "duration": "numeric", "happy": "boolean"},
            },
        ),
    )
    repository.import_tables([], R("tmp"), [], foreign_tables=True, do_checkout=True)
    R("tmp").delete()


def _mount_mysql(repository):
    # We don't use this one in tests beyond basic mounting, so no point importing it.
    mount(
        repository.to_schema(),
        "mysql_fdw",
        dict(
            dict(
                server="mysqlorigin",
                port=3306,
                username="originuser",
                password="originpass",
                remote_schema="mysqlschema",
            )
        ),
    )


TEST_MOUNTPOINTS = [
    PG_MNT,
    PG_MNT_PULL,
    OUTPUT,
    MG_MNT,
    R("output_stage_2"),
    Repository(READONLY_NAMESPACE, "pg_mount"),
    MYSQL_MNT,
]


def healthcheck():
    # A pre-flight check for most tests: check that the local and the remote engine fixtures are up and
    # running.
    get_current_repositories(get_engine())
    get_current_repositories(get_engine("remote_engine"))


def healthcheck_mounting():
    # A pre-flight check for heavier tests that also ensures the three origin databases that we mount for FDW tests
    # are up. Tests that require one of these databases to be up are marked with @pytest.mark.mounting and can be
    # excluded with `poetry run pytest -m "not mounting"`.
    for mountpoint in [PG_MNT, MG_MNT, MYSQL_MNT]:
        mountpoint.delete()
    _mount_postgres(PG_MNT)
    _mount_mongo(MG_MNT)
    _mount_mysql(MYSQL_MNT)
    try:
        assert (
            get_engine().run_sql(
                'SELECT COUNT(*) FROM "test/pg_mount".fruits', return_shape=ResultShape.ONE_ONE
            )
            is not None
        )
        assert (
            get_engine().run_sql(
                'SELECT COUNT(*) FROM "test_mg_mount".stuff', return_shape=ResultShape.ONE_ONE
            )
            is not None
        )
        assert (
            get_engine().run_sql(
                'SELECT COUNT(*) FROM "test/mysql_mount".mushrooms',
                return_shape=ResultShape.ONE_ONE,
            )
            is not None
        )
    finally:
        for mountpoint in [PG_MNT, MG_MNT, MYSQL_MNT]:
            mountpoint.delete()


@pytest.fixture
def local_engine_empty():
    engine = get_engine()
    # A connection to the local engine that has nothing mounted on it.
    for mountpoint, _ in get_current_repositories(engine):
        mountpoint.delete()
    for mountpoint in TEST_MOUNTPOINTS:
        mountpoint.delete()
    ObjectManager(engine).cleanup()
    engine.commit()
    try:
        yield engine
    finally:
        engine.rollback()
        for mountpoint, _ in get_current_repositories(engine):
            mountpoint.delete()
        for mountpoint in TEST_MOUNTPOINTS:
            mountpoint.delete()
        ObjectManager(engine).cleanup()
        engine.commit()


with open(
    os.path.join(os.path.dirname(__file__), "../architecture/data/pgorigin/setup.sql"), "r"
) as f:
    PG_DATA = f.read()


def make_pg_repo(engine, repository=None):
    repository = repository or Repository("test", "pg_mount")
    repository = Repository.from_template(repository, engine=engine)
    repository.init()
    repository.run_sql(PG_DATA)
    assert repository.has_pending_changes()
    repository.commit()
    return repository


def make_multitag_pg_repo(pg_repo):
    pg_repo.head.tag("v1")
    pg_repo.run_sql("DELETE FROM fruits WHERE fruit_id = 1")
    pg_repo.commit().tag("v2")
    pg_repo.commit_engines()
    return pg_repo


@pytest.fixture
def pg_repo_local(local_engine_empty):
    yield make_pg_repo(local_engine_empty)


@pytest.fixture
def pg_repo_remote(remote_engine):
    yield make_pg_repo(remote_engine)


# remote_engine setup but registry-like (no audit triggers, no in-engine object storage)
@pytest.fixture
def remote_engine_registry():
    engine = get_engine(REMOTE_ENGINE)
    ensure_metadata_schema(engine)
    _ensure_registry_schema(engine)
    set_info_key(engine, "registry_mode", False)
    setup_registry_mode(engine)
    toggle_registry_rls(engine, "DISABLE")
    # "Unpublish" all images
    engine.run_sql("DELETE FROM registry_meta.images")
    for mountpoint, _ in get_current_repositories(engine):
        mountpoint.delete(uncheckout=False)
    ObjectManager(engine).cleanup_metadata()
    engine.commit()
    engine.close()
    try:
        yield engine
    finally:
        engine.rollback()
        for mountpoint, _ in get_current_repositories(engine):
            mountpoint.delete(uncheckout=False)
        ObjectManager(engine).cleanup_metadata()
        engine.commit()
        engine.close()


# A fixture for a test repository that exists on the remote with objects
# hosted on S3.
@pytest.fixture
def pg_repo_remote_registry(local_engine_empty, remote_engine_registry, clean_minio):
    staging = Repository("test", "pg_mount_staging")
    staging = make_pg_repo(get_engine(), staging)
    result = staging.push(
        Repository(REMOTE_NAMESPACE, "pg_mount", engine=remote_engine_registry),
        handler="S3",
        handler_options={},
    )
    staging.delete()
    staging.objects.cleanup()
    yield result


@pytest.fixture
def unprivileged_pg_repo(unprivileged_remote_engine, pg_repo_remote_registry):
    """Like pg_repo_remote_registry but accessed as an unprivileged user that can't
    access splitgraph_meta directly and has to use splitgraph_api. If access to
    splitgraph_meta is required, the test can use both fixtures and do e.g.
    pg_repo_remote_registry.objects.get_all_objects()"""
    yield Repository.from_template(pg_repo_remote_registry, engine=unprivileged_remote_engine)


# Like unprivileged_pg_repo but we rename the repository (and all of its objects)
# to be in a different namespace.
@pytest.fixture
def readonly_pg_repo(unprivileged_remote_engine, pg_repo_remote_registry):
    target = Repository.from_template(pg_repo_remote_registry, namespace=READONLY_NAMESPACE)
    clone(pg_repo_remote_registry, target)
    pg_repo_remote_registry.delete(uncheckout=False)
    pg_repo_remote_registry.engine.run_sql(
        "UPDATE splitgraph_meta.objects SET namespace=%s WHERE namespace=%s",
        (READONLY_NAMESPACE, REMOTE_NAMESPACE),
    )
    pg_repo_remote_registry.engine.commit()
    yield Repository.from_template(target, engine=unprivileged_remote_engine)


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


@pytest.fixture
def remote_engine():
    engine = get_engine(REMOTE_ENGINE)
    ensure_metadata_schema(engine)
    _ensure_registry_schema(engine)
    set_info_key(engine, "registry_mode", False)
    setup_registry_mode(engine)
    toggle_registry_rls(engine, "DISABLE")
    # "Unpublish" all images
    engine.run_sql("DELETE FROM registry_meta.images")
    for mountpoint, _ in get_current_repositories(engine):
        mountpoint.delete()
    ObjectManager(engine).cleanup()
    engine.commit()
    engine.close()
    try:
        yield engine
    finally:
        engine.rollback()
        for mountpoint, _ in get_current_repositories(engine):
            mountpoint.delete()
        ObjectManager(engine).cleanup()
        engine.commit()
        engine.close()


@pytest.fixture()
def unprivileged_remote_engine(remote_engine_registry):
    toggle_registry_rls(remote_engine_registry, "ENABLE")
    remote_engine_registry.commit()
    remote_engine_registry.close()
    # Assuption: unprivileged_remote_engine is the same server as remote_engine_registry but with an
    # unprivileged user.
    engine = get_engine("unprivileged_remote_engine")
    engine.close()
    try:
        yield engine
    finally:
        engine.rollback()
        engine.close()


def load_splitfile(name):
    with open(SPLITFILE_ROOT + name, "r") as f:
        return f.read()


def _cleanup_minio():
    if MINIO.bucket_exists(S3_BUCKET):
        objects = [o.object_name for o in MINIO.list_objects(bucket_name=S3_BUCKET)]
        # remove_objects is an iterator, so we force evaluate it
        list(MINIO.remove_objects(bucket_name=S3_BUCKET, objects_iter=objects))


@pytest.fixture
def clean_minio():
    try:
        MINIO.make_bucket(S3_BUCKET)
    except BucketAlreadyExists:
        pass
    except BucketAlreadyOwnedByYou:
        pass

    # Make sure to delete extra objects in the remote Minio bucket
    _cleanup_minio()
    yield MINIO
    # Comment this out if tests fail and you want to see what the hell went on in the bucket.
    _cleanup_minio()


def load_csv(fname):
    with open(os.path.join(INGESTION_RESOURCES, fname), "r") as f:
        return f.read()


@pytest.fixture
def ingestion_test_repo():
    repo = Repository.from_schema("test/ingestion")
    try:
        repo.delete()
        repo.objects.cleanup()
        repo.init()
        yield repo
    finally:
        repo.delete()


def _assert_cache_occupancy(object_manager, number_of_objects):
    """With object sizes varying, we can't exactly assert the cache
    occupancy without putting a bunch of magic constants all over the tests.
    Hence, we assume that all of our small test objects (<10 rows) are within
    150 bytes of SMALL_OBJECT_SIZE and check that the current cache
    occupancy values are consistent with that."""

    cache_occupancy = object_manager.get_cache_occupancy()
    assert abs(cache_occupancy - SMALL_OBJECT_SIZE * number_of_objects) <= 150 * number_of_objects
    assert cache_occupancy == object_manager._recalculate_cache_occupancy()
