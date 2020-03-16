import logging
import os

import docker
import docker.errors
import pytest
from minio.error import BucketAlreadyExists, BucketAlreadyOwnedByYou
from psycopg2.sql import Identifier, SQL

from splitgraph.commandline.engine import copy_to_container
from splitgraph.config import SPLITGRAPH_META_SCHEMA, CONFIG
from splitgraph.core.common import META_TABLES
from splitgraph.core.engine import get_current_repositories
from splitgraph.core.object_manager import ObjectManager
from splitgraph.core.registry import (
    setup_registry_mode,
    set_info_key,
)
from splitgraph.core.repository import Repository, clone
from splitgraph.engine import get_engine, ResultShape, switch_engine
from splitgraph.hooks.mount_handlers import mount
from splitgraph.hooks.s3_server import MINIO, S3_BUCKET

# Clear out the logging root handler that was installed by click_log
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    format="%(asctime)s [%(process)d] %(levelname)s %(message)s", level=logging.DEBUG
)

R = Repository.from_schema

PG_MNT = R("test/pg_mount")
PG_MNT_PULL = R("test_pg_mount_pull")
MG_MNT = R("test_mg_mount")
MYSQL_MNT = R("test/mysql_mount")
OUTPUT = R("output")

# On the host, mapped into localhost; on the local engine works as intended.
REMOTE_ENGINE = "remote_engine"

# Docker container name for the test engines (used to inject .sgconfig)
SPLITGRAPH_ENGINE_CONTAINER = os.getenv(
    "SG_TEST_LOCAL_ENGINE_CONTAINER", "architecture_local_engine_1"
)

SG_ENGINE_PREFIX = CONFIG["SG_ENGINE_PREFIX"]

# Namespace to push to on the remote engine that the user owns
REMOTE_NAMESPACE = get_engine("unprivileged_remote_engine").conn_params["SG_NAMESPACE"]

# Namespace that the user can read from but can't write to
READONLY_NAMESPACE = "otheruser"

SPLITFILE_ROOT = os.path.join(os.path.dirname(__file__), "../resources/")

INGESTION_RESOURCES = os.path.join(os.path.dirname(__file__), "../resources/ingestion")

API_RESOURCES = os.path.join(os.path.dirname(__file__), "../resources/api")

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
    get_engine().run_sql("SELECT 1")
    get_engine(REMOTE_ENGINE).run_sql("SELECT 1")


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


@pytest.fixture(scope="session")
def test_local_engine():
    # A local Splitgraph engine fixture.
    logging.info("Initializing the test local Splitgraph engine...")
    engine = get_engine()
    engine.initialize()

    # Copy the config file over into the test engine container, since some of the layered querying
    # tests/object downloading tests get the engine to connect to the registry.
    # It's still an open question regarding how we should do it properly (config file changes do have
    # to be reflected in the engine as well). However, doing this instead of the bind mount lets
    # us switch the config used in test by switching the envvar.

    config_path = CONFIG["SG_CONFIG_FILE"]
    client = docker.from_env()

    try:
        container = client.containers.get(SPLITGRAPH_ENGINE_CONTAINER)
        logging.info("Copying .sgconfig (%s) to container %s", config_path, container.short_id)
        copy_to_container(container, config_path, "/.sgconfig")
    except docker.errors.NotFound:
        logging.exception(
            "Could not find the engine test container %s, is it running?",
            SPLITGRAPH_ENGINE_CONTAINER,
        )
    engine.commit()
    logging.info("Test local Splitgraph engine initialized.")
    return engine


@pytest.fixture(scope="session")
def test_remote_engine():
    # A remote (registry-like) Splitgraph engine fixture.
    engine = get_engine(REMOTE_ENGINE)
    if os.getenv("SG_TEST_SKIP_REMOTE_INIT"):
        logging.info("Skipping initializing the test remote Splitgraph engine...")
        return engine

    logging.info("Initializing the test remote Splitgraph engine...")
    engine.initialize()
    engine.commit()
    logging.info("Test remote Splitgraph engine initialized.")
    return engine


@pytest.fixture
def local_engine_empty(test_local_engine):
    # A connection to the local engine that has nothing mounted on it.
    clean_out_engine(test_local_engine)
    try:
        yield test_local_engine
    finally:
        test_local_engine.rollback()
        clean_out_engine(test_local_engine)


def clean_out_engine(engine):
    logging.info("Cleaning out engine %r", engine)
    for mountpoint, _ in get_current_repositories(engine):
        mountpoint.delete()
    for mountpoint in TEST_MOUNTPOINTS:
        Repository.from_template(mountpoint, engine=engine).delete()
        # Make sure schemata, not only repositories, are deleted on the engine.
        engine.run_sql(
            SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(mountpoint.to_schema()))
        )
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


@pytest.fixture(scope="class")
def lq_test_repo(test_local_engine):
    """A read-only fixture for LQ tests that gets reused so that we don't clean out
    the engine after every SELECT query."""
    clean_out_engine(test_local_engine)
    pg_repo_local = make_pg_repo(test_local_engine)

    # Most of these tests are only interesting for where there are multiple fragments, so we have PKs on tables
    # and store them as deltas.
    prepare_lq_repo(pg_repo_local, commit_after_every=True, include_pk=True)

    # Discard the actual materialized table and query everything via FDW
    pg_repo_local.head.checkout(layered=True)

    try:
        yield pg_repo_local
    finally:
        # Canary to make sure that all BASE tables (we use those as temporary storage for Multicorn
        # to read data from) have been deleted by LQs.

        # This is pretty flaky though, since the deletion happens in a separate thread inside
        # of the engine (otherwise there's a deadlock, see _generate_nonsingleton_query
        # in splitgraph.core.table) -- so on teardown sometimes we're faster than the deletion thread
        # and can see temporary tables. Hence, we sleep for a bit before running our check.

        import time

        time.sleep(5)

        tables_in_meta = {
            c
            for c in test_local_engine.get_all_tables(SPLITGRAPH_META_SCHEMA)
            if c not in META_TABLES
        }
        non_foreign_tables = [
            t
            for t in tables_in_meta
            if test_local_engine.get_table_type(SPLITGRAPH_META_SCHEMA, t).startswith("BASE")
        ]
        if non_foreign_tables:
            raise AssertionError(
                "Layered query left temporary tables behind: %r" % non_foreign_tables
            )

        clean_out_engine(test_local_engine)


@pytest.fixture
def pg_repo_remote(remote_engine):
    yield make_pg_repo(remote_engine)


# remote_engine setup but registry-like (no audit triggers, no in-engine object storage)
@pytest.fixture
def remote_engine_registry(test_remote_engine):
    set_info_key(test_remote_engine, "registry_mode", False)
    setup_registry_mode(test_remote_engine)
    for mountpoint, _ in get_current_repositories(test_remote_engine):
        mountpoint.delete(uncheckout=False)
    ObjectManager(test_remote_engine).cleanup_metadata()
    test_remote_engine.commit()
    test_remote_engine.close()
    try:
        yield test_remote_engine
    finally:
        test_remote_engine.rollback()
        for mountpoint, _ in get_current_repositories(test_remote_engine):
            mountpoint.delete(uncheckout=False)
        ObjectManager(test_remote_engine).cleanup_metadata()
        test_remote_engine.commit()
        test_remote_engine.close()


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
def remote_engine(test_remote_engine):
    # "Unpublish" all images
    clean_out_engine(test_remote_engine)
    test_remote_engine.close()
    try:
        yield test_remote_engine
    finally:
        test_remote_engine.rollback()
        clean_out_engine(test_remote_engine)
        test_remote_engine.close()


@pytest.fixture()
def unprivileged_remote_engine(remote_engine_registry):
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


def prepare_lq_repo(repo, commit_after_every, include_pk, snap_only=False):
    OPS = [
        "INSERT INTO fruits VALUES (3, 'mayonnaise')",
        "DELETE FROM fruits WHERE name = 'apple'",
        "DELETE FROM vegetables WHERE vegetable_id = 1;INSERT INTO vegetables VALUES (3, 'celery')",
        "UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2",
    ]

    repo.run_sql("ALTER TABLE fruits ADD COLUMN number NUMERIC DEFAULT 1")
    repo.run_sql("ALTER TABLE fruits ADD COLUMN timestamp TIMESTAMP DEFAULT '2019-01-01T12:00:00'")
    repo.run_sql("COMMENT ON COLUMN fruits.name IS 'Name of the fruit'")
    if include_pk:
        repo.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
        repo.run_sql("ALTER TABLE vegetables ADD PRIMARY KEY (vegetable_id)")
        repo.commit()

    for o in OPS:
        repo.run_sql(o)
        print(o)

        if commit_after_every:
            repo.commit(snap_only=snap_only)
    if not commit_after_every:
        repo.commit(snap_only=snap_only)
