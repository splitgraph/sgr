from unittest import mock

import psycopg2
import pytest

from splitgraph import (
    SPLITGRAPH_META_SCHEMA,
    REGISTRY_META_SCHEMA,
    setup_registry_mode,
    get_published_info,
    Repository,
    ResultShape,
)
from splitgraph.core import get_current_repositories, lookup_repository, CONFIG, repository_exists
from splitgraph.core._common import ensure_metadata_schema
from splitgraph.core.object_manager import ObjectManager
from splitgraph.core.registry import _ensure_registry_schema
from splitgraph.engine import _prepare_engine_config
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.exceptions import UninitializedEngineError, ObjectNotFoundError


def test_metadata_schema(pg_repo_local):
    # Exercise the metadata schema creation code since it might never get reached
    # in test runs where the schema already exists
    try:
        pg_repo_local.engine.delete_schema(SPLITGRAPH_META_SCHEMA)
        ensure_metadata_schema(pg_repo_local.engine)
        assert get_current_repositories(pg_repo_local.engine) == []
    finally:
        pg_repo_local.engine.rollback()


def test_registry_schema(remote_engine):
    # Similar idea -- exercise the registry meta schema code if for when it's already set up,
    try:
        remote_engine.delete_schema(REGISTRY_META_SCHEMA)
        _ensure_registry_schema(remote_engine)
        setup_registry_mode(remote_engine)
        assert get_published_info(Repository("test", "pg_mount", remote_engine), "latest") is None
    finally:
        remote_engine.rollback()


def test_engine_reconnect(local_engine_empty):
    conn = local_engine_empty.connection
    # Put the connection back into the pool without closing it
    local_engine_empty.rollback()
    # Close the connection ourselves (simulate a timeout/db going away)
    conn.close()

    assert local_engine_empty.run_sql("SELECT 1") == [(1,)]


def test_engine_retry(local_engine_empty):
    conn = local_engine_empty.connection

    with mock.patch.object(local_engine_empty, "_pool") as pool:
        pool.getconn.side_effect = [psycopg2.OperationalError, conn]
        assert local_engine_empty.connection == conn
        assert pool.getconn.call_count == 2


def test_run_sql_namedtuple(local_engine_empty):
    many_many_result = local_engine_empty.run_sql("SELECT 1 as foo, 2 as bar", named=True)
    assert len(many_many_result) == 1
    assert many_many_result[0].foo == 1
    assert many_many_result[0][0] == 1
    assert many_many_result[0].bar == 2
    assert many_many_result[0][1] == 2

    one_many_result = local_engine_empty.run_sql(
        "SELECT 1 as foo, 2 as bar", named=True, return_shape=ResultShape.ONE_MANY
    )
    assert one_many_result.foo == 1
    assert one_many_result[0] == 1
    assert one_many_result.bar == 2
    assert one_many_result[1] == 2


def test_uninitialized_engine_error(local_engine_empty):
    # Test things like the audit triggers/splitgraph meta schema missing raise
    # uninitialized engine errors rather than generic SQL errors.
    try:
        local_engine_empty.run_sql("DROP SCHEMA splitgraph_meta CASCADE")
        with pytest.raises(UninitializedEngineError) as e:
            lookup_repository("some/repo", include_local=True)
        assert "splitgraph_meta" in str(e.value)
        local_engine_empty.initialize()
        local_engine_empty.commit()

        local_engine_empty.run_sql("DROP SCHEMA splitgraph_api CASCADE")
        with pytest.raises(UninitializedEngineError) as e:
            ObjectManager(local_engine_empty).get_downloaded_objects()
        assert "splitgraph_api" in str(e.value)
        local_engine_empty.initialize()
        local_engine_empty.commit()

        local_engine_empty.run_sql("DROP SCHEMA audit CASCADE")
        with pytest.raises(UninitializedEngineError) as e:
            local_engine_empty.discard_pending_changes("some/repo")
        assert "Audit triggers" in str(e.value)
    finally:
        local_engine_empty.initialize()
        local_engine_empty.commit()


def test_object_not_found_error(pg_repo_local):
    fruits = pg_repo_local.head.get_table("fruits")
    pg_repo_local.objects.delete_objects(fruits.objects)
    with pytest.raises(ObjectNotFoundError):
        pg_repo_local.engine.run_sql("SELECT * FROM splitgraph_meta." + fruits.objects[0])


def test_engine_autocommit(local_engine_empty):
    conn_params = _prepare_engine_config(CONFIG)
    engine = PostgresEngine(conn_params=conn_params, name="test_engine", autocommit=True)

    repo = Repository("test", "repo", engine=engine)
    repo.init()

    repo.engine.rollback()
    assert repository_exists(Repository.from_template(repo, engine=local_engine_empty))
