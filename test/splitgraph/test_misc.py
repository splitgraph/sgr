import pytest
import psycopg2
from splitgraph import SplitGraphException
from splitgraph.config import SPLITGRAPH_META_SCHEMA, REGISTRY_META_SCHEMA
from splitgraph.core._common import ensure_metadata_schema
from splitgraph.core.engine import get_current_repositories, lookup_repository
from splitgraph.core.registry import setup_registry_mode, get_published_info, _ensure_registry_schema
from splitgraph.core.repository import Repository

try:
    from unittest import mock
except ImportError:
    import mock


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
        assert get_published_info(Repository('test', 'pg_mount', remote_engine), 'latest') is None
    finally:
        remote_engine.rollback()


def test_repo_lookup_override(remote_engine):
    test_repo = Repository("overridden", "repo", engine=remote_engine)
    try:
        test_repo.init()
        assert lookup_repository("overridden/repo") == test_repo
    finally:
        test_repo.delete(unregister=True, uncheckout=True)


def test_repo_lookup_override_fail():
    with pytest.raises(SplitGraphException) as e:
        lookup_repository("does/not_exist")
    assert "Unknown repository" in str(e)


def test_engine_reconnect(local_engine_empty):
    conn = local_engine_empty.connection
    # Put the connection back into the pool without closing it
    local_engine_empty.rollback()
    # Close the connection ourselves (simulate a timeout/db going away)
    conn.close()

    assert local_engine_empty.run_sql("SELECT 1") == [(1,)]


def test_engine_retry(local_engine_empty):
    conn = local_engine_empty.connection

    with mock.patch.object(local_engine_empty, '_pool') as pool:
        pool.getconn.side_effect = [psycopg2.OperationalError, conn]
        assert local_engine_empty.connection == conn
        assert pool.getconn.call_count == 2
