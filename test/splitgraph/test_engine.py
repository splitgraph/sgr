from io import StringIO
from unittest import mock

import psycopg2
import pytest
from packaging.version import Version

from splitgraph.__version__ import __version__
from splitgraph.config import SPLITGRAPH_META_SCHEMA, CONFIG
from splitgraph.core.common import ensure_metadata_schema
from splitgraph.core.engine import get_current_repositories, lookup_repository, repository_exists
from splitgraph.core.object_manager import ObjectManager
from splitgraph.core.repository import Repository
from splitgraph.engine import _prepare_engine_config, ResultShape
from splitgraph.engine.postgres.engine import PostgresEngine, _API_VERSION
from splitgraph.exceptions import (
    EngineInitializationError,
    ObjectNotFoundError,
    APICompatibilityError,
)


def test_metadata_schema(pg_repo_local):
    # Exercise the metadata schema creation code since it might never get reached
    # in test runs where the schema already exists
    try:
        pg_repo_local.engine.delete_schema(SPLITGRAPH_META_SCHEMA)
        ensure_metadata_schema(pg_repo_local.engine)
        assert get_current_repositories(pg_repo_local.engine) == []
    finally:
        pg_repo_local.engine.rollback()


def test_engine_reconnect(local_engine_empty):
    conn = local_engine_empty.connection
    # Put the connection back into the pool without closing it
    local_engine_empty.rollback()
    # Close the connection ourselves (simulate a timeout/db going away)
    conn.close()

    assert local_engine_empty.run_sql("SELECT 1") == [(1,)]


def test_engine_retry(local_engine_empty):
    conn = local_engine_empty.connection

    with mock.patch("splitgraph.engine.postgres.engine.RETRY_DELAY", 0.1):
        with mock.patch.object(local_engine_empty, "_pool") as pool:
            with mock.patch(
                "splitgraph.engine.postgres.engine.sys.stdin.isatty", return_value=True
            ):
                stdout = StringIO()
                with mock.patch("sys.stdout", stdout):
                    pool.getconn.side_effect = [
                        psycopg2.OperationalError,
                        psycopg2.OperationalError,
                        conn,
                    ]
                    assert local_engine_empty.connection == conn
                    assert pool.getconn.call_count == 3
                    assert "Waiting for connection..." in stdout.getvalue()

        with mock.patch.object(local_engine_empty, "_pool") as pool:
            with mock.patch("splitgraph.engine.postgres.engine.RETRY_AMOUNT", 1):
                pool.getconn.side_effect = [
                    psycopg2.OperationalError,
                    psycopg2.OperationalError,
                    conn,
                ]
                with pytest.raises(psycopg2.OperationalError):
                    conn = local_engine_empty.connection


def test_engine_retry_admin(local_engine_empty):
    conn = local_engine_empty._admin_conn()

    with mock.patch("splitgraph.engine.postgres.engine.RETRY_DELAY", 0.1):
        with mock.patch("splitgraph.engine.postgres.engine.psycopg2.connect") as connect:
            with mock.patch(
                "splitgraph.engine.postgres.engine.sys.stdin.isatty", return_value=True
            ):
                stdout = StringIO()
                with mock.patch("sys.stdout", stdout):
                    connect.side_effect = [
                        psycopg2.DatabaseError,
                        psycopg2.DatabaseError,
                        conn,
                    ]
                    assert local_engine_empty._admin_conn() == conn
                    assert connect.call_count == 3
                    assert "Waiting for connection..." in stdout.getvalue()

        with mock.patch("splitgraph.engine.postgres.engine.psycopg2.connect") as connect:
            with mock.patch("splitgraph.engine.postgres.engine.RETRY_AMOUNT", 1):
                connect.side_effect = [
                    psycopg2.DatabaseError,
                    psycopg2.DatabaseError,
                    conn,
                ]
                with pytest.raises(psycopg2.DatabaseError):
                    conn = local_engine_empty._admin_conn()


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
        with pytest.raises(EngineInitializationError) as e:
            lookup_repository("some/repo", include_local=True)
        assert "splitgraph_meta" in str(e.value)
        local_engine_empty.initialize()
        local_engine_empty.commit()

        local_engine_empty.run_sql("DROP SCHEMA splitgraph_api CASCADE")
        with pytest.raises(EngineInitializationError) as e:
            ObjectManager(local_engine_empty).get_downloaded_objects()
        assert "splitgraph_api" in str(e.value)
        local_engine_empty.initialize()
        local_engine_empty.commit()

        local_engine_empty.run_sql("DROP SCHEMA audit CASCADE")
        with pytest.raises(EngineInitializationError) as e:
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


@pytest.mark.registry
def test_client_api_compat(unprivileged_remote_engine):
    # Check client version gets set as an application name + test API
    # version gets verified by the client.
    assert (
        unprivileged_remote_engine.connection.info.dsn_parameters["application_name"]
        == "sgr " + __version__
    )

    unprivileged_remote_engine.close()
    unprivileged_remote_engine.connected = False
    with mock.patch.object(unprivileged_remote_engine, "_call_version_func") as cvf:
        cvf.return_value = _API_VERSION
        unprivileged_remote_engine.run_sql("SELECT 1")

    unprivileged_remote_engine.close()
    unprivileged_remote_engine.connected = False
    with mock.patch.object(unprivileged_remote_engine, "_call_version_func") as cvf:
        v = Version(_API_VERSION)
        cvf.return_value = "%d.%d.%d" % (v.major + 1, v.minor, v.micro)
        with pytest.raises(APICompatibilityError) as e:
            unprivileged_remote_engine.run_sql("SELECT 1")

    unprivileged_remote_engine.close()
    unprivileged_remote_engine.connected = False
    with mock.patch.object(unprivileged_remote_engine, "_call_version_func") as cvf:
        with mock.patch("splitgraph.engine.postgres.engine.logging") as log:
            v = Version(_API_VERSION)
            cvf.return_value = "%d.%d.%d" % (v.major, v.minor + 1, v.micro)
            unprivileged_remote_engine.run_sql("SELECT 1")

            assert "Client has a different API version" in log.warning.mock_calls[0][1][0]
