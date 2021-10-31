from io import StringIO
from test.splitgraph.conftest import SPLITGRAPH_ENGINE_CONTAINER
from unittest import mock
from unittest.mock import MagicMock, Mock, call

import docker
import psycopg2
import pytest
from packaging.version import Version
from splitgraph.__version__ import __version__
from splitgraph.config import CONFIG, SPLITGRAPH_META_SCHEMA
from splitgraph.core.common import ensure_metadata_schema
from splitgraph.core.engine import (
    get_current_repositories,
    lookup_repository,
    repository_exists,
)
from splitgraph.core.object_manager import ObjectManager
from splitgraph.core.repository import Repository
from splitgraph.engine import ResultShape, _prepare_engine_config
from splitgraph.engine.postgres.engine import (
    _API_VERSION,
    PostgresEngine,
    PsycopgEngine,
    _paginate_by_size,
)
from splitgraph.exceptions import (
    APICompatibilityError,
    EngineInitializationError,
    ObjectNotFoundError,
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
            with mock.patch("splitgraph.engine.postgres.engine._quiet", return_value=True):
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
            with mock.patch("splitgraph.engine.postgres.engine._quiet", return_value=True):
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

        local_engine_empty.run_sql("DROP SCHEMA splitgraph_audit CASCADE")
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
    with pytest.raises(ValueError) as e:
        _ = unprivileged_remote_engine.splitgraph_version
    assert "is a registry" in str(e.value)

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
        with pytest.raises(APICompatibilityError):
            unprivileged_remote_engine.run_sql("SELECT 1")

    unprivileged_remote_engine.close()
    unprivileged_remote_engine.connected = False
    with mock.patch.object(unprivileged_remote_engine, "_call_version_func") as cvf:
        with mock.patch("splitgraph.engine.postgres.engine.logging") as log:
            v = Version(_API_VERSION)
            cvf.return_value = "%d.%d.%d" % (v.major, v.minor + 1, v.micro)
            unprivileged_remote_engine.run_sql("SELECT 1")

            assert "Client has a different API version" in log.warning.mock_calls[0][1][0]


def test_client_large_query_chunking():
    cursor = Mock()
    cursor.mogrify = lambda query, args: (query % args).encode("utf-8")

    query = "SELECT splitgraph_api.do_thing(arg1, %s, arg3)"
    args = [("arg2")] * 3
    mogrified = b"SELECT splitgraph_api.do_thing(arg1, arg2, arg3);"

    # Query fits in max_size
    assert list(_paginate_by_size(cursor, query, args, max_size=200)) == [
        mogrified + mogrified + mogrified
    ]

    # First two queries fit
    assert list(_paginate_by_size(cursor, query, args, max_size=100)) == [
        mogrified + mogrified,
        mogrified,
    ]

    # First two queries barely fit (49 + 49 = 98 with semicolons)
    assert list(_paginate_by_size(cursor, query, args, max_size=98)) == [
        mogrified + mogrified,
        mogrified,
    ]

    # First two queries barely don't fit
    assert list(_paginate_by_size(cursor, query, args, max_size=97)) == [
        mogrified,
        mogrified,
        mogrified,
    ]

    # Window slightly larger than query size
    assert list(_paginate_by_size(cursor, query, args, max_size=60)) == [
        mogrified,
        mogrified,
        mogrified,
    ]

    # Query doesn't fit
    with pytest.raises(ValueError):
        assert list(_paginate_by_size(cursor, query, args, max_size=30)) == [
            mogrified,
            mogrified,
            mogrified,
        ]


def test_client_query_batch_chunk_whole():
    engine = MagicMock(spec=PsycopgEngine)
    query = "SELECT splitgraph_api.get_thing(%s, %s, arg3)"
    args = [("arg1_1", "arg2_1"), ("arg1_2", "arg2_2"), ("arg1_3", "arg2_3")]

    # Return list of singletons
    engine.run_sql.side_effect = (["result_1", "result_2"], ["result_3"])
    assert (
        PsycopgEngine.run_chunked_sql(
            engine,
            query,
            args,
            return_shape=ResultShape.MANY_ONE,
            chunk_size=2,
        )
        == ["result_1", "result_2", "result_3"]
    )
    assert engine.run_sql.mock_calls == [
        call(query, args[:2], ResultShape.MANY_ONE),
        call(query, args[2:], ResultShape.MANY_ONE),
    ]
    engine.reset_mock()

    # Return list of tuples
    engine.run_sql.side_effect = (
        [("result_1_1", "result_1_2"), ("result_2_1", "result_2_2")],
        [("result_3_1", "result_3_2")],
    )
    assert PsycopgEngine.run_chunked_sql(
        engine,
        query,
        args,
        return_shape=ResultShape.MANY_MANY,
        chunk_size=2,
    ) == [
        ("result_1_1", "result_1_2"),
        ("result_2_1", "result_2_2"),
        ("result_3_1", "result_3_2"),
    ]
    assert engine.run_sql.mock_calls == [
        call(query, args[:2], ResultShape.MANY_MANY),
        call(query, args[2:], ResultShape.MANY_MANY),
    ]
    engine.reset_mock()

    # Return None
    # (we return empty list here)
    engine.run_sql.side_effect = (None, None)
    assert (
        PsycopgEngine.run_chunked_sql(
            engine,
            query,
            args,
            return_shape=ResultShape.MANY_ONE,
            chunk_size=2,
        )
        == []
    )
    assert engine.run_sql.mock_calls == [
        call(query, args[:2], ResultShape.MANY_ONE),
        call(query, args[2:], ResultShape.MANY_ONE),
    ]
    engine.reset_mock()


def test_client_query_batch_chunk_part():
    # This is useful for when we do e.g. get_object_upload_urls(s3_host, [o1, o2, o3, ...])
    # and want to batch it up to turn into
    #   get_object_upload_urls(s3_host, [o1, o2]);
    #   get_object_upload_urls(s3_host, [o3]);

    engine = MagicMock(spec=PsycopgEngine)
    query = "SELECT splitgraph_api.get_thing(%s, %s, arg3)"
    args = ("arg1", ["arg2_1", "arg2_2", "arg2_3"])
    expected_args = [("arg1", ["arg2_1", "arg2_2"]), ("arg1", ["arg2_3"])]

    engine.run_sql.side_effect = (["result_1", "result_2"], ["result_3"])
    assert PsycopgEngine.run_chunked_sql(
        engine, query, args, return_shape=ResultShape.MANY_ONE, chunk_size=2, chunk_position=1
    ) == ["result_1", "result_2", "result_3"]
    assert engine.run_sql.mock_calls == [
        call(query, expected_args[0], ResultShape.MANY_ONE),
        call(query, expected_args[1], ResultShape.MANY_ONE),
    ]
    engine.reset_mock()


def test_savepoint_stack(pg_repo_local):
    def _count():
        return pg_repo_local.run_sql(
            "SELECT count(*) FROM fruits", return_shape=ResultShape.ONE_ONE
        )

    pg_repo_local.images["latest"].checkout()
    assert _count() == 2
    pg_repo_local.run_sql("INSERT INTO fruits VALUES(3, 'banana')")
    assert _count() == 3

    with pg_repo_local.object_engine.savepoint("banana"):
        pg_repo_local.run_sql("INSERT INTO fruits VALUES(4, 'pineapple')")
        assert _count() == 4

        # Even though we did a rollback, we had a savepoint so we don't delete
        # the first line we inserted.
        pg_repo_local.rollback_engines()
        assert _count() == 3

    # Leave the context manager and roll back now
    assert _count() == 3
    pg_repo_local.rollback_engines()
    assert _count() == 2


def test_object_storage_remounting(pg_repo_local):
    # Simulate physical object file going away and recommitted again.
    missing_object = pg_repo_local.images["latest"].get_table("fruits").objects[0]
    pg_repo_local.images["latest"].checkout()

    assert (
        pg_repo_local.object_engine.run_sql(
            "SELECT splitgraph_api.object_exists(%s)",
            (missing_object,),
            return_shape=ResultShape.ONE_ONE,
        )
        is True
    )
    # Commit again -- this should try to recreate the cstore foreign table but not do
    # anything else since the object files and the table already exist.
    with mock.patch("splitgraph.engine.postgres.engine.logging") as log:
        head = pg_repo_local.commit(snap_only=True)
        assert head.get_table("fruits").objects == [missing_object]
        assert any(
            "already exists" in c[1][0] and c[1][1] == missing_object for c in log.info.mock_calls
        )

    # Now delete the physical object files.
    # Test the corner case of a commit being terminated mid-object writing where
    # one of the object files doesn't actually get written (even though we're
    # doing it inside of a transaction, we write the .schema file separately,
    # so that might not be in place). We have to delete the file manually with Docker.

    client = docker.from_env()
    container = client.containers.get(SPLITGRAPH_ENGINE_CONTAINER)
    container.exec_run(["rm", "/var/lib/splitgraph/objects/%s.schema" % missing_object])

    assert (
        pg_repo_local.object_engine.run_sql(
            "SELECT splitgraph_api.object_exists(%s)",
            (missing_object,),
            return_shape=ResultShape.ONE_ONE,
        )
        is False
    )

    assert missing_object not in pg_repo_local.object_engine.run_sql(
        "SELECT splitgraph_api.list_objects()",
        return_shape=ResultShape.ONE_ONE,
    )

    with mock.patch("splitgraph.engine.postgres.engine.logging") as log:
        head = pg_repo_local.commit(snap_only=True)
        assert head.get_table("fruits").objects == [missing_object]
        assert any(
            "no physical file, recreating" in c[1][0] and c[1][1] == missing_object
            for c in log.info.mock_calls
        )


def test_rename_object(pg_repo_local):
    tested_object = pg_repo_local.images["latest"].get_table("fruits").objects[0]
    # Check we can rename the object (foreign table) and the CStore file backing it
    pg_repo_local.engine.rename_object(tested_object, new_object_id="renamed_object")
    pg_repo_local.engine.commit()

    assert tested_object not in pg_repo_local.objects.get_downloaded_objects()
    assert "renamed_object" in pg_repo_local.objects.get_downloaded_objects()

    pg_repo_local.engine.sync_object_mounts()

    with pytest.raises(ObjectNotFoundError):
        pg_repo_local.engine.run_sql("SELECT * FROM splitgraph_meta." + tested_object)

    pg_repo_local.engine.run_sql("SELECT * FROM splitgraph_meta.renamed_object")
