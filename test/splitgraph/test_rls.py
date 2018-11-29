import tempfile

import pytest
from psycopg2._psycopg import ProgrammingError

from splitgraph import Repository, get_remote_connection_params
from splitgraph._data.common import toggle_registry_rls
from splitgraph._data.images import get_all_images_parents
from splitgraph._data.objects import register_object_locations
from splitgraph.commands import clone, push, commit, checkout
from splitgraph.commands.info import get_table
from splitgraph.commands.publish import publish
from splitgraph.commands.repository import _unregister_repository
from splitgraph.commands.tagging import get_tagged_id, set_tag
from splitgraph.connection import override_driver_connection, serialize_connection_string
from splitgraph.registry_meta_handler import get_published_info, unpublish_repository
from test.splitgraph.conftest import PG_MNT, add_multitag_dataset_to_remote_driver


def _init_rls_test(remote_driver_conn):
    add_multitag_dataset_to_remote_driver(remote_driver_conn)
    with override_driver_connection(remote_driver_conn):
        toggle_registry_rls('ENABLE')
    remote_driver_conn.commit()


@pytest.fixture()
def unprivileged_conn_string(unprivileged_remote_conn):
    server, port, username, password, dbname = get_remote_connection_params('remote_driver')
    return serialize_connection_string(server, port, 'testuser', 'testpassword', dbname)


@pytest.fixture()
def unprivileged_remote_conn(remote_driver_conn):
    _init_rls_test(remote_driver_conn)
    try:
        with remote_driver_conn.cursor() as cur:
            cur.execute("SET ROLE TO testuser;")
        yield remote_driver_conn
    finally:
        # Reset the role back to admin so that the teardown doesn't break + rollback any failed txns
        remote_driver_conn.rollback()
        with remote_driver_conn.cursor() as cur:
            cur.execute("SET ROLE TO clientuser;")


def test_rls_pull_public(empty_pg_conn, unprivileged_conn_string):
    clone(PG_MNT, remote_conn_string=unprivileged_conn_string)


def test_rls_push_own_delete_own(empty_pg_conn, unprivileged_conn_string, unprivileged_remote_conn):
    clone(PG_MNT, remote_conn_string=unprivileged_conn_string)
    checkout(PG_MNT, tag='latest')

    with empty_pg_conn.cursor() as cur:
        cur.execute("""UPDATE "test/pg_mount".fruits SET name = 'banana' WHERE fruit_id = 1""")
    commit(PG_MNT)

    target_repo = Repository(namespace='testuser', repository='pg_mount')

    # Test we can push to our namespace
    push(PG_MNT, remote_conn_string=unprivileged_conn_string, remote_repository=target_repo)

    # Test we can delete our own repo once we've pushed it
    with override_driver_connection(unprivileged_remote_conn):
        _unregister_repository(target_repo, is_remote=True)
    assert len(get_all_images_parents(target_repo)) == 0


def test_rls_push_others(empty_pg_conn, unprivileged_conn_string):
    clone(PG_MNT, remote_conn_string=unprivileged_conn_string)
    checkout(PG_MNT, tag='latest')

    with empty_pg_conn.cursor() as cur:
        cur.execute("""UPDATE "test/pg_mount".fruits SET name = 'banana' WHERE fruit_id = 1""")
    commit(PG_MNT)

    with pytest.raises(ProgrammingError) as e:
        push(PG_MNT, remote_conn_string=unprivileged_conn_string, remote_repository=PG_MNT)
    assert 'new row violates row-level security policy for table "images"' in str(e.value)


def test_rls_delete_others(unprivileged_remote_conn):
    # RLS doesn't actually raise an error for this, since it just appends the policy qualifier to the query.
    # Hence in this case this simply does nothing (the rows in "test" namespace aren't available for deletion).
    with override_driver_connection(unprivileged_remote_conn):
        _unregister_repository(PG_MNT, is_remote=True)
        # Check that the policy worked by verifying that the repository still exists on the remote.
        assert len(get_all_images_parents(PG_MNT)) > 0


def test_rls_push_own_with_uploading(empty_pg_conn, unprivileged_conn_string):
    clone(PG_MNT, remote_conn_string=unprivileged_conn_string)
    checkout(PG_MNT, tag='latest')

    with empty_pg_conn.cursor() as cur:
        cur.execute("""UPDATE "test/pg_mount".fruits SET name = 'banana' WHERE fruit_id = 1""")
    commit(PG_MNT)

    target_repo = Repository(namespace='testuser', repository='pg_mount')

    with tempfile.TemporaryDirectory() as tmpdir:
        push(PG_MNT, remote_conn_string=unprivileged_conn_string, remote_repository=target_repo, handler='FILE',
             handler_options={'path': tmpdir})


def test_rls_impersonate_external_object(unprivileged_remote_conn):
    with override_driver_connection(unprivileged_remote_conn):
        latest = get_tagged_id(PG_MNT, 'latest')
        sample_object = get_table(PG_MNT, 'fruits', latest)[0][0]
        assert sample_object is not None

        # Try to impersonate the owner of the "test" namespace and add a different external link to
        # an object that they own.
        with pytest.raises(ProgrammingError) as e:
            register_object_locations([(sample_object, 'fake_location', 'FILE')])
        assert 'new row violates row-level security policy for table "object_locations"' in str(e.value)


def test_rls_publish_unpublish_own(empty_pg_conn, unprivileged_conn_string, unprivileged_remote_conn):
    clone(PG_MNT, remote_conn_string=unprivileged_conn_string)
    set_tag(PG_MNT, get_tagged_id(PG_MNT, 'latest'), 'my_tag')
    target_repo = Repository(namespace='testuser', repository='pg_mount')
    push(PG_MNT, remote_conn_string=unprivileged_conn_string, remote_repository=target_repo)

    publish(PG_MNT, 'my_tag', remote_conn_string=unprivileged_conn_string, remote_repository=target_repo,
            readme="my_readme", include_provenance=True, include_table_previews=True)

    with override_driver_connection(unprivileged_remote_conn):
        assert get_published_info(target_repo, 'my_tag') is not None
        unpublish_repository(target_repo)
        assert get_published_info(target_repo, 'my_tag') is None


def test_rls_publish_unpublish_others(empty_pg_conn, remote_driver_conn, unprivileged_conn_string):
    # Tag the remote repo as an admin user and try to publish as an unprivileged one
    with override_driver_connection(remote_driver_conn):
        with remote_driver_conn.cursor() as cur:
            # Make sure we're running this with the elevated privileges
            cur.execute("SET ROLE TO clientuser;")
        set_tag(PG_MNT, get_tagged_id(PG_MNT, 'latest'), 'my_tag')
        remote_driver_conn.commit()
    clone(PG_MNT, remote_conn_string=unprivileged_conn_string)

    # Publish into the "test" namespace as someone who doesn't have access to it.
    with pytest.raises(ProgrammingError) as e:
        publish(PG_MNT, 'my_tag', remote_conn_string=unprivileged_conn_string, readme="my_readme")
    assert 'new row violates row-level security policy for table "images"' in str(e.value)

    # Publish as the admin user
    publish(PG_MNT, 'my_tag',
            remote_conn_string=serialize_connection_string(*get_remote_connection_params('remote_driver')),
            readme="my_readme")

    # Try to delete as the remote user -- should fail (no error raised since the RLS just doesn't make
    # those rows available for deletion)
    with remote_driver_conn.cursor() as cur:
        cur.execute("SET ROLE TO testuser;")

    with override_driver_connection(remote_driver_conn):
        unpublish_repository(PG_MNT)
        assert get_published_info(PG_MNT, 'my_tag') is not None
