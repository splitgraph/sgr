import tempfile
from copy import copy

import pytest
from psycopg2._psycopg import ProgrammingError

from splitgraph import Repository
from splitgraph._data.images import get_all_image_info
from splitgraph._data.objects import register_object_locations
from splitgraph._data.registry import get_published_info, unpublish_repository, toggle_registry_rls
from splitgraph.commands import clone, push, commit, checkout
from splitgraph.commands.info import get_table
from splitgraph.commands.publish import publish
from splitgraph.commands.repository import unregister_repository
from splitgraph.commands.tagging import get_tagged_id, set_tag
from splitgraph.engine import switch_engine, get_engine
from test.splitgraph.conftest import PG_MNT, add_multitag_dataset_to_engine, REMOTE_ENGINE

UNPRIVILEGED = 'unprivileged_remote_engine'


def _init_rls_test(remote_engine):
    add_multitag_dataset_to_engine(remote_engine)
    with switch_engine(remote_engine):
        toggle_registry_rls('ENABLE')
    remote_engine.commit()


@pytest.fixture()
def unprivileged_remote_engine(remote_engine):
    """Temporarily adds an unprivileged remote engine"""
    _init_rls_test(remote_engine)
    from splitgraph.engine import CONFIG
    CONFIG['remotes'][UNPRIVILEGED] = copy(CONFIG['remotes'][REMOTE_ENGINE])
    CONFIG['remotes'][UNPRIVILEGED]['SG_ENGINE_USER'] = 'testuser'
    CONFIG['remotes'][UNPRIVILEGED]['SG_ENGINE_PWD'] = 'testpassword'
    try:
        E = get_engine(UNPRIVILEGED)
        assert E.conn_params[2] == 'testuser'
        assert E.conn_params[3] == 'testpassword'
        yield E
    finally:
        E.rollback()
        E.close()
        del CONFIG['remotes'][UNPRIVILEGED]


def test_rls_pull_public(local_engine_empty, unprivileged_remote_engine):
    clone(PG_MNT, remote_engine=UNPRIVILEGED)


def test_rls_push_own_delete_own(local_engine_empty, unprivileged_remote_engine):
    clone(PG_MNT, remote_engine=UNPRIVILEGED)
    checkout(PG_MNT, tag='latest')

    local_engine_empty.run_sql("""UPDATE "test/pg_mount".fruits SET name = 'banana' WHERE fruit_id = 1""",
                               return_shape=None)
    commit(PG_MNT)

    target_repo = Repository(namespace='testuser', repository='pg_mount')

    # Test we can push to our namespace
    push(PG_MNT, remote_engine=UNPRIVILEGED, remote_repository=target_repo)

    # Test we can delete our own repo once we've pushed it
    with switch_engine(UNPRIVILEGED):
        unregister_repository(target_repo, is_remote=True)
        assert len(get_all_image_info(target_repo)) == 0


def test_rls_push_others(local_engine_empty, unprivileged_remote_engine):
    clone(PG_MNT, remote_engine=UNPRIVILEGED)
    checkout(PG_MNT, tag='latest')

    local_engine_empty.run_sql("""UPDATE "test/pg_mount".fruits SET name = 'banana' WHERE fruit_id = 1""",
                               return_shape=None)
    commit(PG_MNT)

    with pytest.raises(ProgrammingError) as e:
        push(PG_MNT, remote_engine=UNPRIVILEGED, remote_repository=PG_MNT)
    assert 'new row violates row-level security policy for table "images"' in str(e.value)


def test_rls_delete_others(unprivileged_remote_engine):
    # RLS doesn't actually raise an error for this, since it just appends the policy qualifier to the query.
    # Hence in this case this simply does nothing (the rows in "test" namespace aren't available for deletion).
    with switch_engine(UNPRIVILEGED):
        unregister_repository(PG_MNT, is_remote=True)
        # Check that the policy worked by verifying that the repository still exists on the remote.
        assert len(get_all_image_info(PG_MNT)) > 0


def test_rls_push_own_with_uploading(local_engine_empty, unprivileged_remote_engine):
    clone(PG_MNT, remote_engine=UNPRIVILEGED)
    checkout(PG_MNT, tag='latest')

    local_engine_empty.run_sql("""UPDATE "test/pg_mount".fruits SET name = 'banana' WHERE fruit_id = 1""",
                               return_shape=None)
    commit(PG_MNT)

    target_repo = Repository(namespace='testuser', repository='pg_mount')

    with tempfile.TemporaryDirectory() as tmpdir:
        push(PG_MNT, remote_engine=UNPRIVILEGED, remote_repository=target_repo, handler='S3')


def test_rls_impersonate_external_object(unprivileged_remote_engine):
    with switch_engine(UNPRIVILEGED):
        latest = get_tagged_id(PG_MNT, 'latest')
        sample_object = get_table(PG_MNT, 'fruits', latest)[0][0]
        assert sample_object is not None

        # Try to impersonate the owner of the "test" namespace and add a different external link to
        # an object that they own.
        with pytest.raises(ProgrammingError) as e:
            register_object_locations([(sample_object, 'fake_location', 'S3')])
        assert 'new row violates row-level security policy for table "object_locations"' in str(e.value)


def test_rls_publish_unpublish_own(local_engine_empty, unprivileged_remote_engine):
    clone(PG_MNT, remote_engine=UNPRIVILEGED, download_all=True)
    set_tag(PG_MNT, get_tagged_id(PG_MNT, 'latest'), 'my_tag')
    target_repo = Repository(namespace='testuser', repository='pg_mount')
    push(PG_MNT, remote_engine=UNPRIVILEGED, remote_repository=target_repo)

    publish(PG_MNT, 'my_tag', remote_engine_name=UNPRIVILEGED, remote_repository=target_repo,
            readme="my_readme", include_provenance=True, include_table_previews=True)

    with switch_engine(UNPRIVILEGED):
        assert get_published_info(target_repo, 'my_tag') is not None
        unpublish_repository(target_repo)
        assert get_published_info(target_repo, 'my_tag') is None


def test_rls_publish_unpublish_others(local_engine_empty, remote_engine, unprivileged_remote_engine):
    # Tag the remote repo as an admin user and try to publish as an unprivileged one
    with switch_engine(REMOTE_ENGINE):
        set_tag(PG_MNT, get_tagged_id(PG_MNT, 'latest'), 'my_tag')
        remote_engine.commit()
    clone(PG_MNT, remote_engine=UNPRIVILEGED, download_all=True)

    # Publish into the "test" namespace as someone who doesn't have access to it.
    with pytest.raises(ProgrammingError) as e:
        publish(PG_MNT, 'my_tag', remote_engine_name=UNPRIVILEGED, readme="my_readme")
    assert 'new row violates row-level security policy for table "images"' in str(e.value)

    # Publish as the admin user
    publish(PG_MNT, 'my_tag', remote_engine_name=REMOTE_ENGINE, readme="my_readme")

    # Try to delete as the remote user -- should fail (no error raised since the RLS just doesn't make
    # those rows available for deletion)
    with switch_engine(UNPRIVILEGED):
        unpublish_repository(PG_MNT)
        assert get_published_info(PG_MNT, 'my_tag') is not None
