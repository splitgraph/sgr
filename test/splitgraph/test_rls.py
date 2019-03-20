from copy import copy

import pytest
from psycopg2._psycopg import ProgrammingError
from splitgraph.core.registry import get_published_info, unpublish_repository, toggle_registry_rls
from splitgraph.core.repository import Repository, clone
from splitgraph.engine import get_engine
from test.splitgraph.conftest import PG_MNT, REMOTE_ENGINE

UNPRIVILEGED = 'unprivileged_remote_engine'


def _init_rls_test(remote_engine):
    toggle_registry_rls(remote_engine, 'ENABLE')
    remote_engine.commit()


@pytest.fixture()
def unprivileged_remote_engine(remote_engine):
    """Temporarily adds an unprivileged remote engine"""
    _init_rls_test(remote_engine)
    from splitgraph.engine import CONFIG
    CONFIG['remotes'][UNPRIVILEGED] = copy(CONFIG['remotes'][REMOTE_ENGINE])
    CONFIG['remotes'][UNPRIVILEGED]['SG_ENGINE_USER'] = 'testuser'
    CONFIG['remotes'][UNPRIVILEGED]['SG_ENGINE_PWD'] = 'testpassword'
    E = get_engine(UNPRIVILEGED)
    try:
        assert E.conn_params[2] == 'testuser'
        assert E.conn_params[3] == 'testpassword'
        yield E
    finally:
        E.rollback()
        E.close()
        del CONFIG['remotes'][UNPRIVILEGED]


@pytest.fixture()
def unprivileged_pg_repo(pg_repo_remote, unprivileged_remote_engine):
    return Repository.from_template(pg_repo_remote, engine=unprivileged_remote_engine)


def test_rls_pull_public(local_engine_empty, unprivileged_pg_repo):
    clone(unprivileged_pg_repo)


def test_rls_push_own_delete_own(local_engine_empty, unprivileged_pg_repo, clean_minio):
    destination = Repository(namespace='testuser', repository='pg_mount')
    clone(unprivileged_pg_repo, local_repository=destination)

    destination.images['latest'].checkout()
    destination.run_sql("""UPDATE fruits SET name = 'banana' WHERE fruit_id = 1""")
    destination.commit()

    # Test we can push to our namespace -- can't upload the object to the splitgraph_meta since we can't create
    # tables there
    remote_destination = Repository.from_template(destination, engine=unprivileged_pg_repo.engine)
    destination.upstream = remote_destination

    destination.push(handler='S3')
    # Test we can delete our own repo once we've pushed it
    remote_destination.delete(uncheckout=False)
    assert len(remote_destination.images()) == 0


def test_rls_push_own_delete_own_different_namespaces(local_engine_empty, unprivileged_pg_repo, clean_minio):
    # Same as previous but we clone into test/pg_mount and push to our own namespace
    # to check that the objects we push get their namespaces rewritten to be testuser, not test.
    destination = clone(unprivileged_pg_repo)

    destination.images['latest'].checkout()
    destination.run_sql("""UPDATE fruits SET name = 'banana' WHERE fruit_id = 1""")
    destination.commit()

    remote_destination = Repository('testuser', 'pg_mount', unprivileged_pg_repo.engine)
    destination.upstream = remote_destination

    destination.push(handler='S3')
    # Test we can delete our own repo once we've pushed it
    remote_destination.delete(uncheckout=False)
    assert len(remote_destination.images()) == 0


def test_rls_push_others(local_engine_empty, unprivileged_pg_repo):
    clone(unprivileged_pg_repo)
    PG_MNT.images['latest'].checkout()
    PG_MNT.run_sql("""UPDATE fruits SET name = 'banana' WHERE fruit_id = 1""")
    PG_MNT.commit()

    with pytest.raises(ProgrammingError) as e:
        PG_MNT.push(remote_repository=unprivileged_pg_repo)
    assert 'new row violates row-level security policy for table "images"' in str(e.value)


def test_rls_delete_others(unprivileged_pg_repo, unprivileged_remote_engine):
    # RLS doesn't actually raise an error for this, since it just appends the policy qualifier to the query.
    # Hence in this case this simply does nothing (the rows in "test" namespace aren't available for deletion).

    unprivileged_pg_repo.delete(uncheckout=False)
    # Check that the policy worked by verifying that the repository still exists on the remote.
    assert len(unprivileged_pg_repo.images()) > 0


def test_rls_impersonate_external_object(unprivileged_pg_repo, unprivileged_remote_engine):
    sample_object = unprivileged_pg_repo.images['latest'].get_table('fruits').objects[0][0]
    assert sample_object is not None

    # Try to impersonate the owner of the "test" namespace and add a different external link to
    # an object that they own.
    with pytest.raises(ProgrammingError) as e:
        unprivileged_pg_repo.objects.register_object_locations([(sample_object, 'fake_location', 'S3')])
    assert 'new row violates row-level security policy for table "object_locations"' in str(e.value)


def test_rls_publish_unpublish_own(local_engine_empty, pg_repo_remote, unprivileged_remote_engine):
    clone(pg_repo_remote, download_all=True)
    PG_MNT.images['latest'].tag('my_tag')
    target_repo = Repository(namespace='testuser', repository='pg_mount', engine=unprivileged_remote_engine)

    PG_MNT.push(remote_repository=target_repo)

    PG_MNT.publish('my_tag', remote_repository=target_repo,
                   readme="my_readme", include_provenance=True, include_table_previews=True)

    assert get_published_info(target_repo, 'my_tag') is not None
    unpublish_repository(target_repo)
    assert get_published_info(target_repo, 'my_tag') is None


def test_rls_publish_unpublish_others(local_engine_empty, pg_repo_remote, unprivileged_pg_repo,
                                      unprivileged_remote_engine):
    # Tag the remote repo as an admin user and try to publish as an unprivileged one
    pg_repo_remote.images['latest'].tag('my_tag')
    pg_repo_remote.engine.commit()
    clone(unprivileged_pg_repo, download_all=True)

    # Publish into the "test" namespace as someone who doesn't have access to it.
    with pytest.raises(ProgrammingError) as e:
        PG_MNT.publish('my_tag', remote_repository=unprivileged_pg_repo, readme="my_readme")
    assert 'new row violates row-level security policy for table "images"' in str(e.value)

    # Publish as the admin user
    PG_MNT.publish('my_tag', remote_repository=pg_repo_remote, readme="my_readme")

    # Try to delete as the remote user -- should fail (no error raised since the RLS just doesn't make
    # those rows available for deletion)
    unpublish_repository(unprivileged_pg_repo)
    assert get_published_info(unprivileged_pg_repo, 'my_tag') is not None
