import pytest
from psycopg2._psycopg import ProgrammingError
from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core.common import META_TABLES, select
from splitgraph.core.registry import get_published_info, unpublish_repository
from splitgraph.core.repository import Repository, clone


@pytest.mark.registry
def test_pull_public(local_engine_empty, readonly_pg_repo):
    clone(readonly_pg_repo)


@pytest.mark.registry
def test_push_own_delete_own(local_engine_empty, unprivileged_pg_repo):
    destination = Repository.from_template(unprivileged_pg_repo, engine=local_engine_empty)
    clone(unprivileged_pg_repo, local_repository=destination)

    destination.images["latest"].checkout()
    destination.run_sql("""UPDATE fruits SET name = 'banana' WHERE fruit_id = 1""")
    destination.commit()

    # Test we can push to our namespace -- can't upload the object to the splitgraph_meta since we can't create
    # tables there
    remote_destination = Repository.from_template(
        destination,
        namespace=unprivileged_pg_repo.engine.conn_params["SG_NAMESPACE"],
        engine=unprivileged_pg_repo.engine,
    )
    destination.upstream = remote_destination

    destination.push(handler="S3")
    # Test we can delete our own repo once we've pushed it
    remote_destination.delete(uncheckout=False)
    assert len(remote_destination.images()) == 0


@pytest.mark.registry
def test_push_own_delete_own_different_namespaces(local_engine_empty, readonly_pg_repo):
    # Same as previous but we clone the read-only repo and push to our own namespace
    # to check that the objects we push get their namespaces rewritten to be the unprivileged user, not test.
    destination = clone(readonly_pg_repo)

    destination.images["latest"].checkout()
    destination.run_sql("""UPDATE fruits SET name = 'banana' WHERE fruit_id = 1""")
    destination.commit()

    remote_destination = Repository.from_template(
        readonly_pg_repo,
        namespace=readonly_pg_repo.engine.conn_params["SG_NAMESPACE"],
        engine=readonly_pg_repo.engine,
    )
    destination.upstream = remote_destination

    destination.push(handler="S3")

    object_id = destination.head.get_table("fruits").objects[-1]
    assert (
        remote_destination.objects.get_object_meta([object_id])[object_id].namespace
        == readonly_pg_repo.engine.conn_params["SG_NAMESPACE"]
    )

    # Test we can delete our own repo once we've pushed it
    remote_destination.delete(uncheckout=False)
    assert len(remote_destination.images()) == 0


@pytest.mark.registry
def test_push_others(local_engine_empty, readonly_pg_repo):
    destination = clone(readonly_pg_repo)
    destination.images["latest"].checkout()
    destination.run_sql("""UPDATE fruits SET name = 'banana' WHERE fruit_id = 1""")
    destination.commit()

    with pytest.raises(ProgrammingError) as e:
        destination.push(remote_repository=readonly_pg_repo, handler="S3")
    assert "You do not have access to this namespace!" in str(e.value)


@pytest.mark.registry
def test_delete_others(readonly_pg_repo):
    with pytest.raises(ProgrammingError) as e:
        readonly_pg_repo.delete(uncheckout=False)
    assert "You do not have access to this namespace!" in str(e.value)

    # Check the repository still exists on the remote.
    assert len(readonly_pg_repo.images()) > 0


@pytest.mark.registry
def test_impersonate_external_object(readonly_pg_repo):
    sample_object = readonly_pg_repo.images["latest"].get_table("fruits").objects[0]
    assert sample_object is not None

    # Try to impersonate the owner of the "otheruser" namespace and add a different external link to
    # an object that they own.
    with pytest.raises(ProgrammingError) as e:
        readonly_pg_repo.objects.register_object_locations([(sample_object, "fake_location", "S3")])
    assert "You do not have access to this namespace!" in str(e.value)


@pytest.mark.registry
def test_publish_unpublish_own(local_engine_empty, readonly_pg_repo, unprivileged_remote_engine):
    destination = clone(readonly_pg_repo, download_all=True)
    destination.images["latest"].tag("my_tag")
    target_repo = Repository.from_template(
        readonly_pg_repo,
        namespace=unprivileged_remote_engine.conn_params["SG_NAMESPACE"],
        engine=unprivileged_remote_engine,
    )

    destination.push(remote_repository=target_repo)

    destination.publish(
        "my_tag",
        remote_repository=target_repo,
        readme="my_readme",
        include_provenance=True,
        include_table_previews=True,
    )
    assert get_published_info(target_repo, "my_tag") is not None
    unpublish_repository(target_repo)
    assert get_published_info(target_repo, "my_tag") is None


@pytest.mark.registry
def test_publish_unpublish_others(local_engine_empty, remote_engine_registry, readonly_pg_repo):
    # Tag the remote repo as an admin user and try to publish as an unprivileged one
    readonly_pg_repo_superuser = Repository.from_template(
        readonly_pg_repo, engine=remote_engine_registry
    )
    readonly_pg_repo_superuser.images["latest"].tag("my_tag")
    readonly_pg_repo_superuser.commit_engines()

    destination = clone(readonly_pg_repo, download_all=True)

    # Publish into the "test" namespace as someone who doesn't have access to it.
    with pytest.raises(ProgrammingError) as e:
        destination.publish("my_tag", remote_repository=readonly_pg_repo, readme="my_readme")
    assert "You do not have access to this namespace!" in str(e.value)

    # Publish as the admin user
    destination.publish("my_tag", remote_repository=readonly_pg_repo_superuser, readme="my_readme")

    # Try to delete as the remote user -- should fail
    with pytest.raises(ProgrammingError) as e:
        unpublish_repository(readonly_pg_repo)
    assert "You do not have access to this namespace!" in str(e.value)
    assert get_published_info(readonly_pg_repo, "my_tag") is not None


@pytest.mark.registry
def test_no_direct_table_access(unprivileged_pg_repo):
    # Canary to check users can't manipulate splitgraph_meta tables directly
    for table in META_TABLES:
        with pytest.raises(ProgrammingError) as e:
            unprivileged_pg_repo.engine.run_sql(select(table, "1"))
        assert "permission denied for table" in str(e.value)

        with pytest.raises(ProgrammingError) as e:
            unprivileged_pg_repo.engine.run_sql(
                SQL("DELETE FROM {}.{} WHERE 1 = 2").format(
                    Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table)
                )
            )
        assert "permission denied for table" in str(e.value)
