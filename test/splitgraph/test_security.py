import psycopg2
import pytest
from psycopg2._psycopg import ProgrammingError
from psycopg2.sql import SQL, Identifier
from test.splitgraph.conftest import REMOTE_NAMESPACE

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core.common import META_TABLES
from splitgraph.core.repository import Repository, clone
from splitgraph.core.sql import select


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
    # Test we can delete a single image from our own repo
    assert len(remote_destination.images()) == 3
    remote_destination.images.delete([destination.images["latest"].image_hash])
    assert len(remote_destination.images()) == 2

    # Test we can delete our own repo once we've pushed it
    remote_destination.delete()
    assert len(remote_destination.images()) == 0


@pytest.mark.registry
def test_api_funcs(readonly_pg_repo):
    # Test some image-reading API functions work over non-superuser connections.
    readonly_pg_repo.images["latest"].get_size()

    readonly_pg_repo.images["latest"].get_table("fruits").get_size()


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

    with pytest.raises(ProgrammingError) as e:
        readonly_pg_repo.images.delete([readonly_pg_repo.images["latest"].image_hash])

    # Check the repository still exists on the remote.
    assert len(readonly_pg_repo.images()) > 0


@pytest.mark.registry
def test_overwrite_own_object_meta(unprivileged_pg_repo):
    fruits = unprivileged_pg_repo.images["latest"].get_table("fruits")
    object_meta = unprivileged_pg_repo.objects.get_object_meta(fruits.objects)[fruits.objects[0]]

    object_meta = object_meta._replace(size=12345)
    unprivileged_pg_repo.objects.register_objects([object_meta])

    object_meta = unprivileged_pg_repo.objects.get_object_meta(fruits.objects)[fruits.objects[0]]
    assert object_meta.size == 12345


@pytest.mark.registry
def test_overwrite_other_object_meta(readonly_pg_repo):
    fruits = readonly_pg_repo.images["latest"].get_table("fruits")
    object_meta = readonly_pg_repo.objects.get_object_meta(fruits.objects)[fruits.objects[0]]

    object_meta = object_meta._replace(size=12345)

    with pytest.raises(ProgrammingError) as e:
        readonly_pg_repo.objects.register_objects([object_meta])
    assert "You do not have access to this namespace!" in str(e.value)
    object_meta = readonly_pg_repo.objects.get_object_meta(fruits.objects)[fruits.objects[0]]
    assert object_meta.size != 12345

    with pytest.raises(ProgrammingError) as e:
        readonly_pg_repo.objects.register_objects([object_meta], namespace=REMOTE_NAMESPACE)
    assert "You do not have access to this namespace!" in str(e.value)


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
def test_no_direct_table_access(unprivileged_pg_repo):
    # Canary to check users can't manipulate splitgraph_meta tables directly
    for table in META_TABLES:
        with pytest.raises(psycopg2.Error) as e:
            unprivileged_pg_repo.engine.run_sql(select(table, "1"))

        with pytest.raises(psycopg2.Error) as e:
            unprivileged_pg_repo.engine.run_sql(
                SQL("DELETE FROM {}.{} WHERE 1 = 2").format(
                    Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table)
                )
            )
