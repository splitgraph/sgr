import pytest

from splitgraph.core.repository import clone
from test.splitgraph.conftest import PG_MNT


@pytest.mark.parametrize("download_all", [True, False])
def test_pull(local_engine_empty, pg_repo_remote, download_all):
    # Pull the schema from the remote
    # Here, it's the pg on local_engine that connects to the remote engine, so we can use the actual hostname
    # (as opposed to the one exposed to us). However, the clone procedure also uses that connection string to talk to
    # the remote. Hence, there's an /etc/hosts indirection on the host mapping the remote engine to localhost.
    clone(pg_repo_remote, local_repository=PG_MNT, download_all=download_all)

    remote_head = pg_repo_remote.head
    PG_MNT.images.by_hash(remote_head.image_hash).checkout()

    # Do something to fruits on the remote
    pg_repo_remote.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    head_1 = pg_repo_remote.commit()

    # Canary to make sure everything got committed on the remote
    assert pg_repo_remote.diff('fruits', remote_head.image_hash, head_1, aggregate=False) \
           == [((3, 'mayonnaise'), 0, {'c': [], 'v': []})]

    # Check that the fruits table changed on the original repository
    assert pg_repo_remote.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

    # ...and check it's unchanged on the pulled one.
    assert PG_MNT.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange')]
    assert PG_MNT.images.by_hash(head_1.image_hash, raise_on_none=False) is None

    # Since the pull procedure initializes a new connection, we have to commit our changes
    # in order to see them.
    pg_repo_remote.engine.commit()
    PG_MNT.pull()
    head_1 = PG_MNT.images.by_hash(head_1.image_hash)

    # Check out the newly-pulled commit and verify it has the same data.
    head_1.checkout()

    assert PG_MNT.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
    assert PG_MNT.head == head_1


def test_pulls_with_lazy_object_downloads(local_engine_empty, pg_repo_remote):
    clone(pg_repo_remote, local_repository=PG_MNT, download_all=False)
    # Make sure we haven't downloaded anything until checkout
    assert not PG_MNT.objects.get_downloaded_objects()

    remote_head = pg_repo_remote.head

    PG_MNT.images.by_hash(remote_head.image_hash).checkout()
    assert len(PG_MNT.objects.get_downloaded_objects()) == 2  # Original fruits and vegetables tables.
    assert PG_MNT.objects.get_downloaded_objects() == PG_MNT.objects.get_existing_objects()

    # In the meantime, make two branches off of origin (a total of 3 commits)
    pg_repo_remote.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    left = pg_repo_remote.commit()

    remote_head.checkout()
    pg_repo_remote.run_sql("INSERT INTO fruits VALUES (3, 'mustard')")
    right = pg_repo_remote.commit()

    # Pull from upstream.
    PG_MNT.pull(download_all=False)
    # Make sure we have the pointers to the three versions of the fruits table + the original vegetables
    assert len(PG_MNT.objects.get_existing_objects()) == 4

    # Also make sure still only have the objects with the original fruits + vegetables tables
    assert len(PG_MNT.objects.get_downloaded_objects()) == 2

    # Check out left commit: since it only depends on the root, we should download just the new version of fruits.
    PG_MNT.images.by_hash(left.image_hash).checkout()

    assert len(PG_MNT.objects.get_downloaded_objects()) == 3  # now have 2 versions of fruits + 1 vegetables

    PG_MNT.images.by_hash(right.image_hash).checkout()
    assert len(PG_MNT.objects.get_downloaded_objects()) == 4  # now have 2 versions of fruits + 1 vegetables
    assert PG_MNT.objects.get_downloaded_objects() == PG_MNT.objects.get_existing_objects()


def test_push(local_engine_empty, pg_repo_remote):
    # Clone from the remote engine like in the previous test.
    clone(pg_repo_remote, local_repository=PG_MNT)

    remote_head = pg_repo_remote.head
    PG_MNT.images.by_hash(remote_head.image_hash).checkout()

    # Then, change our copy and commit.
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    head_1 = PG_MNT.commit()

    # Now, push to remote.
    PG_MNT.push(remote_repository=pg_repo_remote)

    # See if the original mountpoint got updated.
    assert len(pg_repo_remote.objects.get_existing_objects()) == 3

    pg_repo_remote.images.by_hash(head_1.image_hash).checkout()
    assert pg_repo_remote.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
