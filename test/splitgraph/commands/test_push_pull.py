import pytest

from splitgraph._data.images import get_all_image_info
from splitgraph._data.objects import get_existing_objects, get_downloaded_objects
from splitgraph.core.repository import clone
from test.splitgraph.conftest import PG_MNT


@pytest.mark.parametrize("download_all", [True, False])
def test_pull(local_engine_empty, pg_repo_remote, download_all):
    # Pull the schema from the remote
    # Here, it's the pg on local_engine that connects to the remote engine, so we can use the actual hostname
    # (as opposed to the one exposed to us). However, the clone procedure also uses that connection string to talk to
    # the remote. Hence, there's an /etc/hosts indirection on the host mapping the remote engine to localhost.
    clone(pg_repo_remote, local_repository=PG_MNT, download_all=download_all)

    remote_head = pg_repo_remote.get_head()
    PG_MNT.checkout(remote_head)

    # Do something to fruits on the remote
    pg_repo_remote.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    head_1 = pg_repo_remote.commit()

    # Check that the fruits table changed on the original repository
    assert pg_repo_remote.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

    # ...and check it's unchanged on the pulled one.
    assert PG_MNT.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange')]
    assert head_1 not in [snapdata[0] for snapdata in
                          get_all_image_info(PG_MNT)]

    # Since the pull procedure initializes a new connection, we have to commit our changes
    # in order to see them.
    pg_repo_remote.engine.commit()
    PG_MNT.pull()
    assert head_1 in [snapdata[0] for snapdata in get_all_image_info(PG_MNT)]

    # Check out the newly-pulled commit and verify it has the same data.
    PG_MNT.checkout(head_1)

    assert PG_MNT.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
    assert PG_MNT.get_head() == head_1


@pytest.mark.parametrize("keep_downloaded", [True, False])
def test_pulls_with_lazy_object_downloads(local_engine_empty, pg_repo_remote, keep_downloaded):
    clone(pg_repo_remote, local_repository=PG_MNT, download_all=False)
    # Make sure we haven't downloaded anything until checkout
    assert not get_downloaded_objects()

    head = pg_repo_remote.get_head()

    PG_MNT.checkout(head, keep_downloaded_objects=keep_downloaded)
    if keep_downloaded:
        assert len(get_downloaded_objects()) == 2  # Original fruits and vegetables tables.
        assert get_downloaded_objects() == get_existing_objects()
    else:
        # If we're deleting remote objects after checkout, there shouldn't be any left.
        assert not get_downloaded_objects()

    # In the meantime, make two branches off of origin (a total of 3 commits)
    pg_repo_remote.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    left = pg_repo_remote.commit()

    pg_repo_remote.checkout(head)
    pg_repo_remote.run_sql("INSERT INTO fruits VALUES (3, 'mustard')")
    right = pg_repo_remote.commit()

    # Pull from upstream.
    PG_MNT.pull(download_all=False)
    # Make sure we have the pointers to the three versions of the fruits table + the original vegetables
    assert len(get_existing_objects()) == 4

    if keep_downloaded:
        # Also make sure still only have the objects with the original fruits + vegetables tables
        assert len(get_downloaded_objects()) == 2

    # Check out left commit: since it only depends on the root, we should download just the new version of fruits.
    PG_MNT.checkout(left, keep_downloaded_objects=keep_downloaded)

    if keep_downloaded:
        assert len(get_downloaded_objects()) == 3  # now have 2 versions of fruits + 1 vegetables
    else:
        assert not get_downloaded_objects()

    PG_MNT.checkout(right, keep_downloaded_objects=keep_downloaded)
    if keep_downloaded:
        assert len(get_downloaded_objects()) == 4  # now have 2 versions of fruits + 1 vegetables
        assert get_downloaded_objects() == get_existing_objects()
    else:
        assert not get_downloaded_objects()


def test_push(local_engine_empty, pg_repo_remote):
    # Clone from the remote engine like in the previous test.
    clone(pg_repo_remote, local_repository=PG_MNT)

    head = pg_repo_remote.get_head()
    PG_MNT.checkout(head)

    # Then, change our copy and commit.
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    head_1 = PG_MNT.commit()

    # Now, push to remote.
    PG_MNT.push(remote_repository=pg_repo_remote)

    # See if the original mountpoint got updated.
    pg_repo_remote.checkout(head_1)
    assert pg_repo_remote.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
