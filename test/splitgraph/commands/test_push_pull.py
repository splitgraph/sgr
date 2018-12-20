import pytest

from splitgraph._data.images import get_all_image_info
from splitgraph._data.objects import get_existing_objects, get_downloaded_objects
from splitgraph.commands import clone
from splitgraph.engine import switch_engine
from test.splitgraph.conftest import PG_MNT, PG_MNT_PULL, REMOTE_ENGINE


@pytest.mark.parametrize("download_all", [True, False])
def test_pull(local_engine_with_pg, remote_engine, download_all):
    # Pull the schema from the remote
    # Here, it's the pg on cachedb that connects to the remote engine, so we can use the actual hostname
    # (as opposed to the one exposed to us). However, the clone procedure also uses that connection string to talk to
    # the remote. Hence, there's an /etc/hosts indirection on the host mapping the remote engine to localhost.
    clone(PG_MNT, local_repository=PG_MNT_PULL, download_all=download_all)

    with switch_engine(REMOTE_ENGINE):
        remote_head = PG_MNT.get_head()
    PG_MNT_PULL.checkout(remote_head)

    # Do something to fruits on the remote
    remote_engine.run_sql("INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')")
    with switch_engine(REMOTE_ENGINE):
        head_1 = PG_MNT.commit()

    # Check that the fruits table changed on the original mount
    assert remote_engine.run_sql("SELECT * FROM \"test/pg_mount\".fruits") == \
           [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

    # ...and check it's unchanged on the pulled one.
    assert local_engine_with_pg.run_sql("SELECT * FROM test_pg_mount_pull.fruits") == [(1, 'apple'), (2, 'orange')]
    assert head_1 not in [snapdata[0] for snapdata in
                          get_all_image_info(PG_MNT_PULL)]

    # Since the pull procedure initializes a new connection, we have to commit our changes
    # in order to see them.
    remote_engine.commit()

    PG_MNT_PULL.pull()
    assert head_1 in [snapdata[0] for snapdata in get_all_image_info(PG_MNT_PULL)]

    # Check out the newly-pulled commit and verify it has the same data.
    PG_MNT_PULL.checkout(head_1)

    assert PG_MNT_PULL.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
    assert PG_MNT_PULL.get_head() == head_1


@pytest.mark.parametrize("keep_downloaded", [True, False])
def test_pulls_with_lazy_object_downloads(local_engine_empty, remote_engine, keep_downloaded):
    clone(PG_MNT, local_repository=PG_MNT_PULL, download_all=False)
    # Make sure we haven't downloaded anything until checkout
    assert not get_downloaded_objects()

    with switch_engine(REMOTE_ENGINE):
        head = PG_MNT.get_head()

    PG_MNT_PULL.checkout(head, keep_downloaded_objects=keep_downloaded)
    if keep_downloaded:
        assert len(get_downloaded_objects()) == 2  # Original fruits and vegetables tables.
        assert get_downloaded_objects() == get_existing_objects()
    else:
        # If we're deleting remote objects after checkout, there shouldn't be any left.
        assert not get_downloaded_objects()

    # In the meantime, make two branches off of origin (a total of 3 commits)
    with switch_engine(REMOTE_ENGINE):
        remote_engine.run_sql("INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')")
        left = PG_MNT.commit()

        PG_MNT.checkout(head)
        remote_engine.run_sql("INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mustard')")
        right = PG_MNT.commit()

    # Pull from upstream.
    PG_MNT_PULL.pull(download_all=False)
    # Make sure we have the pointers to the three versions of the fruits table + the original vegetables
    assert len(get_existing_objects()) == 4

    if keep_downloaded:
        # Also make sure still only have the objects with the original fruits + vegetables tables
        assert len(get_downloaded_objects()) == 2

    # Check out left commit: since it only depends on the root, we should download just the new version of fruits.
    PG_MNT_PULL.checkout(left, keep_downloaded_objects=keep_downloaded)

    if keep_downloaded:
        assert len(get_downloaded_objects()) == 3  # now have 2 versions of fruits + 1 vegetables
    else:
        assert not get_downloaded_objects()

    PG_MNT_PULL.checkout(right, keep_downloaded_objects=keep_downloaded)
    if keep_downloaded:
        assert len(get_downloaded_objects()) == 4  # now have 2 versions of fruits + 1 vegetables
        assert get_downloaded_objects() == get_existing_objects()
    else:
        assert not get_downloaded_objects()


def test_push(local_engine_empty, remote_engine):
    # Clone from the remote engine like in the previous test.
    clone(PG_MNT, local_repository=PG_MNT_PULL)

    with switch_engine(REMOTE_ENGINE):
        head = PG_MNT.get_head()
    PG_MNT_PULL.checkout(head)

    # Then, change our copy and commit.
    PG_MNT_PULL.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    head_1 = PG_MNT_PULL.commit()

    # Now, push to remote.
    PG_MNT_PULL.push(remote_repository=PG_MNT)

    # See if the original mountpoint got updated.
    with switch_engine(REMOTE_ENGINE):
        PG_MNT.checkout(head_1)
    assert remote_engine.run_sql("SELECT * FROM \"test/pg_mount\".fruits") == \
           [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
