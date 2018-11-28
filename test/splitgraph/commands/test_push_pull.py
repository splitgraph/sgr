import pytest

from splitgraph._data.images import get_all_images_parents
from splitgraph._data.objects import get_existing_objects, get_downloaded_objects
from splitgraph.commands import clone, checkout, commit, pull, push
from splitgraph.commands.tagging import get_current_head
from splitgraph.connection import override_driver_connection
from test.splitgraph.conftest import PG_MNT, PG_MNT_PULL


@pytest.mark.parametrize("download_all", [True, False])
def test_pull(sg_pg_conn, remote_driver_conn, download_all):
    # Pull the schema from the remote
    # Here, it's the pg on cachedb that connects to the remote driver, so we can use the actual hostname
    # (as opposed to the one exposed to us). However, the clone procedure also uses that connection string to talk to
    # the remote. Hence, there's an /etc/hosts indirection on the host mapping the remote driver to localhost.
    clone(PG_MNT, local_repository=PG_MNT_PULL, download_all=download_all)

    with override_driver_connection(remote_driver_conn):
        remote_head = get_current_head(PG_MNT)
    checkout(PG_MNT_PULL, remote_head)

    # Do something to fruits on the remote
    with remote_driver_conn.cursor() as cur:
        cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise')""")
    with override_driver_connection(remote_driver_conn):
        head_1 = commit(PG_MNT)

    # Check that the fruits table changed on the original mount
    with remote_driver_conn.cursor() as cur:
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

    # ...and check it's unchanged on the pulled one.
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount_pull.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange')]
    assert head_1 not in [snapdata[0] for snapdata in
                          get_all_images_parents(PG_MNT_PULL)]

    # Since the pull procedure initializes a new connection, we have to commit our changes
    # in order to see them.
    remote_driver_conn.commit()

    pull(PG_MNT_PULL, remote='origin')

    # Check out the newly-pulled commit and verify it has the same data.
    checkout(PG_MNT_PULL, head_1)

    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount_pull.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
    assert get_current_head(PG_MNT_PULL) == head_1


@pytest.mark.parametrize("keep_downloaded", [True, False])
def test_pulls_with_lazy_object_downloads(empty_pg_conn, remote_driver_conn, keep_downloaded):
    clone(PG_MNT, local_repository=PG_MNT_PULL, download_all=False)
    # Make sure we haven't downloaded anything until checkout
    assert not get_downloaded_objects()

    with override_driver_connection(remote_driver_conn):
        head = get_current_head(PG_MNT)

    checkout(PG_MNT_PULL, head, keep_downloaded_objects=keep_downloaded)
    if keep_downloaded:
        assert len(get_downloaded_objects()) == 2  # Original fruits and vegetables tables.
        assert get_downloaded_objects() == get_existing_objects()
    else:
        # If we're deleting remote objects after checkout, there shouldn't be any left.
        assert not get_downloaded_objects()

    # In the meantime, make two branches off of origin (a total of 3 commits)
    with override_driver_connection(remote_driver_conn):
        with remote_driver_conn.cursor() as cur:
            cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise')""")
        left = commit(PG_MNT)

        checkout(PG_MNT, head)
        with remote_driver_conn.cursor() as cur:
            cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mustard')""")
        right = commit(PG_MNT)

    # Pull from origin.
    pull(PG_MNT_PULL, remote='origin', download_all=False)
    # Make sure we have the pointers to the three versions of the fruits table + the original vegetables
    assert len(get_existing_objects()) == 4

    if keep_downloaded:
        # Also make sure still only have the objects with the original fruits + vegetables tables
        assert len(get_downloaded_objects()) == 2

    # Check out left commit: since it only depends on the root, we should download just the new version of fruits.
    checkout(PG_MNT_PULL, left, keep_downloaded_objects=keep_downloaded)

    if keep_downloaded:
        assert len(get_downloaded_objects()) == 3  # now have 2 versions of fruits + 1 vegetables
    else:
        assert not get_downloaded_objects()

    checkout(PG_MNT_PULL, right, keep_downloaded_objects=keep_downloaded)
    if keep_downloaded:
        assert len(get_downloaded_objects()) == 4  # now have 2 versions of fruits + 1 vegetables
        assert get_downloaded_objects() == get_existing_objects()
    else:
        assert not get_downloaded_objects()


def test_push(empty_pg_conn, remote_driver_conn):
    # Clone from the remote driver like in the previous test.
    clone(PG_MNT, local_repository=PG_MNT_PULL)

    with override_driver_connection(remote_driver_conn):
        head = get_current_head(PG_MNT)
    checkout(PG_MNT_PULL, head)

    # Then, change our copy and commit.
    with empty_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount_pull.fruits VALUES (3, 'mayonnaise')""")
    head_1 = commit(PG_MNT_PULL)

    # Now, push to remote.
    push(PG_MNT_PULL, remote_repository=PG_MNT)

    # See if the original mountpoint got updated.
    with override_driver_connection(remote_driver_conn):
        checkout(PG_MNT, head_1)
    with remote_driver_conn.cursor() as cur:
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]


