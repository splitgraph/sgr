import tempfile

import pytest

from splitgraph.commands import clone, checkout, commit, pull, push, unmount
from splitgraph.commands.misc import cleanup_objects
from splitgraph.meta_handler.images import get_all_images_parents
from splitgraph.meta_handler.objects import get_existing_objects, get_downloaded_objects, get_external_object_locations
from splitgraph.meta_handler.tags import get_current_head
from test.splitgraph.conftest import PG_MNT, PG_MNT_PULL


@pytest.mark.parametrize("download_all", [True, False])
def test_pull(sg_pg_conn, remote_driver_conn, download_all):
    # Pull the schema from the remote
    # Here, it's the pg on cachedb that connects to the remote driver, so we can use the actual hostname
    # (as opposed to the one exposed to us). However, the clone procedure also uses that connection string to talk to
    # the remote. Hence, there's an /etc/hosts indirection on the host mapping the remote driver to localhost.
    clone(sg_pg_conn, PG_MNT, local_repository=PG_MNT_PULL, download_all=download_all)
    checkout(sg_pg_conn, PG_MNT_PULL, get_current_head(remote_driver_conn, PG_MNT))

    # Do something to fruits on the remote
    with remote_driver_conn.cursor() as cur:
        cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise')""")
    head_1 = commit(remote_driver_conn, PG_MNT)

    # Check that the fruits table changed on the original mount
    with remote_driver_conn.cursor() as cur:
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

    # ...and check it's unchanged on the pulled one.
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount_pull.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange')]
    assert head_1 not in [snapdata[0] for snapdata in
                          get_all_images_parents(sg_pg_conn, PG_MNT_PULL)]

    # Since the pull procedure initializes a new connection, we have to commit our changes
    # in order to see them.
    remote_driver_conn.commit()

    pull(sg_pg_conn, PG_MNT_PULL, remote='origin')

    # Check out the newly-pulled commit and verify it has the same data.
    checkout(sg_pg_conn, PG_MNT_PULL, head_1)

    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount_pull.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
    assert get_current_head(sg_pg_conn, PG_MNT_PULL) == head_1


@pytest.mark.parametrize("keep_downloaded", [True, False])
def test_pulls_with_lazy_object_downloads(empty_pg_conn, remote_driver_conn, keep_downloaded):
    clone(empty_pg_conn, PG_MNT, local_repository=PG_MNT_PULL, download_all=False)
    # Make sure we haven't downloaded anything until checkout
    assert not get_downloaded_objects(empty_pg_conn)

    checkout(empty_pg_conn, PG_MNT_PULL, get_current_head(remote_driver_conn, PG_MNT),
             keep_downloaded_objects=keep_downloaded)
    if keep_downloaded:
        assert len(get_downloaded_objects(empty_pg_conn)) == 2  # Original fruits and vegetables tables.
        assert get_downloaded_objects(empty_pg_conn) == get_existing_objects(empty_pg_conn)
    else:
        # If we're deleting remote objects after checkout, there shouldn't be any left.
        assert not get_downloaded_objects(empty_pg_conn)

    # In the meantime, make two branches off of origin (a total of 3 commits)
    head = get_current_head(remote_driver_conn, PG_MNT)
    with remote_driver_conn.cursor() as cur:
        cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise')""")
    left = commit(remote_driver_conn, PG_MNT)

    checkout(remote_driver_conn, PG_MNT, head)
    with remote_driver_conn.cursor() as cur:
        cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mustard')""")
    right = commit(remote_driver_conn, PG_MNT)

    # Pull from origin.
    pull(empty_pg_conn, PG_MNT_PULL, remote='origin', download_all=False)
    # Make sure we have the pointers to the three versions of the fruits table + the original vegetables
    assert len(get_existing_objects(empty_pg_conn)) == 4

    if keep_downloaded:
        # Also make sure still only have the objects with the original fruits + vegetables tables
        assert len(get_downloaded_objects(empty_pg_conn)) == 2

    # Check out left commit: since it only depends on the root, we should download just the new version of fruits.
    checkout(empty_pg_conn, PG_MNT_PULL, left, keep_downloaded_objects=keep_downloaded)

    if keep_downloaded:
        assert len(get_downloaded_objects(empty_pg_conn)) == 3  # now have 2 versions of fruits + 1 vegetables
    else:
        assert not get_downloaded_objects(empty_pg_conn)

    checkout(empty_pg_conn, PG_MNT_PULL, right, keep_downloaded_objects=keep_downloaded)
    if keep_downloaded:
        assert len(get_downloaded_objects(empty_pg_conn)) == 4  # now have 2 versions of fruits + 1 vegetables
        assert get_downloaded_objects(empty_pg_conn) == get_existing_objects(empty_pg_conn)
    else:
        assert not get_downloaded_objects(empty_pg_conn)


def test_push(empty_pg_conn, remote_driver_conn):
    # Clone from the remote driver like in the previous test.
    clone(empty_pg_conn, PG_MNT, local_repository=PG_MNT_PULL)
    head = get_current_head(remote_driver_conn, PG_MNT)
    checkout(empty_pg_conn, PG_MNT_PULL, head)

    # Then, change our copy and commit.
    with empty_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount_pull.fruits VALUES (3, 'mayonnaise')""")
    head_1 = commit(empty_pg_conn, PG_MNT_PULL)

    # Now, push to remote.
    push(empty_pg_conn, PG_MNT_PULL, remote_repository=PG_MNT)

    # See if the original mountpoint got updated.
    checkout(remote_driver_conn, PG_MNT, head_1)
    with remote_driver_conn.cursor() as cur:
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]


def test_http_push_pull(empty_pg_conn, remote_driver_conn):
    # Test pushing/pulling when the objects are uploaded to a remote storage instead of to the actual remote DB.
    # "Uploading" to a file for now because I can't seem to get uploading on an HTTP fixture working.

    clone(empty_pg_conn, PG_MNT, local_repository=PG_MNT_PULL, download_all=False)
    # Add a couple of commits, this time on the cloned copy.
    head = get_current_head(remote_driver_conn, PG_MNT)
    checkout(empty_pg_conn, PG_MNT_PULL, head)
    with empty_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount_pull.fruits VALUES (3, 'mayonnaise')""")
    left = commit(empty_pg_conn, PG_MNT_PULL)
    checkout(empty_pg_conn, PG_MNT_PULL, head)
    with empty_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount_pull.fruits VALUES (3, 'mustard')""")
    right = commit(empty_pg_conn, PG_MNT_PULL)

    with tempfile.TemporaryDirectory() as tmpdir:
        # Push to origin, but this time upload the actual objects instead.
        push(empty_pg_conn, PG_MNT_PULL, remote_repository=PG_MNT, handler='FILE',
             handler_options={'path': tmpdir})

        # Check that the actual objects don't exist on the remote but are instead registered with an URL.
        # All the objects on pgcache were registered remotely
        objects = get_existing_objects(remote_driver_conn)
        local_objects = get_existing_objects(empty_pg_conn)
        assert all(o in objects for o in local_objects)
        # Two non-local objects in pgcache, both registered as non-local on the remote driver.
        ext_objects_orig = get_external_object_locations(empty_pg_conn, list(objects))
        ext_objects_pull = get_external_object_locations(remote_driver_conn, list(objects))
        assert len(ext_objects_orig) == 2
        assert all(e in ext_objects_pull for e in ext_objects_orig)

        # Destroy the pulled mountpoint and recreate it again.
        assert len(get_downloaded_objects(empty_pg_conn)) == 4
        unmount(empty_pg_conn, PG_MNT_PULL)
        # Make sure we don't have any leftover physical objects.
        cleanup_objects(empty_pg_conn)
        assert len(get_downloaded_objects(empty_pg_conn)) == 0

        clone(empty_pg_conn, PG_MNT, local_repository=PG_MNT_PULL, download_all=False)

        # Proceed as per the lazy checkout tests to make sure we don't download more than required.
        # Make sure we still haven't downloaded anything.
        assert len(get_downloaded_objects(empty_pg_conn)) == 0

        # Check out left commit: since it only depends on the root, we should download just the new version of fruits.
        checkout(empty_pg_conn, PG_MNT_PULL, left)
        assert len(
            get_downloaded_objects(empty_pg_conn)) == 3  # now have 2 versions of fruits + 1 vegetables

        checkout(empty_pg_conn, PG_MNT_PULL, right)
        assert len(get_downloaded_objects(empty_pg_conn)) == 4
        # Only now we actually have all the objects materialized.
        assert get_downloaded_objects(empty_pg_conn) == get_existing_objects(empty_pg_conn)
