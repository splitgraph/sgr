import pytest
import tempfile

from splitgraph.commands import clone, checkout, commit, pull, push, unmount
from splitgraph.constants import PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB
from splitgraph.meta_handler import get_current_head, get_all_snap_parents, get_downloaded_objects, \
    get_existing_objects, get_external_object_locations
from test.splitgraph.conftest import PG_MNT


@pytest.mark.parametrize("download_all", [True, False])
def test_pull(sg_pg_conn, download_all):
    # This is going to get awkward: we're going to pull a schema from ourselves since there's only one
    # pgcache for now.
    clone(sg_pg_conn, '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB),
          PG_MNT, PG_MNT + '_pull', download_all=download_all)
    checkout(sg_pg_conn, PG_MNT + '_pull', get_current_head(sg_pg_conn, PG_MNT))

    # Do something to fruits
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")

    head_1 = commit(sg_pg_conn, PG_MNT)

    # Check that the fruits table changed on the original mount
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

    # ...and check it's unchanged on the pulled one.
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount_pull.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange')]
    assert head_1 not in [snap_id for snap_id, parent_id, created, comment in
                          get_all_snap_parents(sg_pg_conn, PG_MNT + '_pull')]

    # Since the pull procedure initializes a new connection, we have to commit our changes
    # in order to see them.
    sg_pg_conn.commit()
    pull(sg_pg_conn, PG_MNT + '_pull', remote='origin')

    # Check out the newly-pulled commit and verify it has the same data.
    checkout(sg_pg_conn, PG_MNT + '_pull', head_1)

    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount_pull.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
    assert get_current_head(sg_pg_conn, PG_MNT + '_pull') == head_1


@pytest.mark.xfail(reason="""This test currently doesn't make sense since we are pulling from ourselves and
all objects are shared between mountpoints -- so can't exercise us fetching objects only on checkout.""")
def test_pulls_with_lazy_object_downloads(sg_pg_conn):
    clone(sg_pg_conn, '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB),
          PG_MNT, PG_MNT + '_pull', download_all=False)
    # Make sure we haven't downloaded anything until checkout
    assert not get_downloaded_objects(sg_pg_conn)

    checkout(sg_pg_conn, PG_MNT + '_pull', get_current_head(sg_pg_conn, PG_MNT))
    assert len(get_downloaded_objects(sg_pg_conn)) == 2  # Original fruits and vegetables tables.
    assert get_downloaded_objects(sg_pg_conn) == get_existing_objects(sg_pg_conn, PG_MNT)

    # In the meantime, make two branches off of origin (a total of 3 commits)
    head = get_current_head(sg_pg_conn, PG_MNT)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")
    left = commit(sg_pg_conn, PG_MNT)

    checkout(sg_pg_conn, PG_MNT, head)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mustard')""")
    right = commit(sg_pg_conn, PG_MNT)

    # Pull from origin.
    pull(sg_pg_conn, PG_MNT + '_pull', remote='origin', download_all=False)
    # Make sure we have the pointers to the three versions of the fruits table + the original vegetables
    assert len(get_existing_objects(sg_pg_conn, PG_MNT + '_pull')) == 4
    assert get_existing_objects(sg_pg_conn, PG_MNT + '_pull') == get_existing_objects(sg_pg_conn, PG_MNT)
    # Also make sure still only have the objects with the original fruits + vegetables tables
    assert len(get_downloaded_objects(sg_pg_conn)) == 2

    # Check out left commit: since it only depends on the root, we should download just the new version of fruits.
    checkout(sg_pg_conn, PG_MNT + '_pull', left)
    assert len(
        get_downloaded_objects(sg_pg_conn)) == 3  # now have 2 versions of fruits + 1 vegetables

    checkout(sg_pg_conn, PG_MNT + '_pull', right)
    assert len(get_downloaded_objects(sg_pg_conn)) == 4
    # Only now we actually have all the objects materialized.
    assert get_downloaded_objects(sg_pg_conn) == get_existing_objects(sg_pg_conn, PG_MNT)


@pytest.mark.xfail(reason="""Not because we're connecting to ourselves but because when we enumerate existing objects,
we use the mountpoint as a key, so we think the object doesn't exist on the remote -- it actually exists
in splitgraph_meta, so there's a clash.""")
def test_push(sg_pg_conn):
    # First, clone from ourselves again like in the previous test.
    clone(sg_pg_conn, '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB),
          PG_MNT, PG_MNT + '_pull')
    checkout(sg_pg_conn, PG_MNT + '_pull', get_current_head(sg_pg_conn, PG_MNT))

    # Then, change our copy and commit.
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount_pull.fruits VALUES (3, 'mayonnaise')""")
    head_1 = commit(sg_pg_conn, PG_MNT + '_pull')
    # Since the pull procedure initializes a new connection, we have to commit our changes
    # in order to see them.
    sg_pg_conn.commit()

    # Now, push to remote.
    push(sg_pg_conn, '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB),
         PG_MNT, PG_MNT + '_pull')

    # Since we are connecting to ourselves, let's just reuse our connection to see if
    # the original mountpoint got updated.
    checkout(sg_pg_conn, PG_MNT, head_1)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]


@pytest.mark.xfail(reason="""Same reason as testing lazy pulls: we treat objects independently of mountpoints now
whereas the test used to sling them between mountpoints to test lazy downloads/checkouts.""")
def test_http_push_pull(sg_pg_conn):
    # Test pushing/pulling when the objects are uploaded to a remote storage instead of to the actual remote DB.
    # "Uploading" to a file for now because I can't seem to get uploading on an HTTP fixture working.

    remote_conn_string = '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB)
    clone(sg_pg_conn, remote_conn_string, PG_MNT, PG_MNT + '_pull', download_all=False)
    # Add a couple of commits, this time on the cloned copy.
    head = get_current_head(sg_pg_conn, PG_MNT)
    checkout(sg_pg_conn, PG_MNT + '_pull', head)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount_pull.fruits VALUES (3, 'mayonnaise')""")
    left = commit(sg_pg_conn, PG_MNT + '_pull')
    checkout(sg_pg_conn, PG_MNT + '_pull', head)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount_pull.fruits VALUES (3, 'mustard')""")
    right = commit(sg_pg_conn, PG_MNT + '_pull')

    with tempfile.TemporaryDirectory() as tmpdir:
        # Push to origin, but this time upload the actual objects instead.
        push(sg_pg_conn, remote_conn_string, PG_MNT, PG_MNT + '_pull', handler='FILE',
             handler_options={'path': tmpdir})

        # Check that the actual objects don't exist on the remote but are instead registered with an URL.
        assert len(get_downloaded_objects(sg_pg_conn)) == 2  # just the two original tables on the remote
        objects = get_existing_objects(sg_pg_conn, PG_MNT)
        assert len(objects) == 4  # two original tables + two new versions of fruits
        # Both repos have two non-local objects
        ext_objects_orig = get_external_object_locations(sg_pg_conn, list(objects))
        ext_objects_pull = get_external_object_locations(sg_pg_conn, list(objects))
        assert len(ext_objects_orig) == 2
        assert ext_objects_orig == ext_objects_pull

        # Destroy the pulled mountpoint and recreate it again.
        unmount(sg_pg_conn, PG_MNT + '_pull')
        clone(sg_pg_conn, remote_conn_string, PG_MNT, PG_MNT + '_pull', download_all=False)

        # Proceed as per the lazy checkout tests to make sure we don't download more than required.
        checkout(sg_pg_conn, PG_MNT + '_pull', head)

        assert len(get_existing_objects(sg_pg_conn, PG_MNT + '_pull')) == 4
        assert get_existing_objects(sg_pg_conn, PG_MNT + '_pull') == get_existing_objects(sg_pg_conn, PG_MNT)
        assert len(get_downloaded_objects(sg_pg_conn)) == 2

        checkout(sg_pg_conn, PG_MNT + '_pull', left)
        assert len(get_downloaded_objects(sg_pg_conn)) == 3  # now have 2 versions of fruits + 1 vegetables

        checkout(sg_pg_conn, PG_MNT + '_pull', right)
        assert len(get_downloaded_objects(sg_pg_conn)) == 4
        assert get_downloaded_objects(sg_pg_conn) == get_existing_objects(sg_pg_conn, PG_MNT)

        with sg_pg_conn.cursor() as cur:
            cur.execute("""SELECT * FROM %s.fruits""" % (PG_MNT + '_pull'))
            assert cur.fetchall() == [(1, 'apple'), (2, 'orange'), (3, 'mustard')]