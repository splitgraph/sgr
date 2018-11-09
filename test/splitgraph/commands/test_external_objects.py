import pytest
from minio import Minio
from splitgraph.commands import clone, checkout, commit, push, unmount
from splitgraph.commands.misc import cleanup_objects
from splitgraph.constants import S3_HOST, S3_PORT, S3_SECRET_KEY, S3_ACCESS_KEY
from splitgraph.meta_handler.objects import get_existing_objects, get_external_object_locations, get_downloaded_objects
from splitgraph.meta_handler.tags import get_current_head
from test.splitgraph.conftest import PG_MNT, PG_MNT_PULL


def _cleanup_minio():
    client = Minio('%s:%s' % (S3_HOST, S3_PORT),
                   access_key=S3_ACCESS_KEY,
                   secret_key=S3_SECRET_KEY,
                   secure=False)
    objects = [o.object_name for o in client.list_objects(bucket_name=S3_ACCESS_KEY)]
    # remove_objects is an iterator, so we force evaluate it
    list(client.remove_objects(bucket_name=S3_ACCESS_KEY, objects_iter=objects))


@pytest.fixture
def clean_minio():
    # Make sure to delete extra objects in the remote Minio bucket
    _cleanup_minio()
    yield
    # Comment this out if tests fail and you want to see what the hell went on in the bucket.
    _cleanup_minio()


def test_s3_push_pull(empty_pg_conn, remote_driver_conn, clean_minio):
    # Test pushing/pulling when the objects are uploaded to a remote storage instead of to the actual remote DB.

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

    # Push to origin, but this time upload the actual objects instead.
    push(empty_pg_conn, PG_MNT_PULL, remote_repository=PG_MNT, handler='S3',
         handler_options={})

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
