
from splitgraph._data.objects import get_existing_objects, get_external_object_locations, get_downloaded_objects
from splitgraph.core.engine import cleanup_objects
from splitgraph.core.repository import clone
from splitgraph.engine import switch_engine
from test.splitgraph.conftest import PG_MNT, REMOTE_ENGINE


def test_s3_push_pull(local_engine_empty, pg_repo_remote, clean_minio):
    # Test pushing/pulling when the objects are uploaded to a remote storage instead of to the actual remote DB.

    clone(pg_repo_remote, local_repository=PG_MNT, download_all=False)
    # Add a couple of commits, this time on the cloned copy.
    head = PG_MNT.resolve_image('latest')
    PG_MNT.checkout(head)
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    left = PG_MNT.commit()
    PG_MNT.checkout(head)
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mustard')")
    right = PG_MNT.commit()

    # Push to origin, but this time upload the actual objects instead.
    PG_MNT.push(remote_repository=pg_repo_remote, handler='S3', handler_options={})

    # Check that the actual objects don't exist on the remote but are instead registered with an URL.
    # All the objects on pgcache were registered remotely
    with switch_engine(REMOTE_ENGINE):
        objects = get_existing_objects()
    local_objects = get_existing_objects()
    assert all(o in objects for o in local_objects)
    # Two non-local objects in the local engine, both registered as non-local on the remote engine.
    ext_objects_orig = get_external_object_locations(list(objects))
    with switch_engine(REMOTE_ENGINE):
        ext_objects_pull = get_external_object_locations(list(objects))
    assert len(ext_objects_orig) == 2
    assert all(e in ext_objects_pull for e in ext_objects_orig)

    # Destroy the pulled mountpoint and recreate it again.
    assert len(get_downloaded_objects()) == 4
    PG_MNT.rm()
    # Make sure we don't have any leftover physical objects.
    cleanup_objects()
    assert len(get_downloaded_objects()) == 0

    clone(pg_repo_remote, local_repository=PG_MNT, download_all=False)

    # Proceed as per the lazy checkout tests to make sure we don't download more than required.
    # Make sure we still haven't downloaded anything.
    assert len(get_downloaded_objects()) == 0

    # Check out left commit: since it only depends on the root, we should download just the new version of fruits.
    PG_MNT.checkout(left)
    assert len(get_downloaded_objects()) == 3  # now have 2 versions of fruits + 1 vegetables

    PG_MNT.checkout(right)
    assert len(get_downloaded_objects()) == 4
    # Only now we actually have all the objects materialized.
    assert get_downloaded_objects() == get_existing_objects()
