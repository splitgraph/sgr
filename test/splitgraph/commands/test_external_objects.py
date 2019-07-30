from unittest.mock import patch, Mock

import pytest
from minio.error import MinioError

from splitgraph import ResultShape
from splitgraph.core.repository import clone
from test.splitgraph.conftest import PG_MNT


def test_s3_push_pull(local_engine_empty, pg_repo_remote, clean_minio):
    # Test pushing/pulling when the objects are uploaded to a remote storage instead of to the actual remote DB.

    clone(pg_repo_remote, local_repository=PG_MNT, download_all=False)
    # Add a couple of commits, this time on the cloned copy.
    head = PG_MNT.images["latest"]
    head.checkout()
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    left = PG_MNT.commit()
    head.checkout()
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mustard')")
    right = PG_MNT.commit()

    # Push to origin, but this time upload the actual objects instead.
    PG_MNT.push(remote_repository=pg_repo_remote, handler="S3", handler_options={})

    # Check that the actual objects don't exist on the remote but are instead registered with an URL.
    # All the objects on pgcache were registered remotely
    objects = pg_repo_remote.objects.get_all_objects()
    local_objects = PG_MNT.objects.get_all_objects()
    assert all(o in objects for o in local_objects)
    # Two non-local objects in the local engine, both registered as non-local on the remote engine.
    ext_objects_orig = PG_MNT.objects.get_external_object_locations(list(objects))
    ext_objects_pull = pg_repo_remote.objects.get_external_object_locations(list(objects))
    assert len(ext_objects_orig) == 2
    assert all(e in ext_objects_pull for e in ext_objects_orig)

    # Destroy the pulled mountpoint and recreate it again.
    assert len(PG_MNT.objects.get_downloaded_objects()) == 4
    PG_MNT.delete()
    # Make sure we don't have any leftover physical objects.
    PG_MNT.objects.cleanup()
    assert len(PG_MNT.objects.get_downloaded_objects()) == 0

    clone(pg_repo_remote, local_repository=PG_MNT, download_all=False)

    # Proceed as per the lazy checkout tests to make sure we don't download more than required.
    # Make sure we still haven't downloaded anything.
    assert len(PG_MNT.objects.get_downloaded_objects()) == 0

    # Check out left commit: since it only depends on the root, we should download just the new version of fruits.
    left.checkout()
    assert (
        len(PG_MNT.objects.get_downloaded_objects()) == 3
    )  # now have 2 versions of fruits + 1 vegetables

    right.checkout()
    assert len(PG_MNT.objects.get_downloaded_objects()) == 4
    # Only now we actually have all the objects materialized.
    assert sorted(PG_MNT.objects.get_downloaded_objects()) == sorted(
        PG_MNT.objects.get_all_objects()
    )


def test_push_upload_error(local_engine_empty, pg_repo_remote, clean_minio):
    clone(pg_repo_remote, local_repository=PG_MNT, download_all=False)
    PG_MNT.images["latest"].checkout()
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    head = PG_MNT.commit()

    # If the upload fails for whatever reason (e.g. Minio is inaccessible), the whole
    # push fails rather than leaving the engine in an inconsistent state.
    broken_upload_handler = Mock()
    broken_upload_handler.upload_objects.side_effect = MinioError("Minio Error!")
    with patch(
        "splitgraph.core.object_manager.get_external_object_handler",
        return_value=broken_upload_handler,
    ):
        with pytest.raises(MinioError) as e:
            PG_MNT.push(remote_repository=pg_repo_remote, handler="S3", handler_options={})

    assert head not in pg_repo_remote.images
    # Only the two original tables from the original image upstream
    assert (
        pg_repo_remote.engine.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.tables", return_shape=ResultShape.ONE_ONE
        )
        == 2
    )
    assert len(pg_repo_remote.objects.get_all_objects()) == 2

    # Objects not registered remotely since the upload failed
    assert (
        local_engine_empty.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.object_locations",
            return_shape=ResultShape.ONE_ONE,
        )
        == 0
    )
    assert (
        pg_repo_remote.engine.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.object_locations",
            return_shape=ResultShape.ONE_ONE,
        )
        == 0
    )

    # Now do the push normally and check the image exists upstream.
    PG_MNT.push(remote_repository=pg_repo_remote, handler="S3", handler_options={})
    assert head in pg_repo_remote.images

    assert (
        local_engine_empty.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.object_locations",
            return_shape=ResultShape.ONE_ONE,
        )
        == 1
    )
    assert (
        pg_repo_remote.engine.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.object_locations",
            return_shape=ResultShape.ONE_ONE,
        )
        == 1
    )
