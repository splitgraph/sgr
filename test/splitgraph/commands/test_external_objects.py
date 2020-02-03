from unittest.mock import patch

import pytest

from splitgraph.core.engine import repository_exists
from splitgraph.core.repository import clone
from splitgraph.engine import ResultShape
from splitgraph.exceptions import IncompleteObjectTransferError
from splitgraph.hooks.s3 import S3ExternalObjectHandler
from splitgraph.hooks.s3_server import (
    get_object_upload_urls,
    get_object_download_urls,
    S3_HOST,
    S3_PORT,
)
from test.splitgraph.conftest import PG_MNT


@pytest.mark.registry
def test_s3_presigned_url(local_engine_empty, unprivileged_pg_repo, clean_minio):
    # Test the URL signing stored procedure works on the remote machine
    clone(unprivileged_pg_repo, local_repository=PG_MNT, download_all=False)
    PG_MNT.images["latest"].checkout()
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    head = PG_MNT.commit()
    object_id = head.get_table("fruits").objects[0]

    # Do a test calling the signer locally (the tests currently have access
    # to the S3 credentials on the host they're running on)
    urls_local = get_object_upload_urls("%s:%s" % (S3_HOST, S3_PORT), [object_id])
    assert len(urls_local) == 1
    assert len(urls_local[0]) == 3
    urls_local = get_object_download_urls("%s:%s" % (S3_HOST, S3_PORT), [object_id])
    assert len(urls_local) == 1
    assert len(urls_local[0]) == 3

    urls = unprivileged_pg_repo.run_sql(
        "SELECT * FROM splitgraph_api.get_object_upload_urls(%s, %s)",
        ("%s:%s" % (S3_HOST, S3_PORT), [object_id]),
        return_shape=ResultShape.ONE_ONE,
    )
    assert len(urls) == 1
    assert len(urls[0]) == 3


@pytest.mark.registry
def test_s3_push_pull(
    local_engine_empty, unprivileged_pg_repo, pg_repo_remote_registry, clean_minio
):
    # Test pushing/pulling when the objects are uploaded to a remote storage instead of to the actual remote DB.

    # In the beginning, the registry has two objects, all remote
    objects = pg_repo_remote_registry.objects.get_all_objects()
    assert len(unprivileged_pg_repo.objects.get_external_object_locations(list(objects))) == 2
    assert len(objects) == 2

    clone(unprivileged_pg_repo, local_repository=PG_MNT, download_all=False)
    # Add a couple of commits, this time on the cloned copy.
    head = PG_MNT.images["latest"]
    head.checkout()
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    left = PG_MNT.commit()
    head.checkout()
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mustard')")
    right = PG_MNT.commit()

    # Push to origin, but this time upload the actual objects instead.
    PG_MNT.push(remote_repository=unprivileged_pg_repo, handler="S3", handler_options={})

    # Check that the actual objects don't exist on the remote but are instead registered with an URL.
    # All the objects on pgcache were registered remotely
    objects = pg_repo_remote_registry.objects.get_all_objects()
    local_objects = PG_MNT.objects.get_all_objects()
    assert all(o in objects for o in local_objects)
    # Two new non-local objects in the local engine, both registered as non-local on the remote engine.
    ext_objects_orig = PG_MNT.objects.get_external_object_locations(list(objects))
    ext_objects_pull = unprivileged_pg_repo.objects.get_external_object_locations(list(objects))
    assert len(ext_objects_orig) == 4
    assert all(e in ext_objects_pull for e in ext_objects_orig)

    # Destroy the pulled mountpoint and recreate it again.
    assert len(PG_MNT.objects.get_downloaded_objects()) == 4
    PG_MNT.delete()
    # Make sure we don't have any leftover physical objects.
    PG_MNT.objects.cleanup()
    assert len(PG_MNT.objects.get_downloaded_objects()) == 0

    clone(unprivileged_pg_repo, local_repository=PG_MNT, download_all=False)

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


def _flaky_handler(incomplete=False):
    class FlakyExternalObjectHandler(S3ExternalObjectHandler):
        """
        An external object handler that fails after downloading some objects
        to emulate connection errors/CTRL+Cs etc -- test database is still in a
        consistent state in these cases.
        """

        def __init__(self, params):
            super().__init__(params)

        def download_objects(self, objects, remote_engine):
            """
            Downloads just the first object and then fails.
            """
            super().download_objects(objects[:1], remote_engine)
            ex = Exception("Something bad happened.")
            if incomplete:
                raise IncompleteObjectTransferError(reason=ex, successful_objects=[objects[0][0]])
            else:
                raise ex

        def upload_objects(self, objects, remote_engine):
            """
            Uploads just the first object and then fails.
            """
            super().upload_objects(objects[:1], remote_engine)
            ex = Exception("Something bad happened.")
            if incomplete:
                raise IncompleteObjectTransferError(
                    reason=ex, successful_objects=objects[:1], successful_object_urls=objects[:1]
                )
            else:
                raise ex

    return FlakyExternalObjectHandler


@pytest.mark.registry
@pytest.mark.parametrize("interrupted", [True, False])
def test_push_upload_error(
    local_engine_empty, unprivileged_pg_repo, pg_repo_remote_registry, clean_minio, interrupted
):
    clone(unprivileged_pg_repo, local_repository=PG_MNT, download_all=False)
    PG_MNT.images["latest"].checkout()
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    PG_MNT.run_sql("INSERT INTO vegetables VALUES (3, 'cucumber')")
    head = PG_MNT.commit()

    # If the upload fails for whatever reason (e.g. Minio is inaccessible or the upload was aborted),
    # the whole push fails rather than leaving the registry in an inconsistent state.
    with patch.dict(
        "splitgraph.hooks.external_objects._EXTERNAL_OBJECT_HANDLERS",
        {"S3": _flaky_handler(incomplete=interrupted)},
    ):
        with pytest.raises(Exception) as e:
            PG_MNT.push(remote_repository=unprivileged_pg_repo, handler="S3", handler_options={})

    assert head not in unprivileged_pg_repo.images
    # Only the two original tables from the original image upstream
    assert (
        pg_repo_remote_registry.engine.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.tables", return_shape=ResultShape.ONE_ONE
        )
        == 2
    )

    # Registry had 2 objects before the upload -- if we interrupted the upload,
    # we only managed to upload the first object that was registered (even if the image
    # wasn't).

    expected_object_count = 3 if interrupted else 2

    assert len(pg_repo_remote_registry.objects.get_all_objects()) == expected_object_count

    # Two new objects not registered remotely since the upload failed
    assert (
        local_engine_empty.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.object_locations",
            return_shape=ResultShape.ONE_ONE,
        )
        == expected_object_count
    )
    assert (
        pg_repo_remote_registry.engine.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.object_locations",
            return_shape=ResultShape.ONE_ONE,
        )
        == expected_object_count
    )

    # Now do the push normally and check the image exists upstream.
    PG_MNT.push(remote_repository=unprivileged_pg_repo, handler="S3", handler_options={})

    assert any(i.image_hash == head.image_hash for i in unprivileged_pg_repo.images)

    assert len(pg_repo_remote_registry.objects.get_all_objects()) == 4

    assert (
        local_engine_empty.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.object_locations",
            return_shape=ResultShape.ONE_ONE,
        )
        == 4
    )
    assert (
        pg_repo_remote_registry.engine.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.object_locations",
            return_shape=ResultShape.ONE_ONE,
        )
        == 4
    )


@pytest.mark.registry
@pytest.mark.parametrize("interrupted", [True, False])
def test_pull_download_error(local_engine_empty, unprivileged_pg_repo, clean_minio, interrupted):
    # Same test backwards: if we're pulling and abort or fail the download, make sure we can
    # recover and retry pulling the repo.

    with patch.dict(
        "splitgraph.hooks.external_objects._EXTERNAL_OBJECT_HANDLERS",
        {"S3": _flaky_handler(interrupted)},
    ):
        with pytest.raises(Exception) as e:
            clone(unprivileged_pg_repo, local_repository=PG_MNT, download_all=True)

    # Check that the pull succeeded (repository registered locally) but the objects
    # are just marked as external, not downloaded
    assert repository_exists(PG_MNT)
    assert len(PG_MNT.objects.get_all_objects()) == 2
    assert len(PG_MNT.objects.get_downloaded_objects()) == 1
    assert len(PG_MNT.objects.get_external_object_locations(PG_MNT.objects.get_all_objects())) == 2
    assert (
        PG_MNT.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.object_cache_status",
            return_shape=ResultShape.ONE_ONE,
        )
        == 1
    )

    clone(unprivileged_pg_repo, local_repository=PG_MNT, download_all=True)
    assert len(PG_MNT.objects.get_all_objects()) == 2
    assert len(PG_MNT.objects.get_downloaded_objects()) == 2
    assert len(list(PG_MNT.images)) == 2
    assert (
        PG_MNT.run_sql(
            "SELECT COUNT(*) FROM splitgraph_meta.object_cache_status",
            return_shape=ResultShape.ONE_ONE,
        )
        == 2
    )
