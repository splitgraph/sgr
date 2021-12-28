from test.splitgraph.conftest import PG_MNT

import pytest
from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core.repository import Repository, clone
from splitgraph.engine import ResultShape
from splitgraph.exceptions import ImageNotFoundError


def _add_image_to_repo(repository):
    remote_head = repository.head
    # Do something to fruits on the remote
    repository.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    head_1 = repository.commit()
    # Canary to make sure everything got committed on the remote
    assert repository.diff("fruits", remote_head.image_hash, head_1, aggregate=False) == [
        (True, (3, "mayonnaise"))
    ]
    # Check that the fruits table changed on the original repository
    assert repository.run_sql("SELECT * FROM fruits") == [
        (1, "apple"),
        (2, "orange"),
        (3, "mayonnaise"),
    ]
    # Since the pull procedure initializes a new connection, we have to commit our changes
    # in order to see them.
    repository.commit_engines()
    return head_1


@pytest.mark.parametrize("download_all", [True, False])
def test_pull(local_engine_empty, pg_repo_remote, download_all):
    # Pull the schema from the remote
    # Here, it's the pg on local_engine that connects to the remote engine, so we can use the actual hostname
    # (as opposed to the one exposed to us). However, the clone procedure also uses that connection string to talk to
    # the remote. Hence, there's an /etc/hosts indirection on the host mapping the remote engine to localhost.
    clone(pg_repo_remote, local_repository=PG_MNT, download_all=download_all)
    PG_MNT.images.by_hash(pg_repo_remote.head.image_hash).checkout()

    head_1 = _add_image_to_repo(pg_repo_remote)

    # Check the data is unchanged on the pulled one.
    assert PG_MNT.run_sql("SELECT * FROM fruits") == [(1, "apple"), (2, "orange")]

    with pytest.raises(ImageNotFoundError):
        PG_MNT.images.by_hash(head_1.image_hash)

    PG_MNT.pull()
    head_1 = PG_MNT.images.by_hash(head_1.image_hash)

    # Check out the newly-pulled commit and verify it has the same data.
    head_1.checkout()

    assert PG_MNT.run_sql("SELECT * FROM fruits") == [
        (1, "apple"),
        (2, "orange"),
        (3, "mayonnaise"),
    ]
    assert PG_MNT.head == head_1


def test_pull_tag_overwriting(local_engine_empty, pg_repo_remote):
    head = pg_repo_remote.head
    head_1 = _add_image_to_repo(pg_repo_remote)

    head.tag("tag_1")
    head_1.tag("tag_2")
    head_1.tag("tag_3")
    pg_repo_remote.commit_engines()

    # Clone a single image
    clone(
        pg_repo_remote,
        local_repository=PG_MNT,
        single_image=head.image_hash[:12],
    )
    assert len(PG_MNT.images()) == 1
    assert PG_MNT.images()[0] == head
    assert PG_MNT.images["tag_1"] == head
    assert PG_MNT.images.by_tag("tag_2", raise_on_none=False) is None

    # Clone again, check nothing has changed.
    clone(
        pg_repo_remote,
        local_repository=PG_MNT,
        single_image=head.image_hash[:12],
    )
    assert len(PG_MNT.images()) == 1
    assert PG_MNT.images["tag_1"] == head
    assert PG_MNT.images.by_tag("tag_2", raise_on_none=False) is None

    # Pull the remainder of the repo
    PG_MNT.pull(single_image=head_1.image_hash)
    assert len(PG_MNT.images()) == 2
    assert PG_MNT.images["tag_2"] == head_1

    # Now update the tag on the remote
    head.tag("tag_2")
    pg_repo_remote.commit_engines()

    # Clone head again, check tag_2 wasn't overwritten (is still pointing to head_1)
    clone(
        pg_repo_remote,
        local_repository=PG_MNT,
        single_image=head.image_hash[:12],
    )
    assert PG_MNT.images["tag_1"] == head
    assert PG_MNT.images["tag_2"] == head_1
    assert PG_MNT.images["tag_3"] == head_1

    # Clone head again, this time overwriting the tag
    clone(
        pg_repo_remote,
        local_repository=PG_MNT,
        single_image=head.image_hash[:12],
        overwrite_tags=True,
    )
    assert len(PG_MNT.images()) == 2
    assert PG_MNT.images["tag_1"] == head
    assert PG_MNT.images["tag_2"] == head
    assert PG_MNT.images["tag_3"] == head_1

    # Update tag_3 to point to head as well
    head.tag("tag_3")
    pg_repo_remote.commit_engines()

    # Pull repo, check tag_3 hasn't moved.
    PG_MNT.pull()
    assert PG_MNT.images["tag_1"] == head
    assert PG_MNT.images["tag_2"] == head
    assert PG_MNT.images["tag_3"] == head_1

    # Pull again overwriting all tags, check tags have moved.
    PG_MNT.pull(overwrite_tags=True)
    assert PG_MNT.images["tag_1"] == head
    assert PG_MNT.images["tag_2"] == head
    assert PG_MNT.images["tag_3"] == head


@pytest.mark.parametrize("download_all", [True, False])
def test_pull_single_image(local_engine_empty, pg_repo_remote, download_all):
    head = pg_repo_remote.head
    head_1 = _add_image_to_repo(pg_repo_remote)

    head.tag("tag_1")
    head_1.tag("tag_2")
    pg_repo_remote.commit_engines()

    # Clone a single image first
    assert len(PG_MNT.images()) == 0
    assert len(PG_MNT.objects.get_downloaded_objects()) == 0
    assert len(pg_repo_remote.images()) == 3
    clone(
        pg_repo_remote,
        local_repository=PG_MNT,
        download_all=download_all,
        single_image=head.image_hash[:12],
    )

    # Check only one image got downloaded and check we didn't try
    # to pull tags for images that we weren't pulling.
    assert len(PG_MNT.images()) == 1
    assert PG_MNT.images()[0] == head
    assert PG_MNT.images["tag_1"] == head
    assert PG_MNT.images.by_tag("tag_2", raise_on_none=False) is None

    # Try doing the same thing again
    clone(
        pg_repo_remote,
        local_repository=PG_MNT,
        download_all=download_all,
        single_image=head.image_hash[:12],
    )
    assert len(PG_MNT.images()) == 1

    # If we're downloading objects too, check only the original objects got downloaded
    if download_all:
        assert len(PG_MNT.objects.get_downloaded_objects()) == 2

    # Pull the remainder of the repo
    PG_MNT.pull(single_image=head_1.image_hash, download_all=download_all)
    assert len(PG_MNT.images()) == 2
    if download_all:
        assert len(PG_MNT.objects.get_downloaded_objects()) == 3

    assert PG_MNT.images["tag_2"] == head_1

    # Pull the whole repo
    PG_MNT.pull()
    assert len(PG_MNT.images()) == 3


def test_pulls_with_lazy_object_downloads(local_engine_empty, pg_repo_remote):
    clone(pg_repo_remote, local_repository=PG_MNT, download_all=False)
    # Make sure we haven't downloaded anything until checkout
    assert not PG_MNT.objects.get_downloaded_objects()

    remote_head = pg_repo_remote.head

    PG_MNT.images.by_hash(remote_head.image_hash).checkout()
    assert (
        len(PG_MNT.objects.get_downloaded_objects()) == 2
    )  # Original fruits and vegetables tables.
    assert sorted(PG_MNT.objects.get_downloaded_objects()) == sorted(
        PG_MNT.objects.get_all_objects()
    )

    # In the meantime, make two branches off of origin (a total of 3 commits)
    pg_repo_remote.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    left = pg_repo_remote.commit()

    remote_head.checkout()
    pg_repo_remote.run_sql("INSERT INTO fruits VALUES (3, 'mustard')")
    right = pg_repo_remote.commit()

    # Pull from upstream.
    PG_MNT.pull(download_all=False)
    # Make sure we have the pointers to the three versions of the fruits table + the original vegetables
    assert len(PG_MNT.objects.get_all_objects()) == 4

    # Also make sure still only have the objects with the original fruits + vegetables tables
    assert len(PG_MNT.objects.get_downloaded_objects()) == 2

    # Check out left commit: since it only depends on the root, we should download just the new version of fruits.
    PG_MNT.images.by_hash(left.image_hash).checkout()

    assert (
        len(PG_MNT.objects.get_downloaded_objects()) == 3
    )  # now have 2 versions of fruits + 1 vegetables

    PG_MNT.images.by_hash(right.image_hash).checkout()
    assert (
        len(PG_MNT.objects.get_downloaded_objects()) == 4
    )  # now have 2 versions of fruits + 1 vegetables
    assert sorted(PG_MNT.objects.get_downloaded_objects()) == sorted(
        PG_MNT.objects.get_all_objects()
    )


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
    assert len(pg_repo_remote.objects.get_all_objects()) == 3

    pg_repo_remote.images.by_hash(head_1.image_hash).checkout()
    assert pg_repo_remote.run_sql("SELECT * FROM fruits") == [
        (1, "apple"),
        (2, "orange"),
        (3, "mayonnaise"),
    ]

    # Recommit the local image as a full snap and push it out.
    head_2 = PG_MNT.commit(snap_only=True)
    PG_MNT.push(remote_repository=pg_repo_remote)
    assert head_2.get_table("fruits").objects[0] in pg_repo_remote.objects.get_all_objects()

    # Recommit it again, changing the sort order
    head_3 = PG_MNT.commit(snap_only=True, in_fragment_order={"fruits": ["name"]}, overwrite=True)
    assert head_3.get_table("fruits").objects == head_2.get_table("fruits").objects

    assert (
        PG_MNT.run_sql(
            SQL("SELECT fruit_id FROM {}.{}").format(
                Identifier(SPLITGRAPH_META_SCHEMA),
                Identifier(head_2.get_table("fruits").objects[0]),
            ),
            return_shape=ResultShape.MANY_ONE,
        )
        == [1, 3, 2]
    )

    # Force push overwriting object meta and the actual object
    PG_MNT.push(
        remote_repository=pg_repo_remote,
        single_image=head_3.image_hash,
        overwrite_objects=True,
        reupload_objects=True,
    )

    assert (
        pg_repo_remote.run_sql(
            SQL("SELECT fruit_id FROM {}.{}").format(
                Identifier(SPLITGRAPH_META_SCHEMA),
                Identifier(head_2.get_table("fruits").objects[0]),
            ),
            return_shape=ResultShape.MANY_ONE,
        )
        == [1, 3, 2]
    )
    # apple, mayonnaise, orange (alphabetical order since we sorted on fruit name)


def test_push_single_image(pg_repo_local, remote_engine):
    original_head = pg_repo_local.head
    _add_image_to_repo(pg_repo_local)
    remote_repo = Repository.from_template(pg_repo_local, engine=remote_engine)
    assert len(remote_repo.images()) == 0
    assert len(remote_repo.objects.get_all_objects()) == 0

    pg_repo_local.push(remote_repository=remote_repo, single_image=original_head.image_hash)
    assert len(remote_repo.images()) == 1
    assert len(remote_repo.objects.get_all_objects()) == 2

    # Try pushing the same image again
    pg_repo_local.push(remote_repository=remote_repo, single_image=original_head.image_hash)
    assert len(remote_repo.images()) == 1
    assert len(remote_repo.objects.get_all_objects()) == 2

    # Test we can check the repo out on the remote.
    remote_repo.images[original_head.image_hash].checkout()

    # Push the rest
    pg_repo_local.push(remote_repo)
    assert len(remote_repo.images()) == 3
    assert len(remote_repo.objects.get_all_objects()) == 3
