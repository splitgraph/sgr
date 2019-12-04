import pytest

from splitgraph.engine import ResultShape
from splitgraph.exceptions import ImageNotFoundError


@pytest.mark.parametrize("snap_only", [True, False])
def test_log_checkout(snap_only, pg_repo_local):
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    pg_repo_local.run_sql("COMMENT ON COLUMN fruits.name IS 'Name of the fruit'")

    head = pg_repo_local.head
    head_1 = pg_repo_local.commit(snap_only=snap_only)

    pg_repo_local.run_sql("DELETE FROM fruits WHERE name = 'apple'")

    head_2 = pg_repo_local.commit(snap_only=snap_only)

    assert pg_repo_local.head == head_2
    # 4 images (last one is the empty 00000... image -- should try to avoid having that 0 image at all
    # in the future)
    assert head_2.get_log()[:3] == [head_2, head_1, head]

    # Test that the column comment is correctly reproduced on checkout.
    head.checkout()
    assert pg_repo_local.run_sql("SELECT * FROM fruits") == [(1, "apple"), (2, "orange")]
    assert (
        pg_repo_local.run_sql(
            "SELECT col_description('fruits'::regclass, 2)", return_shape=ResultShape.ONE_ONE
        )
        is None
    )

    head_1.checkout()
    assert pg_repo_local.run_sql("SELECT * FROM fruits") == [
        (1, "apple"),
        (2, "orange"),
        (3, "mayonnaise"),
    ]
    assert (
        pg_repo_local.run_sql(
            "SELECT col_description('fruits'::regclass, 2)", return_shape=ResultShape.ONE_ONE
        )
        == "Name of the fruit"
    )

    head_2.checkout()
    assert pg_repo_local.run_sql("SELECT * FROM fruits") == [(2, "orange"), (3, "mayonnaise")]
    assert (
        pg_repo_local.run_sql(
            "SELECT col_description('fruits'::regclass, 2)", return_shape=ResultShape.ONE_ONE
        )
        == "Name of the fruit"
    )


@pytest.mark.parametrize("snap_only", [True, False])
def test_table_changes(snap_only, pg_repo_local):
    pg_repo_local.run_sql("CREATE TABLE fruits_copy AS SELECT * FROM fruits")

    head = pg_repo_local.head
    # Check that table addition has been detected
    assert pg_repo_local.diff("fruits_copy", image_1=head.image_hash, image_2=None) is True

    head_1 = pg_repo_local.commit(snap_only=snap_only)
    # Checkout the old head and make sure the table doesn't exist in it
    head.checkout()
    assert not pg_repo_local.engine.table_exists(pg_repo_local.to_schema(), "fruits_copy")

    # Make sure the table is reflected in the diff even if we're on a different commit
    assert pg_repo_local.diff("fruits_copy", image_1=head.image_hash, image_2=head_1) is True

    # Go back and now delete a table
    head_1.checkout()
    assert pg_repo_local.engine.table_exists(pg_repo_local.to_schema(), "fruits_copy")
    pg_repo_local.engine.delete_table(pg_repo_local.to_schema(), "fruits")
    assert not pg_repo_local.engine.table_exists(pg_repo_local.to_schema(), "fruits")

    # Make sure the diff shows it's been removed and commit it
    assert pg_repo_local.diff("fruits", image_1=head_1, image_2=None) is False
    head_2 = pg_repo_local.commit(snap_only=snap_only)

    # Go through the 3 commits and ensure the table existence is maintained
    head.checkout()
    assert pg_repo_local.engine.table_exists(pg_repo_local.to_schema(), "fruits")
    head_1.checkout()
    assert pg_repo_local.engine.table_exists(pg_repo_local.to_schema(), "fruits")
    head_2.checkout()
    assert not pg_repo_local.engine.table_exists(pg_repo_local.to_schema(), "fruits")


def test_tagging(pg_repo_local):
    head = pg_repo_local.head
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    pg_repo_local.commit()

    head.checkout()
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'mustard')")
    right = pg_repo_local.commit()

    head.tag("base")
    right.tag("right")

    pg_repo_local.images["base"].checkout()
    assert pg_repo_local.head == head

    pg_repo_local.images["right"].checkout()
    assert pg_repo_local.head == right

    hashes_tags = pg_repo_local.get_all_hashes_tags()
    assert (head.image_hash, "base") in hashes_tags
    assert (right.image_hash, "right") in hashes_tags
    assert (right.image_hash, "HEAD") in hashes_tags


def test_image_resolution(pg_repo_local):
    head = pg_repo_local.head
    assert pg_repo_local.images[head.image_hash[:10]] == head
    assert pg_repo_local.images["latest"] == head
    with pytest.raises(ImageNotFoundError):
        img = pg_repo_local.images["abcdef1234567890abcdef"]
    with pytest.raises(ImageNotFoundError):
        img = pg_repo_local.images["some_weird_tag"]


def test_tag_errors(pg_repo_local):
    pg_repo_local.uncheckout()
    with pytest.raises(ImageNotFoundError) as e:
        head = pg_repo_local.images.by_tag("HEAD")
    assert "No current checked out revision found" in str(e.value)

    with pytest.raises(ImageNotFoundError) as e:
        tag = pg_repo_local.images.by_tag("notatag")
    assert "Tag notatag not found" in str(e.value)


def test_upstream_goes_away(pg_repo_local):
    # Check upstream getter doesn't crash if a remote is deleted from the config.
    pg_repo_local.engine.run_sql(
        "INSERT INTO splitgraph_meta.upstream (namespace, repository, "
        "remote_name, remote_namespace, remote_repository) VALUES (%s, %s, %s, %s, %s)",
        (
            pg_repo_local.namespace,
            pg_repo_local.repository,
            "nonexistent_engine",
            pg_repo_local.namespace,
            pg_repo_local.repository,
        ),
    )

    assert pg_repo_local.upstream is None
