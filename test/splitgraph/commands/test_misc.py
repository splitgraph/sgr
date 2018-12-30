import pytest

from splitgraph import SplitGraphException


@pytest.mark.parametrize("include_snap", [True, False])
def test_log_checkout(include_snap, pg_repo_local):
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")

    head = pg_repo_local.head
    head_1 = pg_repo_local.commit(include_snap=include_snap)

    pg_repo_local.run_sql("DELETE FROM fruits WHERE name = 'apple'")

    head_2 = pg_repo_local.commit(include_snap=include_snap)

    assert pg_repo_local.head.image_hash == head_2
    # 4 images (last one is the empty 00000... image -- should try to avoid having that 0 image at all
    # in the future)
    assert pg_repo_local.images.by_hash(head_2).get_log()[:3] == [head_2, head_1, head.image_hash]

    head.checkout()
    assert pg_repo_local.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange')]

    pg_repo_local.images.by_hash(head_1).checkout()
    assert pg_repo_local.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

    pg_repo_local.images.by_hash(head_2).checkout()
    assert pg_repo_local.run_sql("SELECT * FROM fruits") == [(2, 'orange'), (3, 'mayonnaise')]


@pytest.mark.parametrize("include_snap", [True, False])
def test_table_changes(include_snap, pg_repo_local):
    pg_repo_local.run_sql("CREATE TABLE fruits_copy AS SELECT * FROM fruits")

    head = pg_repo_local.head
    # Check that table addition has been detected
    assert pg_repo_local.diff('fruits_copy', image_1=head.image_hash, image_2=None) is True

    head_1 = pg_repo_local.commit(include_snap=include_snap)
    # Checkout the old head and make sure the table doesn't exist in it
    head.checkout()
    assert not pg_repo_local.engine.table_exists(pg_repo_local.to_schema(), 'fruits_copy')

    # Make sure the table is reflected in the diff even if we're on a different commit
    assert pg_repo_local.diff('fruits_copy', image_1=head.image_hash, image_2=head_1) is True

    # Go back and now delete a table
    pg_repo_local.images.by_hash(head_1).checkout()
    assert pg_repo_local.engine.table_exists(pg_repo_local.to_schema(), 'fruits_copy')
    pg_repo_local.engine.delete_table(pg_repo_local.to_schema(), "fruits")
    assert not pg_repo_local.engine.table_exists(pg_repo_local.to_schema(), 'fruits')

    # Make sure the diff shows it's been removed and commit it
    assert pg_repo_local.diff('fruits', image_1=head_1, image_2=None) is False
    head_2 = pg_repo_local.commit(include_snap=include_snap)

    # Go through the 3 commits and ensure the table existence is maintained
    head.checkout()
    assert pg_repo_local.engine.table_exists(pg_repo_local.to_schema(), 'fruits')
    pg_repo_local.images.by_hash(head_1).checkout()
    assert pg_repo_local.engine.table_exists(pg_repo_local.to_schema(), 'fruits')
    pg_repo_local.images.by_hash(head_2).checkout()
    assert not pg_repo_local.engine.table_exists(pg_repo_local.to_schema(), 'fruits')


def test_tagging(pg_repo_local):
    head = pg_repo_local.head
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    pg_repo_local.commit()

    head.checkout()
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'mustard')")
    right = pg_repo_local.commit()

    head.tag('base')
    pg_repo_local.images.by_hash(right).tag('right')

    pg_repo_local.images['base'].checkout()
    assert pg_repo_local.head == head

    pg_repo_local.images['right'].checkout()
    assert pg_repo_local.head.image_hash == right

    hashes_tags = pg_repo_local.get_all_hashes_tags()
    assert (head.image_hash, 'base') in hashes_tags
    assert (right, 'right') in hashes_tags
    assert (right, 'HEAD') in hashes_tags


def test_image_resolution(pg_repo_local):
    head = pg_repo_local.head
    assert pg_repo_local.images[head.image_hash[:10]] == head
    assert pg_repo_local.images['latest'] == head
    with pytest.raises(SplitGraphException):
        img = pg_repo_local.images['abcdef1234567890abcdef']
    with pytest.raises(SplitGraphException):
        img = pg_repo_local.images['some_weird_tag']
