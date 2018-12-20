import pytest

from splitgraph import SplitGraphException
from test.splitgraph.conftest import PG_MNT


@pytest.mark.parametrize("include_snap", [True, False])
def test_log_checkout(include_snap, local_engine_with_pg):
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")

    head = PG_MNT.get_head()
    head_1 = PG_MNT.commit(include_snap=include_snap)

    PG_MNT.run_sql("DELETE FROM fruits WHERE name = 'apple'")

    head_2 = PG_MNT.commit(include_snap=include_snap)

    assert PG_MNT.get_head() == head_2
    assert PG_MNT.get_image(head_2).get_log() == [head_2, head_1, head]

    PG_MNT.checkout(head)
    assert PG_MNT.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange')]

    PG_MNT.checkout(head_1)
    assert PG_MNT.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

    PG_MNT.checkout(head_2)
    assert PG_MNT.run_sql("SELECT * FROM fruits") == [(2, 'orange'), (3, 'mayonnaise')]


@pytest.mark.parametrize("include_snap", [True, False])
def test_table_changes(include_snap, local_engine_with_pg):
    PG_MNT.run_sql("CREATE TABLE fruits_copy AS SELECT * FROM fruits")

    head = PG_MNT.get_head()
    # Check that table addition has been detected
    assert PG_MNT.diff('fruits_copy', image_1=head, image_2=None) is True

    head_1 = PG_MNT.commit(include_snap=include_snap)
    # Checkout the old head and make sure the table doesn't exist in it
    PG_MNT.checkout(head)
    assert not PG_MNT.engine.table_exists(PG_MNT.to_schema(), 'fruits_copy')

    # Make sure the table is reflected in the diff even if we're on a different commit
    assert PG_MNT.diff('fruits_copy', image_1=head, image_2=head_1) is True

    # Go back and now delete a table
    PG_MNT.checkout(head_1)
    assert PG_MNT.engine.table_exists(PG_MNT.to_schema(), 'fruits_copy')
    PG_MNT.engine.delete_table(PG_MNT.to_schema(), "fruits")
    assert not PG_MNT.engine.table_exists(PG_MNT.to_schema(), 'fruits')

    # Make sure the diff shows it's been removed and commit it
    assert PG_MNT.diff('fruits', image_1=head_1, image_2=None) is False
    head_2 = PG_MNT.commit(include_snap=include_snap)

    # Go through the 3 commits and ensure the table existence is maintained
    PG_MNT.checkout(head)
    assert PG_MNT.engine.table_exists(PG_MNT.to_schema(), 'fruits')
    PG_MNT.checkout(head_1)
    assert PG_MNT.engine.table_exists(PG_MNT.to_schema(), 'fruits')
    PG_MNT.checkout(head_2)
    assert not PG_MNT.engine.table_exists(PG_MNT.to_schema(), 'fruits')


def test_tagging(local_engine_with_pg):
    head = PG_MNT.get_head()
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    PG_MNT.commit()

    PG_MNT.checkout(head)
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mustard')")
    right = PG_MNT.commit()

    PG_MNT.get_image(head).tag('base')
    PG_MNT.get_image(right).tag('right')

    PG_MNT.checkout(tag='base')
    assert PG_MNT.get_head() == head

    PG_MNT.checkout(tag='right')
    assert PG_MNT.get_head() == right

    hashes_tags = PG_MNT.get_all_hashes_tags()
    assert (head, 'base') in hashes_tags
    assert (right, 'right') in hashes_tags
    assert (right, 'HEAD') in hashes_tags


def test_image_resolution(local_engine_with_pg):
    head = PG_MNT.get_head()
    assert PG_MNT.resolve_image(head[:10]) == head
    assert PG_MNT.resolve_image('latest') == head
    with pytest.raises(SplitGraphException):
        PG_MNT.resolve_image('abcdef1234567890abcdef')
    with pytest.raises(SplitGraphException):
        PG_MNT.resolve_image('some_weird_tag')
