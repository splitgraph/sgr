import pytest

from splitgraph import SplitGraphException
from splitgraph.commands import commit, get_log, checkout, diff
from splitgraph.commands.tagging import get_current_head, get_all_hashes_tags, set_tag, resolve_image
from splitgraph.engine import get_engine
from test.splitgraph.conftest import PG_MNT


@pytest.mark.parametrize("include_snap", [True, False])
def test_log_checkout(include_snap, sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise')""")

    head = get_current_head(PG_MNT)
    head_1 = commit(PG_MNT, include_snap=include_snap)

    with sg_pg_conn.cursor() as cur:
        cur.execute("""DELETE FROM "test/pg_mount".fruits WHERE name = 'apple'""")

    head_2 = commit(PG_MNT, include_snap=include_snap)

    assert get_current_head(PG_MNT) == head_2
    assert get_log(PG_MNT, head_2) == [head_2, head_1, head]

    checkout(PG_MNT, head)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange')]

    checkout(PG_MNT, head_1)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

    checkout(PG_MNT, head_2)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        assert list(cur.fetchall()) == [(2, 'orange'), (3, 'mayonnaise')]


@pytest.mark.parametrize("include_snap", [True, False])
def test_table_changes(include_snap, sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""CREATE TABLE "test/pg_mount".fruits_copy AS SELECT * FROM "test/pg_mount".fruits""")

    head = get_current_head(PG_MNT)
    # Check that table addition has been detected
    assert diff(PG_MNT, 'fruits_copy', image_1=head, image_2=None) is True

    head_1 = commit(PG_MNT, include_snap=include_snap)
    # Checkout the old head and make sure the table doesn't exist in it
    checkout(PG_MNT, head)
    engine = get_engine()
    assert not engine.table_exists(PG_MNT.to_schema(), 'fruits_copy')

    # Make sure the table is reflected in the diff even if we're on a different commit
    assert diff(PG_MNT, 'fruits_copy', image_1=head, image_2=head_1) is True

    # Go back and now delete a table
    checkout(PG_MNT, head_1)
    assert engine.table_exists(PG_MNT.to_schema(), 'fruits_copy')
    with sg_pg_conn.cursor() as cur:
        cur.execute("""DROP TABLE "test/pg_mount".fruits""")
    assert not engine.table_exists(PG_MNT.to_schema(), 'fruits')

    # Make sure the diff shows it's been removed and commit it
    assert diff(PG_MNT, 'fruits', image_1=head_1, image_2=None) is False
    head_2 = commit(PG_MNT, include_snap=include_snap)

    # Go through the 3 commits and ensure the table existence is maintained
    checkout(PG_MNT, head)
    assert engine.table_exists(PG_MNT.to_schema(), 'fruits')
    checkout(PG_MNT, head_1)
    assert engine.table_exists(PG_MNT.to_schema(), 'fruits')
    checkout(PG_MNT, head_2)
    assert not engine.table_exists(PG_MNT.to_schema(), 'fruits')


def test_tagging(sg_pg_conn):
    head = get_current_head(PG_MNT)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise')""")
    commit(PG_MNT)

    checkout(PG_MNT, head)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mustard')""")
    right = commit(PG_MNT)

    set_tag(PG_MNT, head, 'base')
    set_tag(PG_MNT, right, 'right')

    checkout(PG_MNT, tag='base')
    assert get_current_head(PG_MNT) == head

    checkout(PG_MNT, tag='right')
    assert get_current_head(PG_MNT) == right

    hashes_tags = get_all_hashes_tags(PG_MNT)
    assert (head, 'base') in hashes_tags
    assert (right, 'right') in hashes_tags
    assert (right, 'HEAD') in hashes_tags


def test_image_resolution(sg_pg_conn):
    head = get_current_head(PG_MNT)
    assert resolve_image(PG_MNT, head[:10]) == head
    assert resolve_image(PG_MNT, 'latest') == head
    with pytest.raises(SplitGraphException):
        resolve_image(PG_MNT, 'abcdef1234567890abcdef')
    with pytest.raises(SplitGraphException):
        resolve_image(PG_MNT, 'some_weird_tag')
