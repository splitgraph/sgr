import pytest

from splitgraph.commands import commit, get_log, checkout, diff
from splitgraph.meta_handler.tags import get_current_head, get_all_hashes_tags, set_tag
from splitgraph.pg_utils import pg_table_exists
from test.splitgraph.conftest import PG_MNT


@pytest.mark.parametrize("include_snap", [True, False])
def test_log_checkout(include_snap, sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise')""")

    head = get_current_head(sg_pg_conn, PG_MNT)
    head_1 = commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

    with sg_pg_conn.cursor() as cur:
        cur.execute("""DELETE FROM "test/pg_mount".fruits WHERE name = 'apple'""")

    head_2 = commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

    assert get_current_head(sg_pg_conn, PG_MNT) == head_2
    assert get_log(sg_pg_conn, PG_MNT, head_2) == [head_2, head_1, head]

    checkout(sg_pg_conn, PG_MNT, head)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange')]

    checkout(sg_pg_conn, PG_MNT, head_1)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

    checkout(sg_pg_conn, PG_MNT, head_2)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        assert list(cur.fetchall()) == [(2, 'orange'), (3, 'mayonnaise')]


@pytest.mark.parametrize("include_snap", [True, False])
def test_table_changes(include_snap, sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""CREATE TABLE "test/pg_mount".fruits_copy AS SELECT * FROM "test/pg_mount".fruits""")

    head = get_current_head(sg_pg_conn, PG_MNT)
    # Check that table addition has been detected
    assert diff(sg_pg_conn, PG_MNT, 'fruits_copy', image_1=head, image_2=None) is True

    head_1 = commit(sg_pg_conn, PG_MNT, include_snap=include_snap)
    # Checkout the old head and make sure the table doesn't exist in it
    checkout(sg_pg_conn, PG_MNT, head)
    assert not pg_table_exists(sg_pg_conn, PG_MNT.to_schema(), 'fruits_copy')

    # Make sure the table is reflected in the diff even if we're on a different commit
    assert diff(sg_pg_conn, PG_MNT, 'fruits_copy', image_1=head, image_2=head_1) is True

    # Go back and now delete a table
    checkout(sg_pg_conn, PG_MNT, head_1)
    assert pg_table_exists(sg_pg_conn, PG_MNT.to_schema(), 'fruits_copy')
    with sg_pg_conn.cursor() as cur:
        cur.execute("""DROP TABLE "test/pg_mount".fruits""")
    assert not pg_table_exists(sg_pg_conn, PG_MNT.to_schema(), 'fruits')

    # Make sure the diff shows it's been removed and commit it
    assert diff(sg_pg_conn, PG_MNT, 'fruits', image_1=head_1, image_2=None) is False
    head_2 = commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

    # Go through the 3 commits and ensure the table existence is maintained
    checkout(sg_pg_conn, PG_MNT, head)
    assert pg_table_exists(sg_pg_conn, PG_MNT.to_schema(), 'fruits')
    checkout(sg_pg_conn, PG_MNT, head_1)
    assert pg_table_exists(sg_pg_conn, PG_MNT.to_schema(), 'fruits')
    checkout(sg_pg_conn, PG_MNT, head_2)
    assert not pg_table_exists(sg_pg_conn, PG_MNT.to_schema(), 'fruits')


def test_tagging(sg_pg_conn):
    head = get_current_head(sg_pg_conn, PG_MNT)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise')""")
    commit(sg_pg_conn, PG_MNT)

    checkout(sg_pg_conn, PG_MNT, head)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mustard')""")
    right = commit(sg_pg_conn, PG_MNT)

    set_tag(sg_pg_conn, PG_MNT, head, 'base')
    set_tag(sg_pg_conn, PG_MNT, right, 'right')

    checkout(sg_pg_conn, PG_MNT, tag='base')
    assert get_current_head(sg_pg_conn, PG_MNT) == head

    checkout(sg_pg_conn, PG_MNT, tag='right')
    assert get_current_head(sg_pg_conn, PG_MNT) == right

    hashes_tags = get_all_hashes_tags(sg_pg_conn, PG_MNT)
    assert (head, 'base') in hashes_tags
    assert (right, 'right') in hashes_tags
    assert (right, 'HEAD') in hashes_tags
