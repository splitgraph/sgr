from decimal import Decimal

import pytest

from splitgraph.commands import diff, commit, init, checkout
from splitgraph.meta_handler import get_current_head, get_all_snap_info, get_snap_parent, get_table
from splitgraph.pg_replication import has_pending_changes
from test.splitgraph.conftest import PG_MNT, MG_MNT


def test_diff_head(sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")
        cur.execute("""DELETE FROM test_pg_mount.fruits WHERE name = 'apple'""")

    sg_pg_conn.commit()  # otherwise the WAL writer won't see this.
    head = get_current_head(sg_pg_conn, PG_MNT)
    change = diff(sg_pg_conn, PG_MNT, 'fruits', snap_1=head, snap_2=None)
    # Added (3, mayonnaise); Deleted (1, 'apple')
    assert change == [((3, 'mayonnaise'), 0, {'c': [], 'v': []}),
                      ((1, 'apple'), 1, None)]


@pytest.mark.parametrize("include_snap", [True, False])
def test_commit_diff(include_snap, sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")
        cur.execute("""DELETE FROM test_pg_mount.fruits WHERE name = 'apple'""")
        cur.execute("""UPDATE test_pg_mount.fruits SET name = 'guitar' WHERE fruit_id = 2""")

    head = get_current_head(sg_pg_conn, PG_MNT)
    new_head = commit(sg_pg_conn, PG_MNT, include_snap=include_snap, comment="test commit")

    # After commit, we should be switched to the new commit hash and there should be no differences.
    assert get_current_head(sg_pg_conn, PG_MNT) == new_head
    assert diff(sg_pg_conn, PG_MNT, 'fruits', snap_1=new_head, snap_2=None) == []
    assert get_all_snap_info(sg_pg_conn, PG_MNT, new_head)[2] == "test commit"

    # The diff between the old and the new snaps should be the same as in the previous test
    change = diff(sg_pg_conn, PG_MNT, 'fruits', snap_1=head, snap_2=new_head)

    # pk (no PK here so the whole row) -- 0 for INS -- extra non-PK cols
    assert change == [((3, 'mayonnaise'), 0, {"c": [], "v": []}),
                      # 1, apple deleted
                      ((1, 'apple'), 1, None),
                      # 2, orange deleted and 2, guitar added (PK (whole tuple) changed)
                      ((2, 'orange'), 1, None),
                      ((2, 'guitar'), 0, {'c': [], 'v': []})]
    assert diff(sg_pg_conn, PG_MNT, 'vegetables', snap_1=head, snap_2=new_head) == []


@pytest.mark.parametrize("include_snap", [True, False])
def test_commit_on_empty(include_snap, sg_pg_conn):
    init(sg_pg_conn, "output")

    with sg_pg_conn.cursor() as cur:
        cur.execute("""CREATE TABLE output.test AS SELECT * FROM test_pg_mount.fruits""")

    # Make sure the WAL changes get flushed anyway if we are only committing a snapshot.
    assert diff(sg_pg_conn, 'output', 'test', snap_1=get_current_head(sg_pg_conn, 'output'), snap_2=None) == True
    commit(sg_pg_conn, 'output', include_snap=include_snap)
    assert diff(sg_pg_conn, 'output', 'test', snap_1=get_current_head(sg_pg_conn, 'output'), snap_2=None) == []


@pytest.mark.parametrize("include_snap", [True, False])
def test_multiple_mountpoint_commit_diff(include_snap, sg_pg_mg_conn):
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")
        cur.execute("""DELETE FROM test_pg_mount.fruits WHERE name = 'apple'""")
        cur.execute("""UPDATE test_pg_mount.fruits SET name = 'guitar' WHERE fruit_id = 2""")
        cur.execute("""UPDATE test_mg_mount.stuff SET duration = 11 WHERE name = 'James'""")
    # Both mountpoints have pending changes if we commit the PG connection.
    sg_pg_mg_conn.commit()
    assert has_pending_changes(sg_pg_mg_conn, MG_MNT) is True
    assert has_pending_changes(sg_pg_mg_conn, PG_MNT) is True

    head = get_current_head(sg_pg_mg_conn, PG_MNT)
    mongo_head = get_current_head(sg_pg_mg_conn, MG_MNT)
    new_head = commit(sg_pg_mg_conn, PG_MNT, include_snap=include_snap)

    change = diff(sg_pg_mg_conn, PG_MNT, 'fruits', snap_1=head, snap_2=new_head)
    assert change == [((3, 'mayonnaise'), 0, {'c': [], 'v': []}),
                      ((1, 'apple'), 1, None),
                      ((2, 'orange'), 1, None),
                      ((2, 'guitar'), 0, {'c': [], 'v': []})]

    # PG has no pending changes, Mongo does
    assert get_current_head(sg_pg_mg_conn, MG_MNT) == mongo_head
    assert has_pending_changes(sg_pg_mg_conn, MG_MNT) is True
    assert has_pending_changes(sg_pg_mg_conn, PG_MNT) is False

    # Discard the commit to the mongodb
    checkout(sg_pg_mg_conn, MG_MNT, mongo_head)
    assert has_pending_changes(sg_pg_mg_conn, MG_MNT) is False
    assert has_pending_changes(sg_pg_mg_conn, PG_MNT) is False
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT duration from test_mg_mount.stuff WHERE name = 'James'""")
        assert cur.fetchall() == [(Decimal(2),)]

    # Update and commit
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""UPDATE test_mg_mount.stuff SET duration = 15 WHERE name = 'James'""")
    sg_pg_mg_conn.commit()
    assert has_pending_changes(sg_pg_mg_conn, MG_MNT) is True
    new_mongo_head = commit(sg_pg_mg_conn, MG_MNT, include_snap=include_snap)
    assert has_pending_changes(sg_pg_mg_conn, MG_MNT) is False
    assert has_pending_changes(sg_pg_mg_conn, PG_MNT) is False

    checkout(sg_pg_mg_conn, MG_MNT, mongo_head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT duration from test_mg_mount.stuff WHERE name = 'James'""")
        assert cur.fetchall() == [(Decimal(2),)]

    checkout(sg_pg_mg_conn, MG_MNT, new_mongo_head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT duration from test_mg_mount.stuff WHERE name = 'James'""")
        assert cur.fetchall() == [(Decimal(15),)]
    assert has_pending_changes(sg_pg_mg_conn, MG_MNT) is False
    assert has_pending_changes(sg_pg_mg_conn, PG_MNT) is False


def test_delete_all_diff(sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""DELETE FROM test_pg_mount.fruits""")
    sg_pg_conn.commit()
    assert has_pending_changes(sg_pg_conn, PG_MNT) is True
    expected_diff = [((1, 'apple'), 1, None), ((2, 'orange'), 1, None)]

    actual_diff = diff(sg_pg_conn, PG_MNT, 'fruits', get_current_head(sg_pg_conn, PG_MNT), None)
    print(actual_diff)
    assert actual_diff == expected_diff
    new_head = commit(sg_pg_conn, PG_MNT)
    assert has_pending_changes(sg_pg_conn, PG_MNT) is False
    assert diff(sg_pg_conn, PG_MNT, 'fruits', new_head, None) == []
    assert diff(sg_pg_conn, PG_MNT, 'fruits', get_snap_parent(sg_pg_conn, PG_MNT, new_head), new_head) == expected_diff


@pytest.mark.parametrize("include_snap", [True, False])
def test_diff_across_far_commits(include_snap, sg_pg_conn):
    head = get_current_head(sg_pg_conn, PG_MNT)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")
    commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

    with sg_pg_conn.cursor() as cur:
        cur.execute("""DELETE FROM test_pg_mount.fruits WHERE name = 'apple'""")
    commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

    with sg_pg_conn.cursor() as cur:
        cur.execute("""UPDATE test_pg_mount.fruits SET name = 'guitar' WHERE fruit_id = 2""")
    new_head = commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

    change = diff(sg_pg_conn, PG_MNT, 'fruits', head, new_head)
    assert change == [((3, 'mayonnaise'), 0, {'c': [], 'v': []}),
                      ((1, 'apple'), 1, None),
                      ((2, 'orange'), 1, None),  # The update is turned into an insert+delete
                      # since the PK has changed.
                      ((2, 'guitar'), 0, {'c': [], 'v': []})]
    change_agg = diff(sg_pg_conn, PG_MNT, 'fruits', head, new_head, aggregate=True)
    assert change_agg == (2, 2, 0)


def test_empty_diff_reuses_object(sg_pg_conn):
    head = get_current_head(sg_pg_conn, PG_MNT)
    head_1 = commit(sg_pg_conn, PG_MNT)

    table_meta_1 = get_table(sg_pg_conn, PG_MNT, 'fruits', head)
    table_meta_2 = get_table(sg_pg_conn, PG_MNT, 'fruits', head_1)

    assert table_meta_1 == table_meta_2
    assert len(table_meta_2) == 1  # Only SNAP stored even if we didn't ask for it, since this is just a pointer
    # to the previous version (which is a mount).
    assert table_meta_1[0][1] == 'SNAP'
