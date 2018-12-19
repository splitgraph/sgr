from datetime import date, datetime as dt
from decimal import Decimal

import pytest

from splitgraph import init
from splitgraph.commands import diff, commit, checkout
from splitgraph.commands.diff import has_pending_changes
from splitgraph.commands.info import get_image, get_table
from splitgraph.commands.tagging import get_current_head
from test.splitgraph.conftest import PG_MNT, MG_MNT, OUTPUT


def test_diff_head(local_engine_with_pg):
    local_engine_with_pg.run_sql("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise');
        DELETE FROM "test/pg_mount".fruits WHERE name = 'apple'""")
    local_engine_with_pg.commit()  # otherwise the audit trigger won't see this
    head = get_current_head(PG_MNT)
    change = diff(PG_MNT, 'fruits', image_1=head, image_2=None)
    # Added (3, mayonnaise); Deleted (1, 'apple')
    assert change == [((3, 'mayonnaise'), 0, {'c': [], 'v': []}),
                      ((1, 'apple'), 1, None)]


@pytest.mark.parametrize("include_snap", [True, False])
def test_commit_diff(include_snap, local_engine_with_pg):
    local_engine_with_pg.run_sql("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise');
        DELETE FROM "test/pg_mount".fruits WHERE name = 'apple';
        UPDATE "test/pg_mount".fruits SET name = 'guitar' WHERE fruit_id = 2""")

    head = get_current_head(PG_MNT)
    new_head = commit(PG_MNT, include_snap=include_snap, comment="test commit")

    # After commit, we should be switched to the new commit hash and there should be no differences.
    assert get_current_head(PG_MNT) == new_head
    assert diff(PG_MNT, 'fruits', image_1=new_head, image_2=None) == []
    assert get_image(PG_MNT, new_head).comment == "test commit"

    change = diff(PG_MNT, 'fruits', image_1=head, image_2=new_head)
    # pk (no PK here so the whole row) -- 0 for INS -- extra non-PK cols
    assert change == [  # 1, apple deleted
        ((1, 'apple'), 1, None),
        # 2, orange deleted and 2, guitar added (PK (whole tuple) changed)
        ((2, 'guitar'), 0, {'c': [], 'v': []}),
        ((2, 'orange'), 1, None),
        ((3, 'mayonnaise'), 0, {"c": [], "v": []})]
    assert diff(PG_MNT, 'vegetables', image_1=head, image_2=new_head) == []


@pytest.mark.parametrize("include_snap", [True, False])
def test_commit_on_empty(include_snap, local_engine_with_pg):
    init(OUTPUT)

    local_engine_with_pg.run_sql("CREATE TABLE output.test AS SELECT * FROM \"test/pg_mount\".fruits",
                                 return_shape=None)

    # Make sure the pending changes get flushed anyway if we are only committing a snapshot.
    assert diff(OUTPUT, 'test', image_1=get_current_head(OUTPUT), image_2=None) == True
    commit(OUTPUT, include_snap=include_snap)
    assert diff(OUTPUT, 'test', image_1=get_current_head(OUTPUT), image_2=None) == []


@pytest.mark.parametrize("include_snap", [True, False])
def test_multiple_mountpoint_commit_diff(include_snap, local_engine_with_pg_and_mg):
    local_engine_with_pg_and_mg.run_sql("""INSERT INTO "test/pg_mount".fruits VALUES (3, 'mayonnaise');
        DELETE FROM "test/pg_mount".fruits WHERE name = 'apple';
        UPDATE "test/pg_mount".fruits SET name = 'guitar' WHERE fruit_id = 2;
        UPDATE test_mg_mount.stuff SET duration = 11 WHERE name = 'James'""")
    # Both mountpoints have pending changes if we commit the PG connection.
    local_engine_with_pg_and_mg.commit()
    assert has_pending_changes(MG_MNT) is True
    assert has_pending_changes(PG_MNT) is True

    head = get_current_head(PG_MNT)
    mongo_head = get_current_head(MG_MNT)
    new_head = commit(PG_MNT, include_snap=include_snap)

    change = diff(PG_MNT, 'fruits', image_1=head, image_2=new_head)
    assert change == [((1, 'apple'), 1, None),
                      ((2, 'guitar'), 0, {'c': [], 'v': []}),
                      ((2, 'orange'), 1, None),
                      ((3, 'mayonnaise'), 0, {'c': [], 'v': []})]

    # PG has no pending changes, Mongo does
    assert get_current_head(MG_MNT) == mongo_head
    assert has_pending_changes(MG_MNT) is True
    assert has_pending_changes(PG_MNT) is False

    # Discard the write to the mongodb
    checkout(MG_MNT, mongo_head, force=True)
    assert has_pending_changes(MG_MNT) is False
    assert has_pending_changes(PG_MNT) is False
    assert local_engine_with_pg_and_mg.run_sql("SELECT duration from test_mg_mount.stuff WHERE name = 'James'") == \
           [(Decimal(2),)]

    # Update and commit
    local_engine_with_pg_and_mg.run_sql("UPDATE test_mg_mount.stuff SET duration = 15 WHERE name = 'James'",
                                        return_shape=None)
    local_engine_with_pg_and_mg.commit()
    assert has_pending_changes(MG_MNT) is True
    new_mongo_head = commit(MG_MNT, include_snap=include_snap)
    assert has_pending_changes(MG_MNT) is False
    assert has_pending_changes(PG_MNT) is False

    checkout(MG_MNT, mongo_head)
    assert local_engine_with_pg_and_mg.run_sql("SELECT duration from test_mg_mount.stuff WHERE name = 'James'") \
           == [(Decimal(2),)]

    checkout(MG_MNT, new_mongo_head)
    assert local_engine_with_pg_and_mg.run_sql("SELECT duration from test_mg_mount.stuff WHERE name = 'James'") \
           == [(Decimal(15),)]
    assert has_pending_changes(MG_MNT) is False
    assert has_pending_changes(PG_MNT) is False


def test_delete_all_diff(local_engine_with_pg):
    local_engine_with_pg.run_sql("DELETE FROM \"test/pg_mount\".fruits")
    local_engine_with_pg.commit()
    assert has_pending_changes(PG_MNT) is True
    expected_diff = [((1, 'apple'), 1, None), ((2, 'orange'), 1, None)]

    actual_diff = diff(PG_MNT, 'fruits', get_current_head(PG_MNT), None)
    print(actual_diff)
    assert actual_diff == expected_diff
    new_head = commit(PG_MNT)
    assert has_pending_changes(PG_MNT) is False
    assert diff(PG_MNT, 'fruits', new_head, None) == []
    assert diff(PG_MNT, 'fruits', get_image(PG_MNT, new_head).parent_id, new_head) == expected_diff


@pytest.mark.parametrize("include_snap", [True, False])
def test_diff_across_far_commits(include_snap, local_engine_with_pg):
    head = get_current_head(PG_MNT)
    local_engine_with_pg.run_sql("INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')")
    commit(PG_MNT, include_snap=include_snap)

    local_engine_with_pg.run_sql("DELETE FROM \"test/pg_mount\".fruits WHERE name = 'apple'")
    commit(PG_MNT, include_snap=include_snap)

    local_engine_with_pg.run_sql("UPDATE \"test/pg_mount\".fruits SET name = 'guitar' WHERE fruit_id = 2",
                                 return_shape=None)
    new_head = commit(PG_MNT, include_snap=include_snap)

    change = diff(PG_MNT, 'fruits', head, new_head)
    assert change == [((3, 'mayonnaise'), 0, {'c': [], 'v': []}),
                      ((1, 'apple'), 1, None),
                      # The update is turned into an insert+delete since the PK has changed.
                      # Diff sorts it so that the insert comes first, but we'll be applying all deletes first anyway.
                      ((2, 'guitar'), 0, {'c': [], 'v': []}),
                      ((2, 'orange'), 1, None)]
    change_agg = diff(PG_MNT, 'fruits', head, new_head, aggregate=True)
    assert change_agg == (2, 2, 0)


@pytest.mark.parametrize("include_snap", [True, False])
def test_non_ordered_inserts(include_snap, local_engine_with_pg):
    head = get_current_head(PG_MNT)
    local_engine_with_pg.run_sql("INSERT INTO \"test/pg_mount\".fruits (name, fruit_id) VALUES ('mayonnaise', 3)",
                                 return_shape=None)
    new_head = commit(PG_MNT, include_snap=include_snap)

    change = diff(PG_MNT, 'fruits', head, new_head)
    assert change == [((3, 'mayonnaise'), 0, {'c': [], 'v': []})]


@pytest.mark.parametrize("include_snap", [True, False])
def test_non_ordered_inserts_with_pk(include_snap, local_engine_with_pg):
    init(OUTPUT)
    local_engine_with_pg.run_sql("""CREATE TABLE output.test
        (a int, 
         b int,
         c int,
         d varchar, PRIMARY KEY(a))""")
    local_engine_with_pg.commit()
    head = commit(OUTPUT)

    local_engine_with_pg.run_sql("INSERT INTO output.test (a, d, b, c) VALUES (1, 'four', 2, 3)")
    local_engine_with_pg.commit()
    new_head = commit(OUTPUT, include_snap=include_snap)
    checkout(OUTPUT, head)
    checkout(OUTPUT, new_head)
    assert local_engine_with_pg.run_sql("SELECT * FROM output.test") == [(1, 2, 3, 'four')]
    change = diff(OUTPUT, 'test', head, new_head)
    assert len(change) == 1
    assert change[0][0] == (1,)
    assert change[0][1] == 0
    assert sorted(zip(change[0][2]['c'], change[0][2]['v'])) == [('b', 2), ('c', 3), ('d', 'four')]


@pytest.mark.parametrize("include_snap", [True, False])
def test_various_types(include_snap, local_engine_with_pg):
    # Schema/data copied from the wal2json tests
    init(OUTPUT)
    local_engine_with_pg.run_sql("""CREATE TABLE output.test
        (a smallserial, 
         b smallint,
         c int,
         d bigint,
         e numeric(5,3),
         f real not null,
         g double precision,
         h char(10),
         i varchar(30),
         j text,
         k bit varying(20),
         l timestamp,
         m date,
         n boolean not null,
         o json,
         p tsvector, PRIMARY KEY(b, c, d));""")
    local_engine_with_pg.commit()
    head = commit(OUTPUT)

    local_engine_with_pg.run_sql(
        """INSERT INTO output.test (b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
        VALUES(1, 2, 3, 3.54, 876.563452345, 1.23, 'test', 'testtesttesttest', 'testtesttesttesttesttesttest',
        B'001110010101010', '2013-11-02 17:30:52', '2013-02-04', true, '{ "a": 123 }',
        'Old Old Parr'::tsvector);""")
    local_engine_with_pg.commit()
    new_head = commit(OUTPUT, include_snap=include_snap)
    checkout(OUTPUT, head)
    checkout(OUTPUT, new_head)
    assert local_engine_with_pg.run_sql("SELECT * FROM output.test") \
           == [(1, 1, 2, 3, Decimal('3.540'), 876.563, 1.23, 'test      ', 'testtesttesttest',
                'testtesttesttesttesttesttest', '001110010101010', dt(2013, 11, 2, 17, 30, 52),
                date(2013, 2, 4), True, {'a': 123}, "'Old' 'Parr'")]


def test_empty_diff_reuses_object(local_engine_with_pg):
    head = get_current_head(PG_MNT)
    head_1 = commit(PG_MNT)

    table_meta_1 = get_table(PG_MNT, 'fruits', head)
    table_meta_2 = get_table(PG_MNT, 'fruits', head_1)

    assert table_meta_1 == table_meta_2
    assert len(table_meta_2) == 1  # Only SNAP stored even if we didn't ask for it, since this is just a pointer
    # to the previous version (which is a mount).
    assert table_meta_1[0][1] == 'SNAP'


def test_update_packing_applying(local_engine_with_pg):
    # Set fruit_id to be the PK first so that an UPDATE operation is stored in the DIFF.
    local_engine_with_pg.run_sql("ALTER TABLE \"test/pg_mount\".fruits ADD PRIMARY KEY (fruit_id)")
    old_head = commit(PG_MNT)

    local_engine_with_pg.run_sql("UPDATE \"test/pg_mount\".fruits SET name = 'pineapple' WHERE fruit_id = 1",
                                 return_shape=None)
    new_head = commit(PG_MNT)
    checkout(PG_MNT, old_head)
    assert local_engine_with_pg.run_sql("SELECT * FROM \"test/pg_mount\".fruits WHERE fruit_id = 1") == [(1, 'apple')]

    checkout(PG_MNT, new_head)
    assert local_engine_with_pg.run_sql("SELECT * FROM \"test/pg_mount\".fruits WHERE fruit_id = 1") == \
           [(1, 'pineapple')]


def test_diff_staging_aggregation(local_engine_with_pg):
    # Test diff from HEAD~1 to the current staging area (accumulate actual DIFF object with the pending changes)
    local_engine_with_pg.run_sql("ALTER TABLE \"test/pg_mount\".fruits ADD PRIMARY KEY (fruit_id)")
    old_head = commit(PG_MNT)

    local_engine_with_pg.run_sql("UPDATE \"test/pg_mount\".fruits SET name = 'pineapple' WHERE fruit_id = 1",
                                 return_shape=None)
    commit(PG_MNT)
    local_engine_with_pg.run_sql("UPDATE \"test/pg_mount\".fruits SET name = 'mustard' WHERE fruit_id = 2",
                                 return_shape=None)

    assert diff(PG_MNT, "fruits", old_head, None, aggregate=True) == (0, 0, 2)
    assert diff(PG_MNT, "fruits", old_head, None, aggregate=False) == [((1,), 2, {'c': ['name'], 'v': ['pineapple']}),
                                                                       ((2,), 2, {'c': ['name'], 'v': ['mustard']})]


def test_diff_schema_change(local_engine_with_pg):
    # Test diff when there's been a schema change and so we stored an intermediate object
    # as a SNAP.
    old_head = get_current_head(PG_MNT)
    local_engine_with_pg.run_sql("ALTER TABLE \"test/pg_mount\".fruits ADD PRIMARY KEY (fruit_id)")
    commit(PG_MNT)

    local_engine_with_pg.run_sql("UPDATE \"test/pg_mount\".fruits SET name = 'pineapple' WHERE fruit_id = 1",
                                 return_shape=None)
    after_update = commit(PG_MNT)
    local_engine_with_pg.run_sql("UPDATE \"test/pg_mount\".fruits SET name = 'mustard' WHERE fruit_id = 2",
                                 return_shape=None)

    # Can't detect an UPDATE since there's been a schema change (old table has no PK)
    assert diff(PG_MNT, "fruits", old_head, after_update, aggregate=True) == (1, 1, 0)
    assert diff(PG_MNT, "fruits", old_head, after_update, aggregate=False) == \
           [((1, 'apple'), 1, None), ((1, 'pineapple'), 0, {'c': [], 'v': []})]

    # Again can't detect UPDATEs -- delete 2 rows, add two rows
    assert diff(PG_MNT, "fruits", old_head, None, aggregate=True) == (2, 2, 0)
    assert diff(PG_MNT, "fruits", old_head, None, aggregate=False) == [((1, 'apple'), 1, None),
                                                                       ((2, 'orange'), 1, None),
                                                                       ((1, 'pineapple'), 0, {'c': [], 'v': []}),
                                                                       ((2, 'mustard'), 0, {'c': [], 'v': []})]
