from datetime import date, datetime as dt
from decimal import Decimal

import pytest

from test.splitgraph.conftest import PG_MNT, MG_MNT, OUTPUT


def test_diff_head(local_engine_with_pg):
    PG_MNT.run_sql("""INSERT INTO fruits VALUES (3, 'mayonnaise');
        DELETE FROM fruits WHERE name = 'apple'""")
    local_engine_with_pg.commit()  # otherwise the audit trigger won't see this
    head = PG_MNT.get_head()
    change = PG_MNT.diff('fruits', image_1=head, image_2=None)
    # Added (3, mayonnaise); Deleted (1, 'apple')
    assert change == [((3, 'mayonnaise'), 0, {'c': [], 'v': []}),
                      ((1, 'apple'), 1, None)]


@pytest.mark.parametrize("include_snap", [True, False])
def test_commit_diff(include_snap, local_engine_with_pg):
    PG_MNT.run_sql("""INSERT INTO fruits VALUES (3, 'mayonnaise');
        DELETE FROM fruits WHERE name = 'apple';
        UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2""")

    head = PG_MNT.get_head()
    new_head = PG_MNT.commit(include_snap=include_snap, comment="test commit")

    # After commit, we should be switched to the new commit hash and there should be no differences.
    assert PG_MNT.get_head() == new_head
    assert PG_MNT.diff('fruits', image_1=new_head, image_2=None) == []
    assert PG_MNT.get_image(new_head).comment == "test commit"

    change = PG_MNT.diff('fruits', image_1=head, image_2=new_head)
    # pk (no PK here so the whole row) -- 0 for INS -- extra non-PK cols
    assert change == [  # 1, apple deleted
        ((1, 'apple'), 1, None),
        # 2, orange deleted and 2, guitar added (PK (whole tuple) changed)
        ((2, 'guitar'), 0, {'c': [], 'v': []}),
        ((2, 'orange'), 1, None),
        ((3, 'mayonnaise'), 0, {"c": [], "v": []})]
    assert PG_MNT.diff('vegetables', image_1=head, image_2=new_head) == []


@pytest.mark.parametrize("include_snap", [True, False])
def test_commit_on_empty(include_snap, local_engine_with_pg):
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test AS SELECT * FROM \"test/pg_mount\".fruits")

    # Make sure the pending changes get flushed anyway if we are only committing a snapshot.
    assert OUTPUT.diff('test', image_1=OUTPUT.get_head(), image_2=None) == True
    OUTPUT.commit(include_snap=include_snap)
    assert OUTPUT.diff('test', image_1=OUTPUT.get_head(), image_2=None) == []


@pytest.mark.parametrize("include_snap", [True, False])
def test_multiple_mountpoint_commit_diff(include_snap, local_engine_with_pg_and_mg):
    PG_MNT.run_sql("""INSERT INTO fruits VALUES (3, 'mayonnaise');
        DELETE FROM fruits WHERE name = 'apple';
        UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2;""")
    MG_MNT.run_sql("UPDATE stuff SET duration = 11 WHERE name = 'James'")
    # Both repositories have pending changes if we commit the PG connection.
    local_engine_with_pg_and_mg.commit()
    assert MG_MNT.has_pending_changes()
    assert PG_MNT.has_pending_changes()

    head = PG_MNT.get_head()
    mongo_head = MG_MNT.get_head()
    new_head = PG_MNT.commit(include_snap=include_snap)

    change = PG_MNT.diff('fruits', image_1=head, image_2=new_head)
    assert change == [((1, 'apple'), 1, None),
                      ((2, 'guitar'), 0, {'c': [], 'v': []}),
                      ((2, 'orange'), 1, None),
                      ((3, 'mayonnaise'), 0, {'c': [], 'v': []})]

    # PG has no pending changes, Mongo does
    assert MG_MNT.get_head() == mongo_head
    assert MG_MNT.has_pending_changes()
    assert not PG_MNT.has_pending_changes()

    # Discard the write to the mongodb
    MG_MNT.checkout(mongo_head, force=True)
    assert not MG_MNT.has_pending_changes()
    assert not PG_MNT.has_pending_changes()
    assert MG_MNT.run_sql("SELECT duration from stuff WHERE name = 'James'") == \
           [(Decimal(2),)]

    # Update and commit
    MG_MNT.run_sql("UPDATE stuff SET duration = 15 WHERE name = 'James'")
    local_engine_with_pg_and_mg.commit()
    assert MG_MNT.has_pending_changes()
    new_mongo_head = MG_MNT.commit(include_snap=include_snap)
    assert not MG_MNT.has_pending_changes()
    assert not PG_MNT.has_pending_changes()

    MG_MNT.checkout(mongo_head)
    assert MG_MNT.run_sql("SELECT duration from stuff WHERE name = 'James'") == [(Decimal(2),)]

    MG_MNT.checkout(new_mongo_head)
    assert MG_MNT.run_sql("SELECT duration from stuff WHERE name = 'James'") == [(Decimal(15),)]
    assert not MG_MNT.has_pending_changes()
    assert not PG_MNT.has_pending_changes()


def test_delete_all_diff(local_engine_with_pg):
    PG_MNT.run_sql("DELETE FROM fruits")
    local_engine_with_pg.commit()
    assert PG_MNT.has_pending_changes()
    expected_diff = [((1, 'apple'), 1, None), ((2, 'orange'), 1, None)]

    actual_diff = PG_MNT.diff('fruits', PG_MNT.get_head(), None)
    print(actual_diff)
    assert actual_diff == expected_diff
    new_head = PG_MNT.commit()
    assert not PG_MNT.has_pending_changes()
    assert PG_MNT.diff('fruits', new_head, None) == []
    assert PG_MNT.diff('fruits', PG_MNT.get_image(new_head).parent_id, new_head) == expected_diff


@pytest.mark.parametrize("include_snap", [True, False])
def test_diff_across_far_commits(include_snap, local_engine_with_pg):
    head = PG_MNT.get_head()
    PG_MNT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    PG_MNT.commit(include_snap=include_snap)

    PG_MNT.run_sql("DELETE FROM fruits WHERE name = 'apple'")
    PG_MNT.commit(include_snap=include_snap)

    PG_MNT.run_sql("UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2")
    new_head = PG_MNT.commit(include_snap=include_snap)

    change = PG_MNT.diff('fruits', head, new_head)
    assert change == [((3, 'mayonnaise'), 0, {'c': [], 'v': []}),
                      ((1, 'apple'), 1, None),
                      # The update is turned into an insert+delete since the PK has changed.
                      # Diff sorts it so that the insert comes first, but we'll be applying all deletes first anyway.
                      ((2, 'guitar'), 0, {'c': [], 'v': []}),
                      ((2, 'orange'), 1, None)]
    change_agg = PG_MNT.diff('fruits', head, new_head, aggregate=True)
    assert change_agg == (2, 2, 0)


@pytest.mark.parametrize("include_snap", [True, False])
def test_non_ordered_inserts(include_snap, local_engine_with_pg):
    head = PG_MNT.get_head()
    PG_MNT.run_sql("INSERT INTO fruits (name, fruit_id) VALUES ('mayonnaise', 3)")
    new_head = PG_MNT.commit(include_snap=include_snap)

    change = PG_MNT.diff('fruits', head, new_head)
    assert change == [((3, 'mayonnaise'), 0, {'c': [], 'v': []})]


@pytest.mark.parametrize("include_snap", [True, False])
def test_non_ordered_inserts_with_pk(include_snap, local_engine_with_pg):
    OUTPUT.init()
    OUTPUT.run_sql("""CREATE TABLE test
        (a int, 
         b int,
         c int,
         d varchar, PRIMARY KEY(a))""")
    head = OUTPUT.commit()

    OUTPUT.run_sql("INSERT INTO test (a, d, b, c) VALUES (1, 'four', 2, 3)")
    new_head = OUTPUT.commit(include_snap=include_snap)
    OUTPUT.checkout(head)
    OUTPUT.checkout(new_head)
    assert OUTPUT.run_sql("SELECT * FROM test") == [(1, 2, 3, 'four')]
    change = OUTPUT.diff('test', head, new_head)
    assert len(change) == 1
    assert change[0][0] == (1,)
    assert change[0][1] == 0
    assert sorted(zip(change[0][2]['c'], change[0][2]['v'])) == [('b', 2), ('c', 3), ('d', 'four')]


@pytest.mark.parametrize("include_snap", [True, False])
def test_various_types(include_snap, local_engine_with_pg):
    # Schema/data copied from the wal2json tests
    OUTPUT.init()
    OUTPUT.run_sql("""CREATE TABLE test
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
    head = OUTPUT.commit()

    OUTPUT.run_sql(
        """INSERT INTO test (b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
        VALUES(1, 2, 3, 3.54, 876.563452345, 1.23, 'test', 'testtesttesttest', 'testtesttesttesttesttesttest',
        B'001110010101010', '2013-11-02 17:30:52', '2013-02-04', true, '{ "a": 123 }',
        'Old Old Parr'::tsvector);""")
    new_head = OUTPUT.commit(include_snap=include_snap)
    OUTPUT.checkout(head)
    OUTPUT.checkout(new_head)
    assert OUTPUT.run_sql("SELECT * FROM test") \
           == [(1, 1, 2, 3, Decimal('3.540'), 876.563, 1.23, 'test      ', 'testtesttesttest',
                'testtesttesttesttesttesttest', '001110010101010', dt(2013, 11, 2, 17, 30, 52),
                date(2013, 2, 4), True, {'a': 123}, "'Old' 'Parr'")]


def test_empty_diff_reuses_object(local_engine_with_pg):
    head = PG_MNT.get_head()
    head_1 = PG_MNT.commit()

    table_meta_1 = PG_MNT.get_image(head).get_table('fruits').objects
    table_meta_2 = PG_MNT.get_image(head_1).get_table('fruits').objects

    assert table_meta_1 == table_meta_2
    assert len(table_meta_2) == 1  # Only SNAP stored even if we didn't ask for it, since this is just a pointer
    # to the previous version (which is a mount).
    assert table_meta_1[0][1] == 'SNAP'


def test_update_packing_applying(local_engine_with_pg):
    # Set fruit_id to be the PK first so that an UPDATE operation is stored in the DIFF.
    PG_MNT.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    old_head = PG_MNT.commit()

    PG_MNT.run_sql("UPDATE fruits SET name = 'pineapple' WHERE fruit_id = 1")
    new_head = PG_MNT.commit()
    PG_MNT.checkout(old_head)
    assert PG_MNT.run_sql("SELECT * FROM fruits WHERE fruit_id = 1") == [(1, 'apple')]

    PG_MNT.checkout(new_head)
    assert PG_MNT.run_sql("SELECT * FROM fruits WHERE fruit_id = 1") == [(1, 'pineapple')]


def test_diff_staging_aggregation(local_engine_with_pg):
    # Test diff from HEAD~1 to the current staging area (accumulate actual DIFF object with the pending changes)
    PG_MNT.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    old_head = PG_MNT.commit()

    PG_MNT.run_sql("UPDATE fruits SET name = 'pineapple' WHERE fruit_id = 1")
    PG_MNT.commit()
    PG_MNT.run_sql("UPDATE fruits SET name = 'mustard' WHERE fruit_id = 2")

    assert PG_MNT.diff("fruits", old_head, None, aggregate=True) == (0, 0, 2)
    assert PG_MNT.diff("fruits", old_head, None, aggregate=False) == [((1,), 2, {'c': ['name'], 'v': ['pineapple']}),
                                                                      ((2,), 2, {'c': ['name'], 'v': ['mustard']})]


def test_diff_schema_change(local_engine_with_pg):
    # Test diff when there's been a schema change and so we stored an intermediate object
    # as a SNAP.
    old_head = PG_MNT.get_head()
    PG_MNT.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    PG_MNT.commit()

    PG_MNT.run_sql("UPDATE fruits SET name = 'pineapple' WHERE fruit_id = 1")
    after_update = PG_MNT.commit()
    PG_MNT.run_sql("UPDATE fruits SET name = 'mustard' WHERE fruit_id = 2")

    # Can't detect an UPDATE since there's been a schema change (old table has no PK)
    assert PG_MNT.diff("fruits", old_head, after_update, aggregate=True) == (1, 1, 0)
    assert PG_MNT.diff("fruits", old_head, after_update, aggregate=False) == \
           [((1, 'apple'), 1, None), ((1, 'pineapple'), 0, {'c': [], 'v': []})]

    # Again can't detect UPDATEs -- delete 2 rows, add two rows
    assert PG_MNT.diff("fruits", old_head, None, aggregate=True) == (2, 2, 0)
    assert PG_MNT.diff("fruits", old_head, None, aggregate=False) == [((1, 'apple'), 1, None),
                                                                       ((2, 'orange'), 1, None),
                                                                       ((1, 'pineapple'), 0, {'c': [], 'v': []}),
                                                                       ((2, 'mustard'), 0, {'c': [], 'v': []})]
