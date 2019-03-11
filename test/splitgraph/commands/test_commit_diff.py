from datetime import date, datetime as dt
from decimal import Decimal

import pytest
from test.splitgraph.conftest import OUTPUT, PG_DATA


def test_diff_head(pg_repo_local):
    pg_repo_local.run_sql("""INSERT INTO fruits VALUES (3, 'mayonnaise');
        DELETE FROM fruits WHERE name = 'apple'""")
    pg_repo_local.engine.commit()  # otherwise the audit trigger won't see this
    change = pg_repo_local.diff('fruits', image_1=pg_repo_local.head.image_hash, image_2=None)
    # Added (3, mayonnaise); Deleted (1, 'apple')
    assert change == [((3, 'mayonnaise'), 0, {}),
                      ((1, 'apple'), 1, None)]


@pytest.mark.parametrize("include_snap", [True, False])
def test_commit_diff(include_snap, pg_repo_local):
    assert (pg_repo_local.to_schema(), 'fruits') in pg_repo_local.engine.get_tracked_tables()

    pg_repo_local.run_sql("""INSERT INTO fruits VALUES (3, 'mayonnaise');
        DELETE FROM fruits WHERE name = 'apple';
        UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2""")

    head = pg_repo_local.head.image_hash
    new_head = pg_repo_local.commit(include_snap=include_snap, comment="test commit")

    # After commit, we should be switched to the new commit hash and there should be no differences.
    assert pg_repo_local.head == new_head
    assert pg_repo_local.diff('fruits', image_1=new_head, image_2=None) == []

    # Test object structure
    table = pg_repo_local.head.get_table('fruits')
    assert table.table_schema == [(1, 'fruit_id', 'integer', False), (2, 'name', 'character varying', False)]

    obj = table.get_object('DIFF')
    obj_meta = pg_repo_local.objects.get_object_meta([obj])[0]
    # Check object size has been written
    assert obj_meta[4] > 0

    assert new_head.comment == "test commit"
    change = pg_repo_local.diff('fruits', image_1=head, image_2=new_head)
    # pk (no PK here so the whole row) -- 0 for INS -- extra non-PK cols
    assert sorted(change) == [  # 1, apple deleted
        ((1, 'apple'), 1, None),
        # 2, orange deleted and 2, guitar added (PK (whole tuple) changed)
        ((2, 'guitar'), 0, {}),
        ((2, 'orange'), 1, None),
        ((3, 'mayonnaise'), 0, {})]

    assert pg_repo_local.diff('vegetables', image_1=head, image_2=new_head) == []


def test_commit_snap_only(pg_repo_local):
    pg_repo_local.run_sql("""INSERT INTO fruits VALUES (3, 'mayonnaise');
        DELETE FROM fruits WHERE name = 'apple';
        UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2""")
    pg_repo_local.commit(snap_only=True, comment="test commit")

    # Test object structure
    table = pg_repo_local.head.get_table('fruits')
    assert table.get_object('DIFF') is None
    assert table.get_object('SNAP') is not None


@pytest.mark.xfail(reason="This currently doesn't work: need to commit the table explicitly with snap_only=True. "
                          "PG DROP triggers are database-specific (we'd have to write a stored procedure that records "
                          "the name/schema that has been dropped since last commit and then use that at commit time "
                          "to see what should be recorded as SNAPs.")
def test_drop_recreate_produces_snap(pg_repo_local):
    # Drops both tables and creates them with the same schema -- check we detect that.
    pg_repo_local.run_sql(PG_DATA)

    # Check there are only SNAPs, no DIFFs.
    table = pg_repo_local.head.get_table('fruits')
    assert table.get_object('DIFF') is None
    assert table.get_object('SNAP') is not None


@pytest.mark.parametrize("include_snap", [True, False])
def test_commit_diff_remote(include_snap, pg_repo_remote):
    # Rerun the previous test against the remote fixture to make sure it works without
    # dependencies on the get_engine()
    test_commit_diff(include_snap, pg_repo_remote)


@pytest.mark.parametrize("include_snap", [True, False])
def test_commit_on_empty(include_snap, pg_repo_local):
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test AS SELECT * FROM \"test/pg_mount\".fruits")

    # Make sure the pending changes get flushed anyway if we are only committing a snapshot.
    assert OUTPUT.diff('test', image_1=OUTPUT.head.image_hash, image_2=None) == True
    OUTPUT.commit(include_snap=include_snap)
    assert OUTPUT.diff('test', image_1=OUTPUT.head.image_hash, image_2=None) == []


@pytest.mark.parametrize("include_snap", [True, False])
def test_multiple_mountpoint_commit_diff(include_snap, pg_repo_local, mg_repo_local):
    pg_repo_local.run_sql("""INSERT INTO fruits VALUES (3, 'mayonnaise');
        DELETE FROM fruits WHERE name = 'apple';
        UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2;""")
    mg_repo_local.run_sql("UPDATE stuff SET duration = 11 WHERE name = 'James'")
    # Both repositories have pending changes if we commit the PG connection.
    pg_repo_local.engine.commit()
    assert mg_repo_local.has_pending_changes()
    assert pg_repo_local.has_pending_changes()

    head = pg_repo_local.head.image_hash
    mongo_head = mg_repo_local.head
    new_head = pg_repo_local.commit(include_snap=include_snap)

    change = pg_repo_local.diff('fruits', image_1=head, image_2=new_head)
    assert sorted(change) == [((1, 'apple'), 1, None),
                              ((2, 'guitar'), 0, {}),
                              ((2, 'orange'), 1, None),
                              ((3, 'mayonnaise'), 0, {})]

    # PG has no pending changes, Mongo does
    assert mg_repo_local.head == mongo_head
    assert mg_repo_local.has_pending_changes()
    assert not pg_repo_local.has_pending_changes()

    # Discard the write to the mongodb
    mongo_head.checkout(force=True)
    assert not mg_repo_local.has_pending_changes()
    assert not pg_repo_local.has_pending_changes()
    assert mg_repo_local.run_sql("SELECT duration from stuff WHERE name = 'James'") == \
           [(Decimal(2),)]

    # Update and commit
    mg_repo_local.run_sql("UPDATE stuff SET duration = 15 WHERE name = 'James'")
    mg_repo_local.engine.commit()
    assert mg_repo_local.has_pending_changes()
    new_mongo_head = mg_repo_local.commit(include_snap=include_snap)
    assert not mg_repo_local.has_pending_changes()
    assert not pg_repo_local.has_pending_changes()

    mongo_head.checkout()
    assert mg_repo_local.run_sql("SELECT duration from stuff WHERE name = 'James'") == [(Decimal(2),)]

    new_mongo_head.checkout()
    assert mg_repo_local.run_sql("SELECT duration from stuff WHERE name = 'James'") == [(Decimal(15),)]
    assert not mg_repo_local.has_pending_changes()
    assert not pg_repo_local.has_pending_changes()


def test_delete_all_diff(pg_repo_local):
    pg_repo_local.run_sql("DELETE FROM fruits")
    pg_repo_local.engine.commit()
    assert pg_repo_local.has_pending_changes()
    expected_diff = [((1, 'apple'), 1, None), ((2, 'orange'), 1, None)]

    actual_diff = pg_repo_local.diff('fruits', pg_repo_local.head.image_hash, None)
    print(actual_diff)
    assert actual_diff == expected_diff
    new_head = pg_repo_local.commit()
    assert not pg_repo_local.has_pending_changes()
    assert pg_repo_local.diff('fruits', new_head, None) == []
    assert pg_repo_local.diff('fruits', new_head.parent_id, new_head) == expected_diff


@pytest.mark.parametrize("include_snap", [True, False])
def test_diff_across_far_commits(include_snap, pg_repo_local):
    head = pg_repo_local.head.image_hash
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    pg_repo_local.commit(include_snap=include_snap)

    pg_repo_local.run_sql("DELETE FROM fruits WHERE name = 'apple'")
    pg_repo_local.commit(include_snap=include_snap)

    pg_repo_local.run_sql("UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2")
    new_head = pg_repo_local.commit(include_snap=include_snap)

    change = pg_repo_local.diff('fruits', head, new_head)
    assert change == \
           [((1, 'apple'), 1, None),
            ((2, 'orange'), 1, None),
            ((3, 'mayonnaise'), 0, {}),
            ((2, 'guitar'), 0, {})]
    change_agg = pg_repo_local.diff('fruits', head, new_head, aggregate=True)
    assert change_agg == (2, 2, 0)


@pytest.mark.parametrize("include_snap", [True, False])
def test_non_ordered_inserts(include_snap, pg_repo_local):
    head = pg_repo_local.head.image_hash
    pg_repo_local.run_sql("INSERT INTO fruits (name, fruit_id) VALUES ('mayonnaise', 3)")
    new_head = pg_repo_local.commit(include_snap=include_snap)

    change = pg_repo_local.diff('fruits', head, new_head)
    assert change == [((3, 'mayonnaise'), 0, {})]


@pytest.mark.parametrize("include_snap", [True, False])
def test_non_ordered_inserts_with_pk(include_snap, local_engine_empty):
    OUTPUT.init()
    OUTPUT.run_sql("""CREATE TABLE test
        (a int, 
         b int,
         c int,
         d varchar, PRIMARY KEY(a))""")
    head = OUTPUT.commit()

    OUTPUT.run_sql("INSERT INTO test (a, d, b, c) VALUES (1, 'four', 2, 3)")
    new_head = OUTPUT.commit(include_snap=include_snap)
    head.checkout()
    new_head.checkout()
    assert OUTPUT.run_sql("SELECT * FROM test") == [(1, 2, 3, 'four')]
    change = OUTPUT.diff('test', head, new_head)
    assert len(change) == 1
    assert change[0] == ((1, 2, 3, 'four'), 0, {})


@pytest.mark.parametrize("include_snap", [True, False])
def test_various_types(include_snap, local_engine_empty):
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
    head.checkout()
    new_head.checkout()
    assert OUTPUT.run_sql("SELECT * FROM test") \
           == [(1, 1, 2, 3, Decimal('3.540'), 876.563, 1.23, 'test      ', 'testtesttesttest',
                'testtesttesttesttesttesttest', '001110010101010', dt(2013, 11, 2, 17, 30, 52),
                date(2013, 2, 4), True, {'a': 123}, "'Old' 'Parr'")]


def test_empty_diff_reuses_object(pg_repo_local):
    head_1 = pg_repo_local.commit()

    table_meta_1 = pg_repo_local.head.get_table('fruits').objects
    table_meta_2 = head_1.get_table('fruits').objects

    assert table_meta_1 == table_meta_2
    assert len(table_meta_2) == 1  # Only SNAP stored even if we didn't ask for it, since this is just a pointer
    # to the previous version (which is a mount).
    assert table_meta_1[0][1] == 'SNAP'


def test_update_packing_applying(pg_repo_local):
    # Set fruit_id to be the PK first so that an UPDATE operation is stored in the DIFF.
    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    old_head = pg_repo_local.commit()

    pg_repo_local.run_sql("UPDATE fruits SET name = 'pineapple' WHERE fruit_id = 1")
    new_head = pg_repo_local.commit()

    pg_repo_local.run_sql("UPDATE fruits SET name = 'kumquat' WHERE fruit_id = 2")
    v_new_head = pg_repo_local.commit()

    old_head.checkout()
    assert pg_repo_local.run_sql("SELECT * FROM fruits ORDER BY fruit_id") == [(1, 'apple'), (2, 'orange')]

    new_head.checkout()
    assert pg_repo_local.run_sql("SELECT * FROM fruits ORDER BY fruit_id") == [(1, 'pineapple'), (2, 'orange')]

    v_new_head.checkout()
    assert pg_repo_local.run_sql("SELECT * FROM fruits ORDER BY fruit_id") == [(1, 'pineapple'), (2, 'kumquat')]


def test_diff_staging_aggregation(pg_repo_local):
    # Test diff from HEAD~1 to the current staging area (accumulate actual DIFF object with the pending changes)
    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    old_head = pg_repo_local.commit()

    pg_repo_local.run_sql("UPDATE fruits SET name = 'pineapple' WHERE fruit_id = 1")
    pg_repo_local.commit()
    pg_repo_local.run_sql("UPDATE fruits SET name = 'mustard' WHERE fruit_id = 2")

    assert pg_repo_local.diff("fruits", old_head, None, aggregate=True) == (2, 2, 0)
    assert pg_repo_local.diff("fruits", old_head, None, aggregate=False) == \
           [((1, 'apple'), 1, None),
            ((2, 'orange'), 1, None),
            ((1, 'pineapple'), 0, {}),
            ((2, 'mustard'), 0, {})]


def test_diff_schema_change(pg_repo_local):
    # Test diff when there's been a schema change and so we stored an intermediate object
    # as a SNAP.
    old_head = pg_repo_local.head.image_hash
    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    pg_repo_local.commit()

    pg_repo_local.run_sql("UPDATE fruits SET name = 'pineapple' WHERE fruit_id = 1")
    after_update = pg_repo_local.commit()
    pg_repo_local.run_sql("UPDATE fruits SET name = 'mustard' WHERE fruit_id = 2")

    # Can't detect an UPDATE since there's been a schema change (old table has no PK)
    assert pg_repo_local.diff("fruits", old_head, after_update, aggregate=True) == (1, 1, 0)
    assert pg_repo_local.diff("fruits", old_head, after_update, aggregate=False) == \
           [((1, 'apple'), 1, None), ((1, 'pineapple'), 0, {})]

    # Again can't detect UPDATEs -- delete 2 rows, add two rows
    assert pg_repo_local.diff("fruits", old_head, None, aggregate=True) == (2, 2, 0)
    assert pg_repo_local.diff("fruits", old_head, None, aggregate=False) == [((1, 'apple'), 1, None),
                                                                             ((2, 'orange'), 1, None),
                                                                             ((1, 'pineapple'), 0, {}),
                                                                             ((2, 'mustard'), 0, {})]
