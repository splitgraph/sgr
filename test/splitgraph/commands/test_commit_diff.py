from datetime import date, datetime as dt
from decimal import Decimal

import pytest
from psycopg2.sql import SQL, Identifier
from splitgraph import SPLITGRAPH_META_SCHEMA, ResultShape, select
from test.splitgraph.conftest import OUTPUT, PG_DATA


def test_diff_head(pg_repo_local):
    pg_repo_local.run_sql("""INSERT INTO fruits VALUES (3, 'mayonnaise');
        DELETE FROM fruits WHERE name = 'apple'""")
    pg_repo_local.engine.commit()  # otherwise the audit trigger won't see this
    change = pg_repo_local.diff('fruits', image_1=pg_repo_local.head.image_hash, image_2=None)
    # Added (3, mayonnaise); Deleted (1, 'apple')
    assert sorted(change) == [((1, 'apple'), 1, None), ((3, 'mayonnaise'), 0, {})]


# Run some tests in multiple commit modes:
# * SNAP: store as a full table snapshot
# * DIFF: store as a single DIFF fragment
# * DIFF_SPLIT: split a DIFF into multiple based on SNAP boundaries
_COMMIT_MODES = ['SNAP', 'DIFF', 'DIFF_SPLIT']


def _commit(repo, mode, **kwargs):
    if mode == 'SNAP':
        return repo.commit(snap_only=True, split_changeset=False, **kwargs)
    if mode == 'DIFF':
        return repo.commit(snap_only=False, split_changeset=False, **kwargs)
    if mode == 'DIFF_SPLIT':
        return repo.commit(snap_only=False, split_changeset=True, **kwargs)


@pytest.mark.parametrize("mode", _COMMIT_MODES)
def test_commit_diff(mode, pg_repo_local):
    assert (pg_repo_local.to_schema(), 'fruits') in pg_repo_local.engine.get_tracked_tables()

    pg_repo_local.run_sql("""INSERT INTO fruits VALUES (3, 'mayonnaise');
        DELETE FROM fruits WHERE name = 'apple';
        UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2""")

    head = pg_repo_local.head.image_hash

    new_head = _commit(pg_repo_local, mode, comment="test commit")

    # After commit, we should be switched to the new commit hash and there should be no differences.
    assert pg_repo_local.head == new_head
    assert pg_repo_local.diff('fruits', image_1=new_head, image_2=None) == []

    # Test object structure
    table = pg_repo_local.head.get_table('fruits')
    assert table.table_schema == [(1, 'fruit_id', 'integer', False), (2, 'name', 'character varying', False)]

    obj = table.objects[0]
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


def test_commit_chunking(local_engine_empty):
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    for i in range(11):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord('a') + i), i * 2))
    head = OUTPUT.commit(chunk_size=5)

    # Should produce 3 objects: PK 1..5, PK 6..10, PK 11
    objects = head.get_table('test').objects
    assert len(objects) == 3
    object_meta = OUTPUT.objects.get_object_meta(objects)

    for i, obj in enumerate(objects):
        # Check object metadata
        min_key = i * 5 + 1
        max_key = min(i * 5 + 5, 11)

        assert object_meta[i] == \
               (obj,  # object ID
                'SNAP',  # full-table snapshot (not a delta compressed chunk)
                None,  # no parent
                '',  # no namespace
                8192,  # size reported by PG (can it change between platforms?)
                # index
                {'key': [min_key, max_key],
                 'value_1': [chr(ord('a') + min_key - 1), chr(ord('a') + max_key - 1)],
                 'value_2': [(min_key - 1) * 2, (max_key - 1) * 2]})

        # Check object contents
        assert local_engine_empty.run_sql(SQL("SELECT key FROM {}.{} ORDER BY key")
                                          .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(obj)),
                                          return_shape=ResultShape.MANY_ONE) == list(range(min_key, max_key + 1))


def test_commit_chunking_order(local_engine_empty):
    # Same but make sure the chunks order by PK for more efficient indexing
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    for i in range(10, -1, -1):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord('a') + i), i * 2))
    head = OUTPUT.commit(chunk_size=5)

    objects = head.get_table('test').objects
    assert len(objects) == 3
    for i, obj in enumerate(objects):
        min_key = i * 5 + 1
        max_key = min(i * 5 + 5, 11)
        assert local_engine_empty.run_sql(SQL("SELECT key FROM {}.{} ORDER BY key")
                                          .format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(obj)),
                                          return_shape=ResultShape.MANY_ONE) == list(range(min_key, max_key + 1))


def test_commit_diff_splitting(local_engine_empty):
    # Similar setup to the chunking test
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    for i in range(11):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord('a') + i), i * 2))
    head = OUTPUT.commit(chunk_size=5)

    # Store the IDs of the base fragments
    base_objects = head.get_table('test').objects

    # Create a change that affects multiple base fragments:
    # INSERT PK 0 (goes before all fragments)
    OUTPUT.run_sql("INSERT INTO test VALUES (0, 'zero', -1)")

    # UPDATE the first fragment and delete a value from it
    OUTPUT.run_sql("DELETE FROM test WHERE key = 4")
    OUTPUT.run_sql("UPDATE test SET value_1 = 'UPDATED' WHERE key = 5")

    # Second fragment -- UPDATE then DELETE (test that gets conflated into a single change)
    OUTPUT.run_sql("UPDATE test SET value_1 = 'UPDATED' WHERE key = 6")
    OUTPUT.run_sql("DELETE FROM test WHERE key = 6")

    # INSERT a value after the last fragment (PK 12) -- should become a new fragment
    OUTPUT.run_sql("INSERT INTO test VALUES (12, 'l', 22)")

    # Now check the objects that were created.
    new_head = OUTPUT.commit(split_changeset=True)
    new_head.checkout()
    new_objects = new_head.get_table('test').objects

    # We expect 5 objects: one with PK 0, one with PKs 4 and 5 (updating the first base fragment),
    # one with PK 6 (updating the second base fragment), one with PK 11 (same as the old fragment since it's
    # unchanged) and one with PK 12.

    # Check the old fragment with PK 11 is reused.
    assert new_objects[3] == base_objects[2]
    object_meta = {t[0]: t[1:] for t in OUTPUT.objects.get_object_meta(new_objects)}
    # The new fragments should be at the beginning and the end of the table objects' list.
    for new_object, expected in zip(new_objects,
                                    [('SNAP', None, '', 8192,
                                      {'key': [0, 0], 'value_1': ['zero', 'zero'], 'value_2': [-1, -1]}),
                                     ('DIFF', base_objects[0], '', 8192,
                                      # for value_1 we have old values for k=4,5 ('d', 'e') and new value
                                      # for k=5 ('UPDATED') included here; same for value_2.
                                      {'key': [4, 5], 'value_1': ['UPDATED', 'e'], 'value_2': [6, 8]}),
                                     ('DIFF', base_objects[1], '', 8192,
                                      # Turned into one deletion, old values included here
                                      {'key': [6, 6], 'value_1': ['f', 'f'], 'value_2': [10, 10]}),
                                     # Old fragment with just the pk=11
                                     ('SNAP', None, '', 8192,
                                      {'key': [11, 11], 'value_1': ['k', 'k'], 'value_2': [20, 20]}),
                                     ('SNAP', None, '', 8192,
                                      {'key': [12, 12], 'value_1': ['l', 'l'], 'value_2': [22, 22]})
                                     ]):
        assert object_meta[new_object] == expected

    # Check the contents of the newly created objects.
    assert OUTPUT.run_sql(select(new_objects[0])) == \
           [(0, 'zero', -1)]  # No deletions in this fragment, so no deletion flag. New inserted value only.
    assert OUTPUT.run_sql(select(new_objects[1])) == \
           [(True, 5, 'UPDATED', 8),  # upserted=True, key, new_value_1, new_value_2
            (False, 4, None, None)]  # upserted=False, key, None, None
    assert OUTPUT.run_sql(select(new_objects[2])) == \
           [(False, 6, None, None)]  # upserted=False, key, None, None
    # No need to check new_objects[3] (since it's the same as the old fragment)
    assert OUTPUT.run_sql(select(new_objects[4])) == \
           [(12, 'l', 22)]


def test_commit_diff_splitting_composite(local_engine_empty):
    # Test splitting works correctly on composite PKs (see a big comment in _extract_min_max_pks for the explanation).
    # Also test timestamp PKs at the same time.

    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key_1 TIMESTAMP, key_2 INTEGER, value VARCHAR, PRIMARY KEY (key_1, key_2))")
    for i in range(10):
        # Set up something like
        # 1/1/2019, 1, '1'
        # 1/1/2019, 2, '2'
        # 1/1/2019, 3, '3'
        # 2/1/2019, 1, '4'
        # 2/1/2019, 2, '5'
        # --- chunk boundary
        # 2/1/2019, 3, '6'
        # ...
        # 4/1/2019, 1, '10'
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)",
                       (dt(2019, 1, i // 3 + 1), i % 3 + 1, str(i)))
    head = OUTPUT.commit(chunk_size=5)
    base_objects = head.get_table('test').objects
    assert len(base_objects) == 2

    # INSERT at 1/1/2019, 4 -- should match first chunk even though it doesn't exist in it.
    OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (dt(2019, 1, 1), 4, 'NEW'))

    # UPDATE 2/1/2019, 2 -- if we index by PK columns individually, it's ambiguous where
    # this should fit (as both chunks span key_1=2/1/2019 and key_2=2) but it should go into the first
    # chunk.
    OUTPUT.run_sql("UPDATE test SET value = 'UPD' WHERE key_1 = %s AND key_2 = %s", (dt(2019, 1, 2), 2))

    # INSERT a new value that comes after all chunks
    OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (dt(2019, 1, 4), 2, 'NEW'))

    new_head = OUTPUT.commit(split_changeset=True)
    new_head.checkout()
    new_objects = new_head.get_table('test').objects

    assert len(new_objects) == 3
    # First chunk: based on the old first chunk, contains the INSERT (1/1/2019, 4) and the UPDATE (2/1/2019, 2)
    object_meta = {t[0]: t[1:] for t in OUTPUT.objects.get_object_meta(new_objects)}
    assert object_meta[new_objects[0]] == \
           ('DIFF', base_objects[0], '', 8192,
            {'key_1': ['2019-01-01T00:00:00', '2019-01-02T00:00:00'],
             # 'value' spans the old value (was '5'), the inserted value ('4') and the new updated value ('UPD').
             'key_2': [2, 4], 'value': ['4', 'UPD']})
    # The second chunk is reused.
    assert new_objects[1] == base_objects[1]
    # The third chunk is new, contains (4/1/2019, 2, NEW)
    assert object_meta[new_objects[2]] == \
           ('SNAP', None, '', 8192,
            {'key_1': ['2019-01-04T00:00:00', '2019-01-04T00:00:00'],
             'key_2': [2, 2], 'value': ['NEW', 'NEW']})


@pytest.mark.xfail(reason="This currently doesn't work: need to commit the table explicitly with snap_only=True. "
                          "PG DROP triggers are database-specific (we'd have to write a stored procedure that records "
                          "the name/schema that has been dropped since last commit and then use that at commit time "
                          "to see what should be recorded as SNAPs.")
def test_drop_recreate_produces_snap(pg_repo_local):
    # Drops both tables and creates them with the same schema -- check we detect that.
    pg_repo_local.run_sql(PG_DATA)

    # Check there are only SNAPs, no DIFFs.
    table = pg_repo_local.head.get_table('fruits')
    assert pg_repo_local.objects.get_object_meta(table.objects)[0][2] is None


@pytest.mark.parametrize("mode", _COMMIT_MODES)
def test_commit_diff_remote(mode, pg_repo_remote):
    # Rerun the previous test against the remote fixture to make sure it works without
    # dependencies on the get_engine()
    test_commit_diff(mode, pg_repo_remote)


@pytest.mark.parametrize("snap_only", [True, False])
def test_commit_on_empty(snap_only, pg_repo_local):
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test AS SELECT * FROM \"test/pg_mount\".fruits")

    # Make sure the pending changes get flushed anyway if we are only committing a snapshot.
    assert OUTPUT.diff('test', image_1=OUTPUT.head.image_hash, image_2=None) == True
    OUTPUT.commit(snap_only=snap_only)
    assert OUTPUT.diff('test', image_1=OUTPUT.head.image_hash, image_2=None) == []


@pytest.mark.parametrize("mode", _COMMIT_MODES)
def test_multiple_mountpoint_commit_diff(mode, pg_repo_local, mg_repo_local):
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
    new_head = _commit(pg_repo_local, mode)

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
    new_mongo_head = _commit(mg_repo_local, mode)
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


@pytest.mark.parametrize("mode", _COMMIT_MODES)
def test_diff_across_far_commits(mode, pg_repo_local):
    head = pg_repo_local.head.image_hash
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    _commit(pg_repo_local, mode)

    pg_repo_local.run_sql("DELETE FROM fruits WHERE name = 'apple'")
    _commit(pg_repo_local, mode)

    pg_repo_local.run_sql("UPDATE fruits SET name = 'guitar' WHERE fruit_id = 2")
    new_head = _commit(pg_repo_local, mode)

    change = pg_repo_local.diff('fruits', head, new_head)
    assert sorted(change) == \
           [((1, 'apple'), 1, None),
            ((2, 'guitar'), 0, {}),
            ((2, 'orange'), 1, None),
            ((3, 'mayonnaise'), 0, {})]
    change_agg = pg_repo_local.diff('fruits', head, new_head, aggregate=True)
    assert change_agg == (2, 2, 0)


@pytest.mark.parametrize("snap_only", [True, False])
def test_non_ordered_inserts(snap_only, pg_repo_local):
    head = pg_repo_local.head.image_hash
    pg_repo_local.run_sql("INSERT INTO fruits (name, fruit_id) VALUES ('mayonnaise', 3)")
    new_head = pg_repo_local.commit(snap_only=snap_only)

    change = pg_repo_local.diff('fruits', head, new_head)
    assert change == [((3, 'mayonnaise'), 0, {})]


@pytest.mark.parametrize("snap_only", [True, False])
def test_non_ordered_inserts_with_pk(snap_only, local_engine_empty):
    OUTPUT.init()
    OUTPUT.run_sql("""CREATE TABLE test
        (a int, 
         b int,
         c int,
         d varchar, PRIMARY KEY(a))""")
    head = OUTPUT.commit()

    OUTPUT.run_sql("INSERT INTO test (a, d, b, c) VALUES (1, 'four', 2, 3)")
    new_head = OUTPUT.commit(snap_only=snap_only)
    head.checkout()
    new_head.checkout()
    assert OUTPUT.run_sql("SELECT * FROM test") == [(1, 2, 3, 'four')]
    change = OUTPUT.diff('test', head, new_head)
    assert len(change) == 1
    assert change[0] == ((1, 2, 3, 'four'), 0, {})


def _write_multitype_dataset():
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

    OUTPUT.run_sql(
        """INSERT INTO test (b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
        VALUES(1, 2, 3, 3.54, 876.563452345, 1.23, 'test', 'testtesttesttest', 'testtesttesttesttesttesttest',
        B'001110010101010', '2013-11-02 17:30:52', '2013-02-04', true, '{ "a": 123 }',
        'Old Old Parr'::tsvector);""")

    # Write another row to test min/max indexing on the resulting object
    OUTPUT.run_sql(
        """INSERT INTO test (b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
        VALUES(15, 22, 1, -1.23, 9.8811, 0.23, 'abcd', '0testtesttesttes', '0testtesttesttesttesttesttes',
        B'111110011111111', '2016-01-01 01:01:05', '2011-11-11', false, '{ "b": 456 }',
        'AAA AAA Bb '::tsvector);""")

    new_head = OUTPUT.commit()
    # Check out the new image again to verify that writing the new image works
    new_head.checkout()

    return new_head


def test_various_types(local_engine_empty):
    new_head = _write_multitype_dataset()

    assert OUTPUT.run_sql("SELECT * FROM test") \
           == [(1, 1, 2, 3, Decimal('3.540'), 876.563, 1.23, 'test      ', 'testtesttesttest',
                'testtesttesttesttesttesttest', '001110010101010', dt(2013, 11, 2, 17, 30, 52),
                date(2013, 2, 4), True, {'a': 123}, "'Old' 'Parr'"),
               ((2, 15, 22, 1, Decimal('-1.230'), 9.8811, 0.23, 'abcd      ', '0testtesttesttes',
                 '0testtesttesttesttesttesttes', '111110011111111', dt(2016, 1, 1, 1, 1, 5),
                 date(2011, 11, 11), False, {'b': 456}, "'AAA' 'Bb'"))]

    object_id = new_head.get_table('test').objects[0]
    object_index = OUTPUT.objects.get_object_meta([object_id])[0][5]
    expected = {'a': [1, 2],
                'b': [1, 15],
                'c': [2, 22],
                'd': [1, 3],
                'e': ['-1.230', '3.540'],
                'f': [9.8811, 876.563],
                'g': [0.23, 1.23],
                'h': ['abcd      ', 'test      '],
                'i': ['0testtesttesttes', 'testtesttesttest'],
                'j': ['0testtesttesttesttesttesttes', 'testtesttesttesttesttesttest'],
                'l': ['2013-11-02T17:30:52', '2016-01-01T01:01:05'],
                'm': ['2011-11-11', '2013-02-04']}

    assert object_index == expected


def test_various_types_with_deletion_index(local_engine_empty):
    _write_multitype_dataset()
    # Insert a row, update a row and delete a row -- check the old rows are included in the index correctly.
    OUTPUT.run_sql(
        """INSERT INTO test (b, c, d, e, f, g, h, i, j, k, l, m, n, o, p)
        VALUES(16, 23, 2, 7.89, 10.01, 9.45, 'defg', '00esttesttesttes', '00esttesttesttesttesttesttes',
        B'111110011111111', '2016-02-01 01:01:05.123456', '2012-12-12', false, '{ "b": 789 }',
        'BBB BBB Cc '::tsvector);""")
    OUTPUT.run_sql("DELETE FROM test WHERE b = 15")
    OUTPUT.run_sql("UPDATE test SET m = '2019-01-01' WHERE m = '2013-02-04'")
    new_head = OUTPUT.commit()

    object_id = new_head.get_table('test').objects[0]
    object_index = OUTPUT.objects.get_object_meta([object_id])[0][5]
    # Deleted row for reference:
    # 15, 22, 1, -1.23, 9.8811, 0.23, 'abcd', '0testtesttesttes', '0testtesttesttesttesttesttes',
    #   B'111110011111111', '2016-01-01 01:01:05', '2011-11-11', false, '{ "b": 456 }',
    #   'AAA AAA Bb '::tsvector
    # Updated row for reference:
    # 1, 2, 3, 3.54, 876.563452345, 1.23, 'test', 'testtesttesttest', 'testtesttesttesttesttesttest',
    #   B'001110010101010', '2013-11-02 17:30:52', '2013-02-04', true, '{ "a": 123 }',
    #   'Old Old Parr'::tsvector

    # Make sure all rows are included since they were all affected + both old and new value for the update
    expected = {'a': [1, 2],  # this one is a sequence so is kinda broken
                'b': [1, 16],  # original values 1 (updated row), 15 (deleted row), 16 (inserted)
                'c': [2, 23],  # 2 (U), 22 (D), 23 (I)
                'd': [1, 3],  # 3 (U), 1 (D), 2 (I)
                'e': [-1.23, '7.890'],  # 3.54 (U), -1.23 (D), 7.89 (I) -- also wtf is up with types?
                'f': [9.8811, 876.563],  # 876.563 (U, numeric(5,3) so truncated), 9.8811 (D), 10.01 (I)
                'g': [0.23, 9.45],  # 1.23 (U), 0.23 (D), 9.45 (I)
                'h': ['abcd      ', 'test      '],  # test (U), abcd (D), defg (I)
                'i': ['00esttesttesttes', 'testtesttesttest'],  # test... (U), 0tes... (D), 00te... (I)
                # same as previous here
                'j': ['00esttesttesttesttesttesttes', 'testtesttesttesttesttesttest'],
                # 2013-11 (U), 2016-01 (D), 2016-02 (I)
                'l': ['2013-11-02T17:30:52', '2016-02-01T01:01:05.123456'],
                # 2013 (U, old value), 2016 (D), 2012 (I), 2019 (U, new value)
                'm': ['2011-11-11', '2019-01-01']}

    assert object_index == expected


def test_empty_diff_reuses_object(pg_repo_local):
    head_1 = pg_repo_local.commit()

    table_meta_1 = pg_repo_local.head.get_table('fruits').objects
    table_meta_2 = head_1.get_table('fruits').objects

    assert table_meta_1 == table_meta_2
    assert len(table_meta_2) == 1


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
