import operator
from functools import reduce
from hashlib import sha256

from splitgraph import SPLITGRAPH_META_SCHEMA
from splitgraph.core.fragment_manager import Digest
from test.splitgraph.conftest import OUTPUT, PG_DATA

TEST_ROWS = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine']
TEST_ROW_HASHES = [sha256(r.encode('ascii')).hexdigest() for r in TEST_ROWS]
TEST_ROW_HASHES_BYTES = [sha256(r.encode('ascii')).digest() for r in TEST_ROWS]
HASH_SUM = '9165f6a070d0f496e234dbf93e0af5f46eacc429aa2cd4f11c72733ab14c5429'


def test_digest_construction():
    assert Digest.from_hex(TEST_ROW_HASHES[0]).hex() == TEST_ROW_HASHES[0]
    assert Digest.from_memoryview(TEST_ROW_HASHES_BYTES[0]).hex() == TEST_ROW_HASHES[0]


def _sum_digests(ds):
    return reduce(operator.add, ds, Digest.empty())


def test_digest_sum():
    # Basic sum
    assert _sum_digests(map(Digest.from_hex, TEST_ROW_HASHES)).hex() == HASH_SUM

    # Hash(first five) + hash(next five) == sum
    assert (_sum_digests(map(Digest.from_hex, TEST_ROW_HASHES[:5]))
            + _sum_digests(map(Digest.from_hex, TEST_ROW_HASHES[5:]))).hex() == HASH_SUM

    # hash(last) + hash(first nine) == sum
    assert (Digest.from_hex(TEST_ROW_HASHES[-1])
            + _sum_digests(map(Digest.from_hex, TEST_ROW_HASHES[:-1]))).hex() == HASH_SUM


def test_digest_subtraction():
    sub_sum = _sum_digests(map(Digest.from_hex, TEST_ROW_HASHES[:5] + TEST_ROW_HASHES[6:]))
    assert (Digest.from_hex(HASH_SUM) - Digest.from_hex(TEST_ROW_HASHES[5])).hex() == sub_sum.hex()
    assert (Digest.from_hex(HASH_SUM) + Digest.from_hex(TEST_ROW_HASHES[5]) - Digest.from_hex(HASH_SUM)).hex() == \
           TEST_ROW_HASHES[5]


def test_digest_negation():
    sub_sum = _sum_digests(map(Digest.from_hex, TEST_ROW_HASHES[:5] + TEST_ROW_HASHES[6:]))
    neg_dig = -Digest.from_hex(TEST_ROW_HASHES[5])
    assert (sub_sum - neg_dig).hex() == HASH_SUM
    assert (Digest.from_hex(HASH_SUM) + neg_dig).hex() == sub_sum.hex()


def test_base_fragment_hashing(pg_repo_local):
    fruits = pg_repo_local.head.get_table('fruits')

    # Object hash is deterministic, so we get the same one every time.
    expected_object = 'o0e742bd2ea4927f5193a2c68f8d4c51ea018b1ef3e3005a50727147d2cf57b'
    assert fruits.objects == [expected_object]

    om = pg_repo_local.objects

    insertion_hash = om.calculate_fragment_insertion_hash(SPLITGRAPH_META_SCHEMA, expected_object).hex()
    assert insertion_hash == 'c01cce6c17bde5b999147b43c6133b11872298842a7388a0b82aee834e9454b0'

    schema_hash = sha256(str(fruits.table_schema).encode('ascii')).hexdigest()
    assert schema_hash == '3e022317e6dd31edb92c18a464dab55750ca16d5f4f111d383b1bdbc53ded5b5'

    full_hash = '0e742bd2ea4927f5193a2c68f8d4c51ea018b1ef3e3005a50727147d2cf57bc9'
    assert sha256((insertion_hash + schema_hash).encode('ascii')).hexdigest() == full_hash

    # Check the actual content hash of the final table -- in this case, since it only consists of one
    # object that doesn't replace anything, it should be equal to the insertion hash of that object.
    assert om.calculate_content_hash(pg_repo_local.to_schema(), 'fruits') == insertion_hash


def test_base_fragment_reused(pg_repo_local):
    fruits = pg_repo_local.head.get_table('fruits')

    # Create the same table, check that it gets linked to the same object.
    pg_repo_local.head.get_log()[0].checkout()
    pg_repo_local.run_sql(PG_DATA)
    pg_repo_local.commit()
    fruits_copy = pg_repo_local.head.get_table('fruits')

    assert fruits.image.image_hash != fruits_copy.image.image_hash
    assert fruits.objects == fruits_copy.objects

    assert len(pg_repo_local.objects.get_all_objects()) == 2  # The 'vegetables' table is included too.


def _make_test_table(repo):
    repo.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    for i in range(11):
        repo.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord('a') + i), i * 2))


def test_base_fragment_reused_chunking(local_engine_empty):
    # Check that if we split a table into chunks and some chunks are the same, they get assigned to the same objects.
    OUTPUT.init()
    base = OUTPUT.head
    _make_test_table(OUTPUT)
    OUTPUT.commit(chunk_size=5)
    table_1 = OUTPUT.head.get_table('test')
    # Table 1 produced 3 objects
    assert len(OUTPUT.objects.get_all_objects()) == 3

    # All chunks are the same
    base.checkout()
    _make_test_table(OUTPUT)
    OUTPUT.commit(chunk_size=5)
    table_2 = OUTPUT.head.get_table('test')
    assert len(OUTPUT.objects.get_all_objects()) == 3
    assert table_1.objects == table_2.objects

    # Insert something else into the middle chunk so that it's different. This will get conflated so won't get recorded
    # as an update.
    base.checkout()
    _make_test_table(OUTPUT)
    OUTPUT.run_sql("UPDATE test SET value_1 = 'UPDATED', value_2 = 42 WHERE key = 7")
    OUTPUT.commit(chunk_size=5)
    table_3 = OUTPUT.head.get_table('test')
    assert len(OUTPUT.objects.get_all_objects()) == 4
    # Table 3 reused the first and the last object but created a new one for the middle fragment.
    assert len(table_3.objects) == 3
    assert table_3.objects[0] == table_1.objects[0]
    assert table_3.objects[1] != table_1.objects[1]
    assert table_3.objects[2] == table_1.objects[2]


def test_diff_fragment_hashing(pg_repo_local):
    pg_repo_local.run_sql("DELETE FROM fruits WHERE fruit_id = 1")
    pg_repo_local.run_sql("UPDATE fruits SET name = 'kumquat' WHERE fruit_id = 2")
    pg_repo_local.commit()
    fruits_v2 = pg_repo_local.head.get_table('fruits')

    expected_object = 'oa4436fec80c4d3ee5052c4f954b555ddf2e139c108e6c854ebd86e201817fb'
    assert fruits_v2.objects == [expected_object]

    om = pg_repo_local.objects

    insertion_hash = om.calculate_fragment_insertion_hash(SPLITGRAPH_META_SCHEMA, expected_object)
    assert insertion_hash.hex() == '71a5c6d67b2466cb57cb8c05aa39400af342dfd4027ae5f333c97265710da844'

    # The homomorphic hash of all deleted rows: we can't yet access it directly but we can recalculate it
    # since we know which rows were deleted
    deleted_rows = ['(1,apple)', '(2,orange)']
    deletion_hash = reduce(operator.add, (Digest.from_hex(sha256(d.encode('ascii')).hexdigest()) for d in deleted_rows))

    # Fun fact: since we effectively replaced all rows in the original fragment, the deletion hash is the same
    # as the insertion hash of the original fragment.
    assert deletion_hash.hex() == 'c01cce6c17bde5b999147b43c6133b11872298842a7388a0b82aee834e9454b0'

    schema_hash = sha256(str(fruits_v2.table_schema).encode('ascii')).hexdigest()
    assert schema_hash == '3e022317e6dd31edb92c18a464dab55750ca16d5f4f111d383b1bdbc53ded5b5'

    # Full hash (less two last bytes) is the same as the object ID.
    full_hash = 'a4436fec80c4d3ee5052c4f954b555ddf2e139c108e6c854ebd86e201817fb28'
    assert sha256(((insertion_hash - deletion_hash).hex() + schema_hash).encode('ascii')).hexdigest() == full_hash

    # Check the actual content hash of the final table. Since the whole of the old fragment got replaced,
    # the hash should be old_insertion_hash - deletion_hash + new_insertion_hash == new_insertion_hash
    assert om.calculate_content_hash(pg_repo_local.to_schema(), 'fruits') == insertion_hash.hex()


def test_diff_fragment_hashing_long_chain(local_engine_empty):
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key TIMESTAMP PRIMARY KEY, val1 INTEGER, val2 VARCHAR, val3 NUMERIC)")
    OUTPUT.run_sql("INSERT INTO TEST VALUES ('2019-01-01 01:01:01.111', 1, 'one', 1.1),"
                   "('2019-01-02 02:02:02.222', 2, 'two', 2.2),"
                   "('2019-01-03 03:03:03.333', 3, 'three', 3.3),"
                   "('2019-01-04 04:04:04.444', 4, 'four', 4.4)")
    OUTPUT.commit()
    base = OUTPUT.head.get_table('test')

    OUTPUT.run_sql("DELETE FROM test WHERE key = '2019-01-03';INSERT INTO test VALUES ('2019-01-05', 5, 'five', 5.5)")
    OUTPUT.commit()
    v1 = OUTPUT.head.get_table('test')

    OUTPUT.run_sql("UPDATE test SET val2 = 'UPDATED', val1 = 42 WHERE key = '2019-01-02'")
    OUTPUT.commit()
    v2 = OUTPUT.head.get_table('test')

    OUTPUT.run_sql("UPDATE test SET val2 = 'UPDATED AGAIN', val1 = 43 WHERE key = '2019-01-02'")
    OUTPUT.commit()
    v3 = OUTPUT.head.get_table('test')

    om = OUTPUT.objects
    final_hash = OUTPUT.objects.calculate_content_hash(OUTPUT.to_schema(), 'test')

    schema_hash = sha256(str(base.table_schema).encode('ascii')).hexdigest()

    # Check that the final hash can be assembled out of intermediate objects' insertion and deletion hashes

    ins_hash_base = om.calculate_fragment_insertion_hash(SPLITGRAPH_META_SCHEMA, base.objects[0])
    assert 'o' + sha256((ins_hash_base.hex() + schema_hash).encode('ascii')).hexdigest()[:-2] == base.objects[0]

    ins_hash_v1 = om.calculate_fragment_insertion_hash(SPLITGRAPH_META_SCHEMA, v1.objects[0])
    # TODO:
    # * fix this
    # * add a test with some DIFFs being reused (eg make the same change to the same table several times)
    # * add the hashes into the object manifest (objects table)
    # * add snap hashing into the import routines (splitfiles/mounting)
    # * add tests for import routines
    # * figure out digest() not being available from the commandline

    del_hash_v1 = Digest.from_hex(sha256('(2019-01-03 03:03:03.333,3,three,3.3)'.encode('ascii')).hexdigest())
    assert 'o' + sha256(((ins_hash_v1 - del_hash_v1).hex() + schema_hash).encode('ascii')).hexdigest()[:-2] \
           == v1.objects[0]

    ins_hash_v2 = om.calculate_fragment_insertion_hash(SPLITGRAPH_META_SCHEMA, v2.objects[0])
    del_hash_v2 = Digest.from_hex(sha256('(2019-01-02 00:00:00.000,2,two,2.2)'.encode('ascii')).hexdigest())
    assert 'o' + sha256(((ins_hash_v2 - del_hash_v2).hex() + schema_hash).encode('ascii')).hexdigest()[:-2] \
           == v2.objects[0]

    ins_hash_v3 = om.calculate_fragment_insertion_hash(SPLITGRAPH_META_SCHEMA, v3.objects[0])
    del_hash_v3 = Digest.from_hex(sha256('(2019-01-02 00:00:00.000,42,UPDATED,2.2)'.encode('ascii')).hexdigest())
    assert 'o' + sha256(((ins_hash_v3 - del_hash_v3).hex() + schema_hash).encode('ascii')).hexdigest()[:-2] \
           == v3.objects[0]

    assert (ins_hash_base + ins_hash_v1 + ins_hash_v2 + ins_hash_v3 - del_hash_v1 - del_hash_v2 - del_hash_v3).hex() \
           == final_hash
