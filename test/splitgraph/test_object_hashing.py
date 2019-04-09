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
