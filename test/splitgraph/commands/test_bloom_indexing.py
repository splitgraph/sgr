from unittest import mock

import pytest

from splitgraph.core.bloom import _prepare_bloom_quals
from test.splitgraph.conftest import OUTPUT


@pytest.mark.parametrize(
    "test_case",
    [
        # Basic case: equality gets left alone
        ([[("a", "=", 5)]], [[("a", mock.ANY, mock.ANY)]]),
        # a = 5 or b > 6: since b > 6 might be true and we don't know it, this collapses
        # into nothing
        ([[("a", "=", 5), ("b", ">", 6)]], []),
        # a = 5 and b > 6: no matter whether b > 6 is true, if a can never be = 5,
        # this statement can still be false
        ([[("a", "=", 5)], [("b", ">", 6)]], [[("a", mock.ANY, mock.ANY)]]),
    ],
)
def test_bloom_qual_preprocessing(test_case):
    case, expected = test_case
    assert _prepare_bloom_quals(case) == expected


def test_bloom_index_structure(local_engine_empty):
    OUTPUT.init()
    OUTPUT.run_sql("CREATE TABLE test (key INTEGER PRIMARY KEY, value_1 VARCHAR, value_2 INTEGER)")
    # Insert 26 rows with value_1 spanning a-z
    for i in range(26):
        OUTPUT.run_sql("INSERT INTO test VALUES (%s, %s, %s)", (i + 1, chr(ord("a") + i), i * 2))
    head = OUTPUT.commit(extra_indexes={"bloom": {"value_1": {"size": 16}}})

    objects = head.get_table("test").objects
    object_meta = OUTPUT.objects.get_object_meta(objects)

    index = object_meta[objects[0]].index
    assert "bloom" in index

    # The bloom index used k=4, note that the formula for optimal k is
    # m(ln2)/n = 16 * 8 (filter size in bits) * 0.693 / 26 (distinct items)
    # = 3.41 which rounds up to 4
    assert index["bloom"] == {"value_1": [4, mock.ANY]}

    # Test indexing two columns with different parameters
    # Delete one row so that the object doesn't get reused and the index gets written again.
    OUTPUT.run_sql("DELETE FROM test WHERE key = 26")
    head = OUTPUT.commit(
        snap_only=True,
        extra_indexes={"bloom": {"value_1": {"size": 16}, "value_2": {"probability": 0.01}}},
    )

    objects = head.get_table("test").objects
    object_meta = OUTPUT.objects.get_object_meta(objects)

    index = object_meta[objects[0]].index
    assert "bloom" in index

    # For fixed probability, we have k = -log2(p) = 6.64 (up to 7)
    # and filter size = -n * ln(p) / ln(2)**2 = 249.211 bits rounded up to 30 bytes
    # which in base64 is represented with ceil(30/3*4) = 40 bytes.
    assert index["bloom"] == {"value_1": [4, mock.ANY], "value_2": [7, mock.ANY]}
    assert len(index["bloom"]["value_2"][1]) == 40
