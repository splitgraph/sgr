from test.splitgraph.conftest import OUTPUT


def test_range_index_ordering_collation(local_engine_empty):
    # Test that range index gets min/max values of text columns using the "C" collation
    # (sort by byte values of characters) rather than anything else (e.g. in en-US
    # "a" comes before "B" even though "B" has a smaller ASCII code).

    OUTPUT.init()
    OUTPUT.run_sql(
        "CREATE TABLE test (key_1 INTEGER, key_2 VARCHAR,"
        " value_1 VARCHAR, value_2 INTEGER, PRIMARY KEY (key_1, key_2))"
    )

    OUTPUT.engine.run_sql_batch(
        "INSERT INTO test VALUES (%s, %s, %s, %s)",
        [
            (1, "ONE", "apple", 4),
            (1, "one", "ORANGE", 3),
            (2, "two", "banana", 2),
            (2, "TWO", "CUCUMBER", 1),
        ],
        schema=OUTPUT.to_schema(),
    )

    head = OUTPUT.commit()
    object_id = head.get_table("test").objects[0]

    assert OUTPUT.objects.get_object_meta([object_id])[object_id].object_index == {
        "range": {
            "$pk": [[1, "ONE"], [2, "two"]],
            "key_1": [1, 2],
            "key_2": ["ONE", "two"],
            "value_1": ["CUCUMBER", "banana"],
            "value_2": [1, 4],
        }
    }
