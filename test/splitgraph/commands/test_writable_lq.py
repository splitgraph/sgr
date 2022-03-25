from psycopg2.sql import SQL, Identifier

from splitgraph.hooks.data_source.base import WRITE_LOWER_PREFIX, WRITE_UPPER_PREFIX


def test_basic_writes_no_pks(pg_repo_local):
    table_name = "fruits"
    head = pg_repo_local.head
    head.checkout(layered=True)

    # Ensure that the table is now in the overlay/LQ mode
    assert pg_repo_local.is_overlay_view(table_name)

    table = head.get_table(table_name)
    assert len(table.objects) == 1

    #
    # Perform some basic writes and check overlay tables
    #

    # Insert a couple of new rows
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'banana')")
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'pear')")

    # Update a row existing in the lower table, and one existing only in the upper table (due to insert above)
    pg_repo_local.run_sql("UPDATE fruits SET name = 'mango' WHERE name = 'orange'")
    pg_repo_local.run_sql("UPDATE fruits SET name = 'watermelon' WHERE name = 'pear'")

    # Delete a row existing in the lower table, and one existing only in the upper (due to insert/update above)
    pg_repo_local.run_sql("DELETE FROM fruits WHERE fruit_id = 1")
    pg_repo_local.run_sql("DELETE FROM fruits WHERE name = 'watermelon'")

    # Won't affect the output of the view due to no PKs on the table
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'banana')")

    # No-OPs: update/delete rows that are neither in upper, nor in lower table
    pg_repo_local.run_sql("UPDATE fruits SET name = 'kumquat' WHERE fruit_id = 10")
    pg_repo_local.run_sql("DELETE FROM fruits WHERE fruit_id = 11")

    # Assert that the lower table has the old values intact
    assert pg_repo_local.run_sql(
        SQL("SELECT * FROM {}").format(Identifier(WRITE_LOWER_PREFIX + table_name))
    ) == [
        (1, "apple"),
        (2, "orange"),
    ]

    # Assert the upper table stores the pending writes
    assert pg_repo_local.run_sql(
        SQL("SELECT * FROM {}").format(Identifier(WRITE_UPPER_PREFIX + table_name))
    ) == [
        (3, "banana", True, 1),
        (4, "pear", True, 2),
        (2, "orange", False, 3),
        (2, "mango", True, 4),
        (4, "pear", False, 5),
        (4, "watermelon", True, 6),
        (1, "apple", False, 7),
        (4, "watermelon", False, 8),
        (3, "banana", True, 9),
    ]

    # Assert the correct result from the overlay view
    assert pg_repo_local.run_sql("SELECT * FROM fruits") == [
        (2, "mango"),
        (3, "banana"),
    ]

    #
    # Commit the pending changes to a new image, and assert expected contents in overlay components
    #

    new_head = pg_repo_local.commit()

    # Ensure new image is a child of the original head
    assert new_head.parent_id == head.image_hash

    table = new_head.get_table(table_name)

    # Ensure there is 1 new object
    assert len(table.objects) == 2

    # Ensure new object meta is expected
    new_object = pg_repo_local.objects.get_object_meta([table.objects[1]])[table.objects[1]]
    assert (
        new_object.deletion_hash
        == "928965083ebe4d9ec5c09b3686d42a7b96fa034b592ad72b96aa9aa8bf78976c"
    )
    assert (
        new_object.insertion_hash
        == "e743d2374b12cf458cc86fa0084b2f326dd137091d614092858f8cced5d862ad"
    )
    assert new_object.object_id == "od7426e7a54a2906204a6cbdc1050bf6f0e0bda631c07253de01fc280c687e5"
    assert new_object.rows_deleted == 4
    assert new_object.rows_inserted == 2

    # Assert the diff contents are compressed
    assert pg_repo_local.run_sql(
        SQL("SELECT * FROM splitgraph_meta.{}").format(Identifier(new_object.object_id))
    ) == [
        (2, "orange", False),
        (2, "mango", True),
        (4, "pear", False),
        (1, "apple", False),
        (4, "watermelon", False),
        (3, "banana", True),
    ]

    # Assert that the lower table now has the new values
    assert pg_repo_local.run_sql(
        SQL("SELECT * FROM {}").format(Identifier(WRITE_LOWER_PREFIX + table_name))
    ) == [
        (2, "mango"),
        (3, "banana"),
    ]

    # Assert the upper table is now empty, since we have just committed all pending changes
    assert (
        pg_repo_local.run_sql(
            SQL("SELECT * FROM {}").format(Identifier(WRITE_UPPER_PREFIX + table_name))
        )
        == []
    )

    # Assert the overlay view also shows the latest data
    assert pg_repo_local.run_sql("SELECT * FROM fruits") == [
        (2, "mango"),
        (3, "banana"),
    ]
