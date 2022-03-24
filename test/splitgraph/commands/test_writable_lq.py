from psycopg2.sql import SQL, Identifier

from splitgraph.hooks.data_source.base import WRITE_LOWER_PREFIX, WRITE_UPPER_PREFIX


def test_basic_writes_no_pks(pg_repo_local):
    table_name = "fruits"
    pg_repo_local.head.checkout(layered=True)

    # Ensure that the table is now in the overlay/LQ mode
    assert pg_repo_local.is_overlay_view(table_name)

    # Perform some basic writes

    # Insert a couple of new rows
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'banana')")
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'pear')")

    # Update a row existing in the lower table, and one existing only in the upper (due to insert above)
    pg_repo_local.run_sql("UPDATE fruits SET name = 'mango' WHERE name = 'orange'")
    pg_repo_local.run_sql("UPDATE fruits SET name = 'watermelon' WHERE name = 'pear'")

    # Delete a row existing in the lower table, and one existing only in the upper (due to insert/update above)
    pg_repo_local.run_sql("DELETE FROM fruits WHERE fruit_id = 1")
    pg_repo_local.run_sql("DELETE FROM fruits WHERE name = 'watermelon'")

    # Won't affect the output of the view due to no PKs on the table
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'banana')")

    # Assert the lower table has the old values intact
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
