import datetime
from decimal import Decimal
from test.splitgraph.conftest import prepare_lq_repo

import pytest
from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core.overlay import (
    WRITE_LOWER_PREFIX,
    WRITE_MERGED_PREFIX,
    WRITE_UPPER_PREFIX,
)


@pytest.mark.parametrize("ddn_layout", [False, True])
def test_surrogate_pks_proper_ordering(pg_repo_local, ddn_layout):
    table_name = "fruits"
    head = pg_repo_local.head
    head.checkout(layered=True, ddn_layout=ddn_layout)

    lower_table = table_name if ddn_layout else WRITE_LOWER_PREFIX + table_name
    overlay_table = WRITE_MERGED_PREFIX + table_name if ddn_layout else table_name

    original_fruits = [
        (1, "apple"),
        (2, "orange"),
    ]

    # Add rows such that actual min and max of the surrogate PKs are reversed compared to the non-surrogate PKs.
    # In other words, while the proper tuple ordering is (5, 'kiwi') < (10, 'mango'), when surrogate PKs are cast
    # as strings the proper ordering is reversed to '(10, mango)' <= '(5, kiwi)'.
    pg_repo_local.run_sql(
        SQL("INSERT INTO {} VALUES (5, 'kiwi'), (10, 'mango')").format(Identifier(overlay_table))
    )
    pg_repo_local.commit()

    new_fruits = [
        (5, "kiwi"),
        (10, "mango"),
    ]

    assert (
        pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(lower_table)))
        == original_fruits + new_fruits
    )

    pg_repo_local.run_sql(
        SQL("DELETE FROM {} WHERE name = 'kiwi' OR name = 'mango'").format(
            Identifier(overlay_table)
        )
    )
    pg_repo_local.commit()

    # Assert that surrogate PKs were properly ordered such that the insertion and deletion object got bundled into
    # non-singleton groups and canceled each other out.
    assert (
        pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(lower_table)))
        == original_fruits
    )


@pytest.mark.parametrize("ddn_layout", [False, True])
def test_basic_writes_no_pks(pg_repo_local, ddn_layout):
    table_name = "fruits"
    head = pg_repo_local.head
    head.checkout(layered=True, ddn_layout=ddn_layout)

    lower_table = table_name if ddn_layout else WRITE_LOWER_PREFIX + table_name
    upper_table = WRITE_UPPER_PREFIX + table_name
    overlay_table = WRITE_MERGED_PREFIX + table_name if ddn_layout else table_name

    # Ensure that the table is now in the overlay/LQ mode
    assert pg_repo_local.is_overlay_view(table_name)

    table = head.get_table(table_name)
    assert len(table.objects) == 1

    #
    # Perform some basic writes and check overlay tables
    #

    # Insert a couple of new rows
    pg_repo_local.run_sql(
        SQL("INSERT INTO {} VALUES (3, 'banana')").format(Identifier(overlay_table))
    )
    pg_repo_local.run_sql(
        SQL("INSERT INTO {} VALUES (4, 'pear')").format(Identifier(overlay_table))
    )

    # Update a row existing in the lower table, and one existing only in the upper table (due to insert above)
    pg_repo_local.run_sql(
        SQL("UPDATE {} SET name = 'mango' WHERE name = 'orange'").format(Identifier(overlay_table))
    )
    pg_repo_local.run_sql(
        SQL("UPDATE {} SET name = 'watermelon' WHERE name = 'pear'").format(
            Identifier(overlay_table)
        )
    )

    # Delete a row existing in the lower table, and one existing only in the upper (due to insert/update above)
    pg_repo_local.run_sql(
        SQL("DELETE FROM {} WHERE fruit_id = 1").format(Identifier(overlay_table))
    )
    pg_repo_local.run_sql(
        SQL("DELETE FROM {} WHERE name = 'watermelon'").format(Identifier(overlay_table))
    )

    # Won't affect the output of the view due to no PKs on the table
    pg_repo_local.run_sql(
        SQL("INSERT INTO {} VALUES (3, 'banana')").format(Identifier(overlay_table))
    )

    # No-OPs: update/delete rows that are neither in upper, nor in lower table
    pg_repo_local.run_sql(
        SQL("UPDATE {} SET name = 'kumquat' WHERE fruit_id = 10").format(Identifier(overlay_table))
    )
    pg_repo_local.run_sql(
        SQL("DELETE FROM {} WHERE fruit_id = 11").format(Identifier(overlay_table))
    )

    # Assert that the lower table has the old values intact
    assert pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(lower_table))) == [
        (1, "apple"),
        (2, "orange"),
    ]

    # Assert the upper table stores the pending writes as expected
    assert pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(upper_table))) == [
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
    assert pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(overlay_table))) == [
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
        SQL("SELECT * FROM {}.{}").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(new_object.object_id)
        )
    ) == [
        (2, "orange", False),
        (2, "mango", True),
        (4, "pear", False),
        (1, "apple", False),
        (4, "watermelon", False),
        (3, "banana", True),
    ]

    # Assert that the lower table now has the new values
    assert pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(lower_table))) == [
        (2, "mango"),
        (3, "banana"),
    ]

    # Assert the upper table is now empty, since we have just committed all pending changes
    assert pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(upper_table))) == []

    # Assert the overlay view also shows the latest data
    assert pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(overlay_table))) == [
        (2, "mango"),
        (3, "banana"),
    ]

    #
    # Lastly, ensure that the vegetables table/object has not been changed in any way
    #

    assert pg_repo_local.run_sql("SELECT * FROM vegetables") == [
        (1, "potato"),
        (2, "carrot"),
    ]

    table = new_head.get_table("vegetables")
    assert len(table.objects) == 1

    latest_object = pg_repo_local.objects.get_object_meta([table.objects[-1]])[table.objects[-1]]
    assert (
        latest_object.deletion_hash
        == "0000000000000000000000000000000000000000000000000000000000000000"
    )
    assert (
        latest_object.insertion_hash
        == "a276ab48c95f161ea7b3a05fba7c2eda03c533a1bb5453e6de1ac66576e6fb8c"
    )
    assert (
        latest_object.object_id == "ob474d04a80c611fc043e8303517ac168444dc7518af60e4ccc56b3b0986470"
    )
    assert latest_object.rows_deleted == 0
    assert latest_object.rows_inserted == 2

    # Assert the diff contents are compressed
    assert pg_repo_local.run_sql(
        SQL("SELECT * FROM {}.{}").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(latest_object.object_id)
        )
    ) == [(1, "potato", True), (2, "carrot", True)]


@pytest.mark.parametrize("ddn_layout", [False, True])
def test_basic_writes_with_pks(pg_repo_local, ddn_layout):
    table_name = "fruits"
    prepare_lq_repo(pg_repo_local, commit_after_every=False, include_pk=True)
    head = pg_repo_local.head
    head.checkout(layered=True, ddn_layout=ddn_layout)

    lower_table = table_name if ddn_layout else WRITE_LOWER_PREFIX + table_name
    upper_table = WRITE_UPPER_PREFIX + table_name
    overlay_table = WRITE_MERGED_PREFIX + table_name if ddn_layout else table_name

    # Ensure that the table is now in the overlay/LQ mode
    assert pg_repo_local.is_overlay_view(table_name)

    table = head.get_table(table_name)
    assert len(table.objects) == 2

    #
    # Perform some basic writes and check overlay tables
    #

    # Insert a couple of new rows. Due to not keeping track of column defaults in our images
    # we can not replicate them during the upper table creation. Consequently we must perform
    # full/explicit inserts for all columns, otherwise INSERT statements will be silent no-ops.
    #
    # Also, note that one has a PK conflict, but due to the overlay mechanism
    # it will simply result in an update without throwing an error.
    pg_repo_local.run_sql(
        SQL("INSERT INTO {} VALUES (3, 'banana', 1, '2022-01-01T12:00:00')").format(
            Identifier(overlay_table)
        )
    )
    pg_repo_local.run_sql(
        SQL("INSERT INTO {} VALUES (4, 'pear', 2, '2022-01-01T12:00:00')").format(
            Identifier(overlay_table)
        )
    )
    pg_repo_local.run_sql(
        SQL("INSERT INTO {} VALUES (5, 'orange', 3, '2022-01-01T12:00:00')").format(
            Identifier(overlay_table)
        )
    )
    pg_repo_local.run_sql(
        SQL("INSERT INTO {} VALUES (6, 'kiwi', 4, '2022-01-01T12:00:00')").format(
            Identifier(overlay_table)
        )
    )

    # Update a row existing in the lower table, and one existing only in the upper table (due to insert above)
    pg_repo_local.run_sql(
        SQL("UPDATE {} SET name = 'mango' WHERE name = 'guitar'").format(Identifier(overlay_table))
    )
    pg_repo_local.run_sql(
        SQL("UPDATE {} SET name = 'watermelon' WHERE name = 'kiwi'").format(
            Identifier(overlay_table)
        )
    )
    # Updating a PK will result in overwriting of the previous row with that PK, instead of an error
    pg_repo_local.run_sql(
        SQL("UPDATE {} SET fruit_id = 5, number = 100 WHERE fruit_id = 4").format(
            Identifier(overlay_table)
        )
    )

    # Delete a row existing in the lower table, and one existing only in the upper (due to insert/update above)
    pg_repo_local.run_sql(
        SQL("DELETE FROM {} WHERE fruit_id = 2").format(Identifier(overlay_table))
    )
    pg_repo_local.run_sql(
        SQL("DELETE FROM {} WHERE name = 'watermelon'").format(Identifier(overlay_table))
    )

    # No-OPs: update/delete rows that are neither in upper, nor in lower table
    pg_repo_local.run_sql(
        SQL("UPDATE {} SET name = 'kumquat' WHERE fruit_id = 10").format(Identifier(overlay_table))
    )
    pg_repo_local.run_sql(
        SQL("DELETE FROM {} WHERE fruit_id = 11").format(Identifier(overlay_table))
    )

    # Assert that the lower table has the old values intact
    assert pg_repo_local.run_sql(
        SQL("SELECT fruit_id, name FROM {}").format(Identifier(lower_table))
    ) == [
        (3, "mayonnaise"),
        (2, "guitar"),
    ]

    # Assert the upper table stores the pending writes as expected
    assert pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(upper_table))) == [
        (3, "banana", Decimal("1"), datetime.datetime(2022, 1, 1, 12, 0), True, 1),
        (4, "pear", Decimal("2"), datetime.datetime(2022, 1, 1, 12, 0), True, 2),
        (5, "orange", Decimal("3"), datetime.datetime(2022, 1, 1, 12, 0), True, 3),
        (6, "kiwi", Decimal("4"), datetime.datetime(2022, 1, 1, 12, 0), True, 4),
        (2, "guitar", Decimal("1"), datetime.datetime(2019, 1, 1, 12, 0), False, 5),
        (2, "mango", Decimal("1"), datetime.datetime(2019, 1, 1, 12, 0), True, 6),
        (6, "kiwi", Decimal("4"), datetime.datetime(2022, 1, 1, 12, 0), False, 7),
        (6, "watermelon", Decimal("4"), datetime.datetime(2022, 1, 1, 12, 0), True, 8),
        (4, "pear", Decimal("2"), datetime.datetime(2022, 1, 1, 12, 0), False, 9),
        (5, "pear", Decimal("100"), datetime.datetime(2022, 1, 1, 12, 0), True, 10),
        (2, "mango", Decimal("1"), datetime.datetime(2019, 1, 1, 12, 0), False, 11),
        (6, "watermelon", Decimal("4"), datetime.datetime(2022, 1, 1, 12, 0), False, 12),
    ]

    # Assert the correct result from the overlay view
    assert pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(overlay_table))) == [
        (3, "banana", Decimal("1"), datetime.datetime(2022, 1, 1, 12, 0)),
        (5, "pear", Decimal("100"), datetime.datetime(2022, 1, 1, 12, 0)),
    ]

    #
    # Commit the pending changes to a new image, and assert expected contents in overlay components
    #

    new_head = pg_repo_local.commit()

    # Ensure new image is a child of the original head
    assert new_head.parent_id == head.image_hash

    table = new_head.get_table(table_name)

    # Ensure there is 1 new object
    assert len(table.objects) == 3

    # Ensure new object meta is expected
    new_object = pg_repo_local.objects.get_object_meta([table.objects[2]])[table.objects[2]]
    assert (
        new_object.deletion_hash
        == "5c1c76c1732ba2d10a1aa8d02cbb0421a2bc3ddbc48d19a56f07752d86c74088"
    )
    assert (
        new_object.insertion_hash
        == "f2ec1445b0d83097e7ce53ef97ad7e3fb514c0f9468ea05bfd9cef18aa51c150"
    )
    assert new_object.object_id == "oa4f27a15ee296359b2b7f7a15200be712daedca74a513b44073b8f5bde4096"
    assert new_object.rows_deleted == 3
    assert new_object.rows_inserted == 2

    # Assert the diff contents are compressed
    assert pg_repo_local.run_sql(
        SQL("SELECT * FROM {}.{}").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(new_object.object_id)
        )
    ) == [
        (3, "banana", Decimal("1"), datetime.datetime(2022, 1, 1, 12, 0), True),
        (4, None, None, None, False),
        (5, "pear", Decimal("100"), datetime.datetime(2022, 1, 1, 12, 0), True),
        (2, None, None, None, False),
        (6, None, None, None, False),
    ]

    # Assert that the lower table now has the new values
    assert pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(lower_table))) == [
        (3, "banana", Decimal("1"), datetime.datetime(2022, 1, 1, 12, 0)),
        (5, "pear", Decimal("100"), datetime.datetime(2022, 1, 1, 12, 0)),
    ]

    # Assert the upper table is now empty, since we have just committed all pending changes
    assert pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(upper_table))) == []

    # Assert the overlay view also shows the latest data
    assert pg_repo_local.run_sql(SQL("SELECT * FROM {}").format(Identifier(overlay_table))) == [
        (3, "banana", Decimal("1"), datetime.datetime(2022, 1, 1, 12, 0)),
        (5, "pear", Decimal("100"), datetime.datetime(2022, 1, 1, 12, 0)),
    ]

    #
    # Lastly, ensure that the vegetables table/object has not been changed in any way
    #

    assert pg_repo_local.run_sql("SELECT * FROM vegetables") == [
        (2, "carrot"),
        (3, "celery"),
    ]

    table = new_head.get_table("vegetables")
    assert len(table.objects) == 2

    latest_object = pg_repo_local.objects.get_object_meta([table.objects[-1]])[table.objects[-1]]
    assert (
        latest_object.deletion_hash
        == "3610f29d91ac88766ecec7faeb389832e0228f2bc5f37d1bfcf5cce3fd4b6e29"
    )
    assert (
        latest_object.insertion_hash
        == "f674879fae139c98bdbb9b8457f3a84bb7362bee9f655012d9bcc57757a5d765"
    )
    assert (
        latest_object.object_id == "of42d8f0637c9a9dfeda7e67a7775f1e99e94de09b9ddc17f1be622591ed196"
    )
    assert latest_object.rows_deleted == 1
    assert latest_object.rows_inserted == 1

    # Assert the diff contents are compressed
    assert pg_repo_local.run_sql(
        SQL("SELECT * FROM {}.{}").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(latest_object.object_id)
        )
    ) == [(3, "celery", True), (1, None, False)]
