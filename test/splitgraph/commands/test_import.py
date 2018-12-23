from splitgraph._data.objects import get_existing_objects, get_downloaded_objects
from splitgraph.core.engine import cleanup_objects, get_current_repositories
from splitgraph.core.repository import import_table_from_remote
from splitgraph.engine import get_engine, switch_engine
from test.splitgraph.conftest import PG_MNT, OUTPUT, REMOTE_ENGINE


def _setup_dataset():
    OUTPUT.init()
    OUTPUT.run_sql("""CREATE TABLE test (id integer, name varchar);
        INSERT INTO test VALUES (1, 'test')""")
    OUTPUT.commit()
    OUTPUT.run_sql("INSERT INTO test VALUES (2, 'test2')")
    return OUTPUT.commit()


def test_import_basic(local_engine_with_pg):
    # Create a new schema and import 'fruits' from the mounted PG table.
    OUTPUT.init()
    head = OUTPUT.get_head()

    # todo maybe flip the import tables -- or make it part of the Image class instead
    PG_MNT.import_tables(tables=['fruits'], target_repository=OUTPUT, target_tables=['imported_fruits'])

    assert OUTPUT.run_sql("SELECT * FROM imported_fruits") == PG_MNT.run_sql("SELECT * FROM fruits")
    new_head = OUTPUT.get_head()

    assert new_head != head
    assert OUTPUT.get_image(new_head).parent_id == head


def test_import_preserves_existing_tables(local_engine_with_pg):
    # Create a new schema and import 'fruits' from the mounted PG table.
    head = _setup_dataset()
    PG_MNT.import_tables(tables=['fruits'], target_repository=OUTPUT, target_tables=['imported_fruits'])
    new_head = OUTPUT.get_head()

    OUTPUT.checkout(head)
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'test')
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'imported_fruits')

    OUTPUT.checkout(new_head)
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'test')
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'imported_fruits')


def test_import_preserves_pending_changes(local_engine_with_pg):
    OUTPUT.init()
    OUTPUT.run_sql("""CREATE TABLE test (id integer, name varchar);
            INSERT INTO test VALUES (1, 'test')""")
    head = OUTPUT.commit()
    OUTPUT.run_sql("INSERT INTO test VALUES (2, 'test2')")
    changes = get_engine().get_pending_changes(OUTPUT.to_schema(), 'test')

    PG_MNT.import_tables(tables=['fruits'], target_repository=OUTPUT, target_tables=['imported_fruits'])
    new_head = OUTPUT.get_head()

    assert OUTPUT.get_image(new_head).parent_id == head
    assert changes == OUTPUT.engine.get_pending_changes(OUTPUT.to_schema(), 'test')


def test_import_multiple_tables(local_engine_with_pg):
    OUTPUT.init()
    head = OUTPUT.get_head()
    PG_MNT.import_tables(tables=[], target_repository=OUTPUT, target_tables=[])

    for table_name in ['fruits', 'vegetables']:
        assert OUTPUT.run_sql("SELECT * FROM %s" % table_name) == PG_MNT.run_sql("SELECT * FROM %s" % table_name)

    new_head = OUTPUT.get_head()
    assert new_head != head
    assert OUTPUT.get_image(new_head).parent_id == head


def test_import_from_remote(local_engine_empty, remote_engine):
    # Start with a clean repo -- add a table to output to see if it's preserved.
    head = _setup_dataset()

    assert len(get_downloaded_objects()) == 2
    assert len(get_existing_objects()) == 2
    assert local_engine_empty.get_all_tables(OUTPUT.to_schema()) == ['test']

    # Import the 'fruits' table from the origin.

    # todo make sure PG_MNT is actually pointing to the remote driver
    with switch_engine(REMOTE_ENGINE):
        remote_head = PG_MNT.get_head()
    import_table_from_remote('remote_engine', PG_MNT, ['fruits'], remote_head, OUTPUT, target_tables=[])
    new_head = OUTPUT.get_head()

    # Check that the table now exists in the output, is committed and there's no trace of the cloned repo.
    # Also clean up the unused objects to make sure that the newly cloned table is still recorded.
    assert sorted(local_engine_empty.get_all_tables(OUTPUT.to_schema())) == ['fruits', 'test']
    cleanup_objects()
    assert len(get_current_repositories()) == 1
    OUTPUT.checkout(head)
    assert local_engine_empty.table_exists(OUTPUT.to_schema(), 'test')
    assert not local_engine_empty.table_exists(OUTPUT.to_schema(), 'fruits')

    OUTPUT.checkout(new_head)
    assert local_engine_empty.table_exists(OUTPUT.to_schema(), 'test')
    assert local_engine_empty.table_exists(OUTPUT.to_schema(), 'fruits')

    assert OUTPUT.run_sql("SELECT * FROM fruits") \
           == remote_engine.run_sql("SELECT * FROM \"test/pg_mount\".fruits")


def test_import_and_update(local_engine_empty, remote_engine):
    OUTPUT.init()
    head = OUTPUT.get_head()
    with switch_engine(REMOTE_ENGINE):
        remote_head = PG_MNT.get_head()
    # Import the 'fruits' table from the origin.
    import_table_from_remote('remote_engine', PG_MNT, ['fruits'], remote_head, OUTPUT, target_tables=[])
    new_head = OUTPUT.get_head()

    OUTPUT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    new_head_2 = OUTPUT.commit()

    OUTPUT.checkout(head)
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'fruits')

    OUTPUT.checkout(new_head)
    assert OUTPUT.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange')]

    OUTPUT.checkout(new_head_2)
    assert OUTPUT.run_sql("SELECT * FROM fruits") == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
