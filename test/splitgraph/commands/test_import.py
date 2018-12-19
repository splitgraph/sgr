from splitgraph import init
from splitgraph._data.objects import get_existing_objects, get_downloaded_objects
from splitgraph.commands import checkout, commit, import_tables
from splitgraph.commands.importing import import_table_from_remote
from splitgraph.commands.info import get_image
from splitgraph.commands.misc import cleanup_objects
from splitgraph.commands.repository import get_current_repositories
from splitgraph.commands.tagging import get_current_head
from splitgraph.engine import get_engine, switch_engine
from test.splitgraph.conftest import PG_MNT, OUTPUT, REMOTE_ENGINE


def _setup_dataset(engine):
    init(OUTPUT)
    engine.run_sql("""CREATE TABLE output.test (id integer, name varchar);
        INSERT INTO output.test VALUES (1, 'test')""")
    commit(OUTPUT)
    engine.run_sql("INSERT INTO output.test VALUES (2, 'test2')")
    return commit(OUTPUT)


def test_import_basic(local_engine_with_pg):
    # Create a new schema and import 'fruits' from the mounted PG table.
    init(OUTPUT)
    head = get_current_head(OUTPUT)
    import_tables(repository=PG_MNT, tables=['fruits'], target_repository=OUTPUT, target_tables=['imported_fruits'])

    assert local_engine_with_pg.run_sql("SELECT * FROM output.imported_fruits") \
           == local_engine_with_pg.run_sql("SELECT * FROM \"test/pg_mount\".fruits")
    new_head = get_current_head(OUTPUT)

    assert new_head != head
    assert get_image(OUTPUT, new_head).parent_id == head


def test_import_preserves_existing_tables(local_engine_with_pg):
    # Create a new schema and import 'fruits' from the mounted PG table.
    head = _setup_dataset(local_engine_with_pg)

    import_tables(repository=PG_MNT, tables=['fruits'], target_repository=OUTPUT, target_tables=['imported_fruits'])

    new_head = get_current_head(OUTPUT)

    checkout(OUTPUT, head)
    engine = get_engine()
    assert engine.table_exists(OUTPUT.to_schema(), 'test')
    assert not engine.table_exists(OUTPUT.to_schema(), 'imported_fruits')

    checkout(OUTPUT, new_head)
    assert engine.table_exists(OUTPUT.to_schema(), 'test')
    assert engine.table_exists(OUTPUT.to_schema(), 'imported_fruits')


def test_import_preserves_pending_changes(local_engine_with_pg):
    init(OUTPUT)
    local_engine_with_pg.run_sql("""CREATE TABLE output.test (id integer, name varchar);
            INSERT INTO output.test VALUES (1, 'test')""")
    head = commit(OUTPUT)
    local_engine_with_pg.run_sql("INSERT INTO output.test VALUES (2, 'test2')")
    changes = get_engine().get_pending_changes(OUTPUT.to_schema(), 'test')

    import_tables(repository=PG_MNT, tables=['fruits'], target_repository=OUTPUT, target_tables=['imported_fruits'])
    new_head = get_current_head(OUTPUT)

    assert get_image(OUTPUT, new_head).parent_id == head
    assert changes == get_engine().get_pending_changes(OUTPUT.to_schema(), 'test')


def test_import_multiple_tables(local_engine_with_pg):
    init(OUTPUT)
    head = get_current_head(OUTPUT)
    import_tables(repository=PG_MNT, tables=[], target_repository=OUTPUT, target_tables=[])

    for table_name in ['fruits', 'vegetables']:
        assert local_engine_with_pg.run_sql("SELECT * FROM output.%s" % table_name) \
               == local_engine_with_pg.run_sql("SELECT * FROM \"test/pg_mount\".%s" % table_name)

    new_head = get_current_head(OUTPUT)
    assert new_head != head
    assert get_image(OUTPUT, new_head).parent_id == head


def test_import_from_remote(local_engine_empty, remote_engine):
    # Start with a clean repo -- add a table to output to see if it's preserved.
    head = _setup_dataset(local_engine_empty)

    assert len(get_downloaded_objects()) == 2
    assert len(get_existing_objects()) == 2
    assert local_engine_empty.get_all_tables(OUTPUT.to_schema()) == ['test']

    # Import the 'fruits' table from the origin.
    with switch_engine(REMOTE_ENGINE):
        remote_head = get_current_head(PG_MNT)
    import_table_from_remote('remote_engine', PG_MNT, ['fruits'], remote_head, OUTPUT, target_tables=[])
    new_head = get_current_head(OUTPUT)

    # Check that the table now exists in the output, is committed and there's no trace of the cloned repo.
    # Also clean up the unused objects to make sure that the newly cloned table is still recorded.
    assert sorted(local_engine_empty.get_all_tables(OUTPUT.to_schema())) == ['fruits', 'test']
    cleanup_objects()
    assert len(get_current_repositories()) == 1
    checkout(OUTPUT, head)
    assert local_engine_empty.table_exists(OUTPUT.to_schema(), 'test')
    assert not local_engine_empty.table_exists(OUTPUT.to_schema(), 'fruits')

    checkout(OUTPUT, new_head)
    assert local_engine_empty.table_exists(OUTPUT.to_schema(), 'test')
    assert local_engine_empty.table_exists(OUTPUT.to_schema(), 'fruits')

    assert local_engine_empty.run_sql("SELECT * FROM output.fruits") \
           == remote_engine.run_sql("SELECT * FROM \"test/pg_mount\".fruits")


def test_import_and_update(local_engine_empty, remote_engine):
    init(OUTPUT)
    head = get_current_head(OUTPUT)
    with switch_engine(REMOTE_ENGINE):
        remote_head = get_current_head(PG_MNT)
    # Import the 'fruits' table from the origin.
    import_table_from_remote('remote_engine', PG_MNT, ['fruits'], remote_head, OUTPUT, target_tables=[])
    new_head = get_current_head(OUTPUT)

    local_engine_empty.run_sql("INSERT INTO output.fruits VALUES (3, 'mayonnaise')")
    new_head_2 = commit(OUTPUT)

    checkout(OUTPUT, head)
    assert not get_engine().table_exists(OUTPUT.to_schema(), 'fruits')

    checkout(OUTPUT, new_head)
    assert local_engine_empty.run_sql("SELECT * FROM output.fruits") == [(1, 'apple'), (2, 'orange')]

    checkout(OUTPUT, new_head_2)
    assert local_engine_empty.run_sql("SELECT * FROM output.fruits") == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
