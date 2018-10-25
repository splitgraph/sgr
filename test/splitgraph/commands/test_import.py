from splitgraph.commands import checkout, commit, init, import_tables
from splitgraph.commands.diff import dump_pending_changes
from splitgraph.commands.importing import import_table_from_remote
from splitgraph.commands.misc import cleanup_objects
from splitgraph.meta_handler.images import get_image
from splitgraph.meta_handler.misc import get_current_repositories
from splitgraph.meta_handler.objects import get_existing_objects, get_downloaded_objects
from splitgraph.meta_handler.tags import get_current_head
from splitgraph.pg_utils import pg_table_exists, get_all_tables
from test.splitgraph.conftest import PG_MNT, REMOTE_CONN_STRING, OUTPUT


def test_import_basic(sg_pg_conn):
    # Create a new schema and import 'fruits' from the mounted PG table.
    init(sg_pg_conn, OUTPUT)
    head = get_current_head(sg_pg_conn, OUTPUT)
    import_tables(sg_pg_conn, repository=PG_MNT, tables=['fruits'], target_repository=OUTPUT,
                  target_tables=['imported_fruits'])

    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.imported_fruits""")
        output = cur.fetchall()
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        input = cur.fetchall()
        assert input == output
    new_head = get_current_head(sg_pg_conn, OUTPUT)

    assert new_head != head
    assert get_image(sg_pg_conn, OUTPUT, new_head).parent_id == head


def test_import_preserves_existing_tables(sg_pg_conn):
    # Create a new schema and import 'fruits' from the mounted PG table.
    init(sg_pg_conn, OUTPUT)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""CREATE TABLE output.test (id integer, name varchar)""")
        cur.execute("""INSERT INTO output.test VALUES (1, 'test')""")
    commit(sg_pg_conn, OUTPUT)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO output.test VALUES (2, 'test2')""")
    head = commit(sg_pg_conn, OUTPUT)

    import_tables(sg_pg_conn, repository=PG_MNT, tables=['fruits'], target_repository=OUTPUT,
                  target_tables=['imported_fruits'])

    new_head = get_current_head(sg_pg_conn, OUTPUT)

    checkout(sg_pg_conn, OUTPUT, head)
    assert pg_table_exists(sg_pg_conn, OUTPUT.to_schema(), 'test')
    assert not pg_table_exists(sg_pg_conn, OUTPUT.to_schema(), 'imported_fruits')

    checkout(sg_pg_conn, OUTPUT, new_head)
    assert pg_table_exists(sg_pg_conn, OUTPUT.to_schema(), 'test')
    assert pg_table_exists(sg_pg_conn, OUTPUT.to_schema(), 'imported_fruits')


def test_import_preserves_pending_changes(sg_pg_conn):
    init(sg_pg_conn, OUTPUT)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""CREATE TABLE output.test (id integer, name varchar)""")
        cur.execute("""INSERT INTO output.test VALUES (1, 'test')""")
    head = commit(sg_pg_conn, OUTPUT)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO output.test VALUES (2, 'test2')""")
    changes = dump_pending_changes(sg_pg_conn, OUTPUT.to_schema(), 'test')

    import_tables(sg_pg_conn, repository=PG_MNT, tables=['fruits'], target_repository=OUTPUT,
                  target_tables=['imported_fruits'])
    new_head = get_current_head(sg_pg_conn, OUTPUT)

    assert get_image(sg_pg_conn, OUTPUT, new_head).parent_id == head
    assert changes == dump_pending_changes(sg_pg_conn, OUTPUT.to_schema(), 'test')


def test_import_multiple_tables(sg_pg_conn):
    init(sg_pg_conn, OUTPUT)
    head = get_current_head(sg_pg_conn, OUTPUT)
    import_tables(sg_pg_conn, repository=PG_MNT, tables=[], target_repository=OUTPUT, target_tables=[])

    for table_name in ['fruits', 'vegetables']:
        with sg_pg_conn.cursor() as cur:
            cur.execute("""SELECT * FROM output.%s""" % table_name)
            output = cur.fetchall()
            cur.execute("""SELECT * FROM "test/pg_mount".%s""" % table_name)
            input = cur.fetchall()
            assert input == output

    new_head = get_current_head(sg_pg_conn, OUTPUT)
    assert new_head != head
    assert get_image(sg_pg_conn, OUTPUT, new_head).parent_id == head


def test_import_from_remote(empty_pg_conn, remote_driver_conn):
    # Start with a clean repo -- add a table to output to see if it's preserved.
    init(empty_pg_conn, OUTPUT)
    with empty_pg_conn.cursor() as cur:
        cur.execute("""CREATE TABLE output.test (id integer, name varchar)""")
        cur.execute("""INSERT INTO output.test VALUES (1, 'test')""")
    commit(empty_pg_conn, OUTPUT)
    with empty_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO output.test VALUES (2, 'test2')""")
    head = commit(empty_pg_conn, OUTPUT)

    assert len(get_downloaded_objects(empty_pg_conn)) == 2
    assert len(get_existing_objects(empty_pg_conn)) == 2
    assert get_all_tables(empty_pg_conn, OUTPUT.to_schema()) == ['test']

    # Import the 'fruits' table from the origin.
    import_table_from_remote(empty_pg_conn, REMOTE_CONN_STRING, PG_MNT, ['fruits'],
                             get_current_head(remote_driver_conn, PG_MNT), OUTPUT, target_tables=[])
    new_head = get_current_head(empty_pg_conn, OUTPUT)

    # Check that the table now exists in the output, is committed and there's no trace of the cloned repo.
    # Also clean up the unused objects to make sure that the newly cloned table is still recorded.
    assert sorted(get_all_tables(empty_pg_conn, OUTPUT.to_schema())) == ['fruits', 'test']
    cleanup_objects(empty_pg_conn)
    assert len(get_current_repositories(empty_pg_conn)) == 1
    checkout(empty_pg_conn, OUTPUT, head)
    assert pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), 'test')
    assert not pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), 'fruits')

    checkout(empty_pg_conn, OUTPUT, new_head)
    assert pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), 'test')
    assert pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), 'fruits')

    with empty_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.fruits""")
        output = cur.fetchall()
    with remote_driver_conn.cursor() as cur:
        cur.execute("""SELECT * FROM "test/pg_mount".fruits""")
        input = cur.fetchall()
    assert input == output


def test_import_and_update(empty_pg_conn, remote_driver_conn):
    init(empty_pg_conn, OUTPUT)
    head = get_current_head(empty_pg_conn, OUTPUT)
    # Import the 'fruits' table from the origin.
    import_table_from_remote(empty_pg_conn, REMOTE_CONN_STRING, PG_MNT, ['fruits'],
                             get_current_head(remote_driver_conn, PG_MNT), OUTPUT, target_tables=[])
    new_head = get_current_head(empty_pg_conn, OUTPUT)

    with empty_pg_conn.cursor() as cur:
        cur.execute("INSERT INTO output.fruits VALUES (3, 'mayonnaise')")
    new_head_2 = commit(empty_pg_conn, OUTPUT)

    checkout(empty_pg_conn, OUTPUT, head)
    assert not pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), 'fruits')

    checkout(empty_pg_conn, OUTPUT, new_head)
    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.fruits")
        assert cur.fetchall() == [(1, 'apple'), (2, 'orange')]

    checkout(empty_pg_conn, OUTPUT, new_head_2)
    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.fruits")
        assert cur.fetchall() == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
