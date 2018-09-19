from splitgraph.commands import checkout, commit, init, import_tables
from splitgraph.commands.diff import dump_pending_changes
from splitgraph.commands.importing import import_table_from_unmounted
from splitgraph.commands.misc import cleanup_objects
from splitgraph.meta_handler.images import get_image_parent
from splitgraph.meta_handler.misc import get_current_mountpoints_hashes
from splitgraph.meta_handler.objects import get_existing_objects, get_downloaded_objects
from splitgraph.meta_handler.tables import get_all_tables
from splitgraph.meta_handler.tags import get_current_head
from splitgraph.pg_utils import pg_table_exists
from test.splitgraph.conftest import PG_MNT, SNAPPER_CONN_STRING


def test_import_basic(sg_pg_conn):
    # Create a new schema and import 'fruits' from the mounted PG table.
    init(sg_pg_conn, 'output')
    head = get_current_head(sg_pg_conn, 'output')
    import_tables(sg_pg_conn, mountpoint=PG_MNT, tables=['fruits'], target_mountpoint='output',
                  target_tables=['imported_fruits'])

    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.imported_fruits""")
        output = cur.fetchall()
        cur.execute("""SELECT * FROM test_pg_mount.fruits""")
        input = cur.fetchall()
        assert input == output
    new_head = get_current_head(sg_pg_conn, 'output')

    assert new_head != head
    assert get_image_parent(sg_pg_conn, 'output', new_head) == head


def test_import_preserves_existing_tables(sg_pg_conn):
    # Create a new schema and import 'fruits' from the mounted PG table.
    init(sg_pg_conn, 'output')
    with sg_pg_conn.cursor() as cur:
        cur.execute("""CREATE TABLE output.test (id integer, name varchar)""")
        cur.execute("""INSERT INTO output.test VALUES (1, 'test')""")
    commit(sg_pg_conn, 'output')
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO output.test VALUES (2, 'test2')""")
    head = commit(sg_pg_conn, 'output')

    import_tables(sg_pg_conn, mountpoint=PG_MNT, tables=['fruits'], target_mountpoint='output',
                  target_tables=['imported_fruits'])

    new_head = get_current_head(sg_pg_conn, 'output')

    checkout(sg_pg_conn, 'output', head)
    assert pg_table_exists(sg_pg_conn, 'output', 'test')
    assert not pg_table_exists(sg_pg_conn, 'output', 'imported_fruits')

    checkout(sg_pg_conn, 'output', new_head)
    assert pg_table_exists(sg_pg_conn, 'output', 'test')
    assert pg_table_exists(sg_pg_conn, 'output', 'imported_fruits')


def test_import_preserves_pending_changes(sg_pg_conn):
    init(sg_pg_conn, 'output')
    with sg_pg_conn.cursor() as cur:
        cur.execute("""CREATE TABLE output.test (id integer, name varchar)""")
        cur.execute("""INSERT INTO output.test VALUES (1, 'test')""")
    head = commit(sg_pg_conn, 'output')
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO output.test VALUES (2, 'test2')""")
    changes = dump_pending_changes(sg_pg_conn, 'output', 'test')

    import_tables(sg_pg_conn, mountpoint=PG_MNT, tables=['fruits'], target_mountpoint='output',
                  target_tables=['imported_fruits'])
    new_head = get_current_head(sg_pg_conn, 'output')

    assert get_image_parent(sg_pg_conn, 'output', new_head) == head
    assert changes == dump_pending_changes(sg_pg_conn, 'output', 'test')


def test_import_multiple_tables(sg_pg_conn):
    init(sg_pg_conn, 'output')
    head = get_current_head(sg_pg_conn, 'output')
    import_tables(sg_pg_conn, mountpoint=PG_MNT, tables=[], target_mountpoint='output', target_tables=[])

    for table_name in ['fruits', 'vegetables']:
        with sg_pg_conn.cursor() as cur:
            cur.execute("""SELECT * FROM output.%s""" % table_name)
            output = cur.fetchall()
            cur.execute("""SELECT * FROM test_pg_mount.%s""" % table_name)
            input = cur.fetchall()
            assert input == output

    new_head = get_current_head(sg_pg_conn, 'output')
    assert new_head != head
    assert get_image_parent(sg_pg_conn, 'output', new_head) == head


def test_import_from_remote(empty_pg_conn, snapper_conn):
    # Start with a clean repo -- add a table to output to see if it's preserved.
    init(empty_pg_conn, 'output')
    with empty_pg_conn.cursor() as cur:
        cur.execute("""CREATE TABLE output.test (id integer, name varchar)""")
        cur.execute("""INSERT INTO output.test VALUES (1, 'test')""")
    commit(empty_pg_conn, 'output')
    with empty_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO output.test VALUES (2, 'test2')""")
    head = commit(empty_pg_conn, 'output')

    assert len(get_downloaded_objects(empty_pg_conn)) == 2
    assert len(get_existing_objects(empty_pg_conn)) == 2
    assert get_all_tables(empty_pg_conn, 'output') == ['test']

    # Import the 'fruits' table from the origin.
    import_table_from_unmounted(empty_pg_conn, SNAPPER_CONN_STRING, PG_MNT, ['fruits'], get_current_head(snapper_conn, PG_MNT),
                                'output', target_tables=[])
    new_head = get_current_head(empty_pg_conn, 'output')

    # Check that the table now exists in the output, is committed and there's no trace of the cloned repo.
    # Also clean up the unused objects to make sure that the newly cloned table is still recorded.
    assert sorted(get_all_tables(empty_pg_conn, 'output')) == ['fruits', 'test']
    cleanup_objects(empty_pg_conn)
    assert len(get_current_mountpoints_hashes(empty_pg_conn)) == 1
    checkout(empty_pg_conn, 'output', head)
    assert pg_table_exists(empty_pg_conn, 'output', 'test')
    assert not pg_table_exists(empty_pg_conn, 'output', 'fruits')

    checkout(empty_pg_conn, 'output', new_head)
    assert pg_table_exists(empty_pg_conn, 'output', 'test')
    assert pg_table_exists(empty_pg_conn, 'output', 'fruits')

    with empty_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.fruits""")
        output = cur.fetchall()
    with snapper_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount.fruits""")
        input = cur.fetchall()
    assert input == output


def test_import_and_update(empty_pg_conn, snapper_conn):
    init(empty_pg_conn, 'output')
    head = get_current_head(empty_pg_conn, 'output')
    # Import the 'fruits' table from the origin.
    import_table_from_unmounted(empty_pg_conn, SNAPPER_CONN_STRING, PG_MNT, ['fruits'], get_current_head(snapper_conn, PG_MNT),
                                'output', target_tables=[])
    new_head = get_current_head(empty_pg_conn, 'output')

    with empty_pg_conn.cursor() as cur:
        cur.execute("INSERT INTO output.fruits VALUES (3, 'mayonnaise')")
    new_head_2 = commit(empty_pg_conn, 'output')

    checkout(empty_pg_conn, 'output', head)
    assert not pg_table_exists(empty_pg_conn, 'output', 'fruits')

    checkout(empty_pg_conn, 'output', new_head)
    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.fruits")
        assert cur.fetchall() == [(1, 'apple'), (2, 'orange')]

    checkout(empty_pg_conn, 'output', new_head_2)
    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.fruits")
        assert cur.fetchall() == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
