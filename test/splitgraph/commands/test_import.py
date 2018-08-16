from splitgraph.commands import checkout, commit, pull, init, import_table
from splitgraph.meta_handler import get_current_head, get_all_snap_parents, get_snap_parent
from splitgraph.pg_replication import dump_pending_changes
from splitgraph.pg_utils import pg_table_exists
from test.splitgraph.conftest import PG_MNT


def test_import_basic(sg_pg_conn):
    # Create a new schema and import 'fruits' from the mounted PG table.
    init(sg_pg_conn, 'output')
    head = get_current_head(sg_pg_conn, 'output')
    import_table(sg_pg_conn, mountpoint=PG_MNT, table='fruits', target_mountpoint='output', target_table='imported_fruits')

    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.imported_fruits""")
        output = cur.fetchall()
        cur.execute("""SELECT * FROM test_pg_mount.fruits""")
        input = cur.fetchall()
        assert input == output
    new_head = get_current_head(sg_pg_conn, 'output')

    assert new_head != head
    assert get_snap_parent(sg_pg_conn, 'output', new_head) == head


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

    import_table(sg_pg_conn, mountpoint=PG_MNT, table='fruits', target_mountpoint='output',
                 target_table='imported_fruits')

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

    import_table(sg_pg_conn, mountpoint=PG_MNT, table='fruits', target_mountpoint='output',
                 target_table='imported_fruits')
    new_head = get_current_head(sg_pg_conn, 'output')

    assert get_snap_parent(sg_pg_conn, 'output', new_head) == head
    assert changes == dump_pending_changes(sg_pg_conn, 'output', 'test')
