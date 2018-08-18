import os

from splitgraph.commands import checkout, commit
from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler import get_current_head, get_snap_parent, set_tag
from splitgraph.pg_utils import pg_table_exists
from splitgraph.sgfile import execute_commands
from test.splitgraph.conftest import SNAPPER_HOST

SGFILE_ROOT = os.path.join(os.path.dirname(__file__), '../resources/')

def _load_sgfile(name):
    with open(SGFILE_ROOT + name, 'r') as f:
        return f.read()
#
# PK_SCHEMA_CHANGE_COMMANDS = BASE_COMMANDS[:3] + [
#     ('SQL', ("""CREATE TABLE output.spirit_fruits AS SELECT test_pg_mount.fruits.fruit_id,
#                                                    test_mg_mount.stuff.name,
#                                                    test_pg_mount.fruits.name AS spirit_fruit
#                 FROM test_pg_mount.fruits JOIN test_mg_mount.stuff
#                 ON test_pg_mount.fruits.fruit_id = test_mg_mount.stuff.duration""",)),
#     # Add a new column, set it to be the old id + 10, make it PK and then delete the old ID.
#     # Currently this produces a snap for every action (since it's a schema change).
#     ('SQL', ("""ALTER TABLE output.spirit_fruits ADD COLUMN new_id integer""",)),
#     ('SQL', ("""UPDATE output.spirit_fruits SET new_id = fruit_id + 10""",)),
#     ('SQL', ("""ALTER TABLE output.spirit_fruits ADD PRIMARY KEY (new_id)""",)),
#     ('SQL', ("""ALTER TABLE output.spirit_fruits DROP COLUMN fruit_id""",)),
# ]


def test_basic_sgfile(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local.sgfile'))
    head = get_current_head(sg_pg_mg_conn, 'output')
    old_head = get_snap_parent(sg_pg_mg_conn, 'output', head)

    checkout(sg_pg_mg_conn, 'output', old_head)
    assert not pg_table_exists(sg_pg_mg_conn, 'output', 'my_fruits')
    assert not pg_table_exists(sg_pg_mg_conn, 'output', 'fruits')

    checkout(sg_pg_mg_conn, 'output', head)
    assert pg_table_exists(sg_pg_mg_conn, 'output', 'my_fruits')
    assert not pg_table_exists(sg_pg_mg_conn, 'output', 'fruits')


def test_advanced_sgfile(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local_multiple_with_queries.sgfile'))
    head = get_current_head(sg_pg_mg_conn, 'output')

    assert pg_table_exists(sg_pg_mg_conn, 'output', 'my_fruits')
    assert pg_table_exists(sg_pg_mg_conn, 'output', 'vegetables')
    assert not pg_table_exists(sg_pg_mg_conn, 'output', 'fruits')
    assert pg_table_exists(sg_pg_mg_conn, 'output', 'join_table')

    old_head = get_snap_parent(sg_pg_mg_conn, 'output', head)
    checkout(sg_pg_mg_conn, 'output', old_head)
    assert not pg_table_exists(sg_pg_mg_conn, 'output', 'join_table')
    checkout(sg_pg_mg_conn, 'output', head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(2, 'orange', 'carrot')]
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT COUNT(1) FROM output.my_fruits""")
        assert cur.fetchall() == [(1,)]


def test_sgfile_cached(sg_pg_mg_conn):
    # Check that no new commits/snaps are created if we rerun the same sgfile
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local_multiple_with_queries.sgfile'))
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT snap_id FROM %s.snap_tree WHERE mountpoint = 'output'""" % SPLITGRAPH_META_SCHEMA)
        commits = [c[0] for c in cur.fetchall()]
    assert len(commits) == 4

    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local_multiple_with_queries.sgfile'))
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT snap_id FROM %s.snap_tree WHERE mountpoint = 'output'""" % SPLITGRAPH_META_SCHEMA)
        new_commits = [c[0] for c in cur.fetchall()]
    assert new_commits == commits


def test_sgfile_remote(sg_pg_mg_conn, snapper_conn):
    # Give test_pg_mount on the remote tag v1
    set_tag(snapper_conn, 'test_pg_mount', get_current_head(snapper_conn, 'test_pg_mount'), 'v1')
    with snapper_conn.cursor() as cur:
        cur.execute("DELETE FROM test_pg_mount.fruits WHERE fruit_id = 1")
    snapper_conn.commit()
    new_head = commit(snapper_conn, 'test_pg_mount')
    set_tag(snapper_conn, 'test_pg_mount', new_head, 'v2')

    # We use the v1 tag when importing from the remote, so fruit_id = 1 still exists there.
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_remote_multiple.sgfile').replace("$SNAPPER", SNAPPER_HOST))
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(1, '', ''), (2, 'orange', 'carrot')]


#
#
# def test_sgfile_rebase(sg_pg_mg_conn):
#     # First, run the sgfile on the old dataset.
#     execute_commands(sg_pg_mg_conn, BASE_COMMANDS[:5])
#     old_output_head = get_current_head(sg_pg_mg_conn, 'output')
#
#     # Then, alter the dataset and rerun the sgfile.
#     with sg_pg_mg_conn.cursor() as cur:
#         cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (4, 'mayonnaise')""")
#     new_input_head = commit(sg_pg_mg_conn, PG_MNT)
#     execute_commands(sg_pg_mg_conn, BASE_COMMANDS[:5])
#     new_output_head = get_current_head(sg_pg_mg_conn, 'output')
#
#     # In total, there should be 5 commits for output (2 branches, 3 commits each, 1 commit in common).
#     old_log = get_log(sg_pg_mg_conn, 'output', old_output_head)
#     new_log = get_log(sg_pg_mg_conn, 'output', new_output_head)
#     assert len(old_log) == 3
#     assert len(new_log) == 3
#     assert len(set(old_log).union(set(new_log))) == 5
#     assert old_log[-1] == new_log[-1] == '0' * 64  # Both based on the empty mountpoint.
#
#     # Old dataset: same as previous (orange + pikachu)
#     checkout(sg_pg_mg_conn, 'output', old_log[0])
#     with sg_pg_mg_conn.cursor() as cur:
#         cur.execute("""SELECT * FROM output.fruits""")
#         assert cur.fetchall() == [(2, 'orange'), (10, 'pikachu')]
#
#     # New dataset: has an extra (4, mayonnaise) from the change.
#     checkout(sg_pg_mg_conn, 'output', new_log[0])
#     with sg_pg_mg_conn.cursor() as cur:
#         cur.execute("""SELECT * FROM output.fruits""")
#         assert cur.fetchall() == [(2, 'orange'), (4, 'mayonnaise'), (10, 'pikachu')]
#
#
# def test_sgfile_schema_changes(sg_pg_mg_conn):
#     execute_commands(sg_pg_mg_conn, PK_SCHEMA_CHANGE_COMMANDS)
#     old_output_head = get_current_head(sg_pg_mg_conn, 'output')
#
#     # Then, alter the dataset and rerun the sgfile.
#     with sg_pg_mg_conn.cursor() as cur:
#         cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (12, 'mayonnaise')""")
#     new_input_head = commit(sg_pg_mg_conn, PG_MNT)
#     execute_commands(sg_pg_mg_conn, PK_SCHEMA_CHANGE_COMMANDS)
#     new_output_head = get_current_head(sg_pg_mg_conn, 'output')
#
#     checkout(sg_pg_mg_conn, 'output', old_output_head)
#     with sg_pg_mg_conn.cursor() as cur:
#         cur.execute("""SELECT * FROM output.spirit_fruits""")
#         assert cur.fetchall() == [('James', 'orange', 12)]
#
#     checkout(sg_pg_mg_conn, 'output', new_output_head)
#     with sg_pg_mg_conn.cursor() as cur:
#         cur.execute("""SELECT * FROM output.spirit_fruits""")
#         # Mayonnaise joined with Alex, ID 12 + 10 = 22.
#         assert cur.fetchall() == [('James', 'orange', 12), ('Alex', 'mayonnaise', 22)]
