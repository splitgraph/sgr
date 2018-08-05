import os

from splitgraph.commands import get_log, checkout, pg_table_exists, commit
from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler import get_current_head, get_snap_parent
from splitgraph.sgfile import parse_commands, execute_commands
from test.splitgraph.test_commands import PG_MNT, MG_MNT, sg_pg_mg_conn

SGFILE_ROOT = os.path.join(os.path.dirname(__file__), '../resources/')

def test_sgfile_parsing():
    with open(SGFILE_ROOT + 'sample.sgfile', 'r') as f:
        lines = f.readlines()
    commands = parse_commands(lines)
    assert commands == [('SOURCE', ['test_mount']),
                        ('SOURCE', ['test_mount_mongo']),
                        ('OUTPUT', ['output', '00000000']),
                        ('SQL',
                         [
                             'create table output.fruits as select * from test_mount.fruits where length(test_mount.fruits.name) > 5']),
                        ('SQL', ["insert into output.fruits values (10, 'pikachu')"]),
                        ('SQL',
                         [
                             'create table output.spirit_fruits as select test_mount.fruits.fruit_id,     test_mount_mongo.stuff.name, test_mount.fruits.name as spirit_fruit from test_mount.fruits     join test_mount_mongo.stuff on test_mount.fruits.fruit_id = test_mount_mongo.stuff.duration'])]


BASE_COMMANDS = [('SOURCE', (PG_MNT,)),
                 ('SOURCE', (MG_MNT,)),
                 ('OUTPUT', ('output', '000')),
                 ('SQL', (
                 'CREATE TABLE output.fruits AS SELECT * FROM test_pg_mount.fruits WHERE LENGTH(test_pg_mount.fruits.name) '
                 '> 5',)),
                 ('SQL', ("INSERT INTO output.fruits VALUES (10, 'pikachu')",))]


def test_basic_sgfile(sg_pg_mg_conn):
    # Execute the imports + 1st SQL statement
    execute_commands(sg_pg_mg_conn, BASE_COMMANDS[:4])
    head = get_current_head(sg_pg_mg_conn, 'output')

    # Single SQL statement -> single commit.
    assert get_snap_parent(sg_pg_mg_conn, 'output', head) == '0' * 64

    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.fruits""")
        assert cur.fetchall() == [(2, 'orange')]


def test_advanced_sgfile(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, BASE_COMMANDS[:5])
    head = get_current_head(sg_pg_mg_conn, 'output')

    # Check expected data at each of the 3 commits created in output
    # Head: both orange and pikachu in the list
    log = get_log(sg_pg_mg_conn, 'output', head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.fruits""")
        assert cur.fetchall() == [(2, 'orange'), (10, 'pikachu')]
    # Before that: just the orange (same as the previous test)
    checkout(sg_pg_mg_conn, 'output', log[1])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.fruits""")
        assert cur.fetchall() == [(2, 'orange')]
    # Before that: empty mountpoint (hash 000...)
    checkout(sg_pg_mg_conn, 'output', log[2])
    assert not pg_table_exists(sg_pg_mg_conn, 'output', 'fruits')


def test_sgfile_cached(sg_pg_mg_conn):
    # Check that no new commits/snaps are created if we rerun the same sgfile
    execute_commands(sg_pg_mg_conn, BASE_COMMANDS[:5])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT snap_id FROM %s.snap_tree WHERE mountpoint = 'output'""" % SPLITGRAPH_META_SCHEMA)
        commits = [c[0] for c in cur.fetchall()]
    assert len(commits) == 3

    execute_commands(sg_pg_mg_conn, BASE_COMMANDS[:5])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT snap_id FROM %s.snap_tree WHERE mountpoint = 'output'""" % SPLITGRAPH_META_SCHEMA)
        new_commits = [c[0] for c in cur.fetchall()]
    assert new_commits == commits


def test_sgfile_rebase(sg_pg_mg_conn):
    # First, run the sgfile on the old dataset.
    execute_commands(sg_pg_mg_conn, BASE_COMMANDS[:5])
    old_output_head = get_current_head(sg_pg_mg_conn, 'output')

    # Then, alter the dataset and rerun the sgfile.
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (4, 'mayonnaise')""")
    new_input_head = commit(sg_pg_mg_conn, PG_MNT)
    execute_commands(sg_pg_mg_conn, BASE_COMMANDS[:5])
    new_output_head = get_current_head(sg_pg_mg_conn, 'output')

    # In total, there should be 5 commits for output (2 branches, 3 commits each, 1 commit in common).
    old_log = get_log(sg_pg_mg_conn, 'output', old_output_head)
    new_log = get_log(sg_pg_mg_conn, 'output', new_output_head)
    assert len(old_log) == 3
    assert len(new_log) == 3
    assert len(set(old_log).union(set(new_log))) == 5
    assert old_log[-1] == new_log[-1] == '0' * 64  # Both based on the empty mountpoint.

    # Old dataset: same as previous (orange + pikachu)
    checkout(sg_pg_mg_conn, 'output', old_log[0])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.fruits""")
        assert cur.fetchall() == [(2, 'orange'), (10, 'pikachu')]

    # New dataset: has an extra (4, mayonnaise) from the change.
    checkout(sg_pg_mg_conn, 'output', new_log[0])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.fruits""")
        assert cur.fetchall() == [(2, 'orange'), (4, 'mayonnaise'), (10, 'pikachu')]
