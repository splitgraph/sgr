# Integration tests that assume the whole infrastructure is running
import pytest

from splitgraph.commandline import _conn
from splitgraph.commands import mount, unmount, diff, commit, get_log, checkout, _table_exists, pull, push
from splitgraph.constants import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PWD
from splitgraph.meta_handler import get_current_head, get_table, get_all_snap_parents

PG_MNT = 'test_pg_mount'
MG_MNT = 'test_mg_mount'


def _mount_postgres(conn, mountpoint):
    mount(conn, server='pgorigin', port=5432, username='originro', password='originpass', mountpoint=mountpoint,
          mount_handler='postgres_fdw', extra_options={"dbname": "origindb", "remote_schema": "public"})


def _mount_mongo(conn, mountpoint):
    mount(conn, server='mongoorigin', port=27017, username='originro', password='originpass', mountpoint=mountpoint,
          mount_handler='mongo_fdw', extra_options={"stuff": {
            "db": "origindb",
            "coll": "stuff",
            "schema": {
                "name": "text",
                "duration": "numeric",
                "happy": "boolean"
            }}})


@pytest.fixture
def sg_pg_conn():
    # SG connection with a mounted Postgres db
    conn = _conn()
    unmount(conn, PG_MNT)
    _mount_postgres(conn, PG_MNT)
    yield conn
    unmount(conn, PG_MNT)


@pytest.fixture
def sg_pg_mg_conn():
    # SG connection with a mounted Mongo + Postgres db
    # Also, remove the 'output' mountpoint that we'll be using in the sgfile tests
    conn = _conn()
    unmount(conn, MG_MNT)
    unmount(conn, PG_MNT)
    unmount(conn, PG_MNT + '_pull')
    unmount(conn, 'output')
    _mount_postgres(conn, PG_MNT)
    _mount_mongo(conn, MG_MNT)
    yield conn
    unmount(conn, MG_MNT)
    unmount(conn, PG_MNT)
    unmount(conn, PG_MNT + '_pull')
    unmount(conn, 'output')


def test_mount_unmount():
    # It's up to us to commit the connection -- if we don't commit it, the actual test db doesn't get changed.
    conn = _conn()
    unmount(conn, PG_MNT)
    _mount_postgres(conn, PG_MNT)
    with conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount.fruits""")
        assert (1, 'apple') in list(cur.fetchall())
    unmount(conn, PG_MNT)
    with conn.cursor() as cur:
        cur.execute("""SELECT * FROM information_schema.schemata where schema_name = '%s'""" % PG_MNT)
        assert cur.fetchone() is None


def test_cross_joins(sg_pg_mg_conn):
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT test_pg_mount.fruits.fruit_id,
                             test_mg_mount.stuff.name,
                             test_pg_mount.fruits.name as spirit_fruit
                      FROM test_pg_mount.fruits 
                             JOIN test_mg_mount.stuff 
                             ON test_pg_mount.fruits.fruit_id = test_mg_mount.stuff.duration""")
        assert cur.fetchall() == [(2, 'James', 'orange')]


def test_diff_head(sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")
        cur.execute("""DELETE FROM test_pg_mount.fruits WHERE name = 'apple'""")

    head = get_current_head(sg_pg_conn, PG_MNT)
    added, removed = diff(sg_pg_conn, PG_MNT, 'fruits', snap_1=head, snap_2=None)
    assert added == [(3, 'mayonnaise')]
    assert removed == [(1, 'apple')]


COMMIT_FORMATS = ['SNAP', 'DIFF', 'WAL']


@pytest.mark.parametrize("commit_format", COMMIT_FORMATS)
def test_commit_diff(commit_format, sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")
        cur.execute("""DELETE FROM test_pg_mount.fruits WHERE name = 'apple'""")

    head = get_current_head(sg_pg_conn, PG_MNT)
    new_head = commit(sg_pg_conn, PG_MNT, storage_format=commit_format)

    # After commit, we should be switched to the new commit hash and there should be no differences.
    assert get_current_head(sg_pg_conn, PG_MNT) == new_head
    assert diff(sg_pg_conn, PG_MNT, 'fruits', snap_1=new_head, snap_2=None) == ([], [])

    # The diff between the old and the new snaps should be the same as in the previous test
    added, removed = diff(sg_pg_conn, PG_MNT, 'fruits', snap_1=head, snap_2=new_head)
    assert added == [(3, 'mayonnaise')]
    assert removed == [(1, 'apple')]


@pytest.mark.parametrize("commit_format", COMMIT_FORMATS)
def test_log_checkout(commit_format, sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")

    head = get_current_head(sg_pg_conn, PG_MNT)
    head_1 = commit(sg_pg_conn, PG_MNT, storage_format=commit_format)

    with sg_pg_conn.cursor() as cur:
        cur.execute("""DELETE FROM test_pg_mount.fruits WHERE name = 'apple'""")

    head_2 = commit(sg_pg_conn, PG_MNT, storage_format=commit_format)

    assert get_current_head(sg_pg_conn, PG_MNT) == head_2
    assert get_log(sg_pg_conn, PG_MNT, head_2) == [head_2, head_1, head]

    checkout(sg_pg_conn, PG_MNT, head)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange')]

    checkout(sg_pg_conn, PG_MNT, head_1)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

    checkout(sg_pg_conn, PG_MNT, head_2)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount.fruits""")
        assert list(cur.fetchall()) == [(2, 'orange'), (3, 'mayonnaise')]


@pytest.mark.parametrize("commit_format", COMMIT_FORMATS)
def test_table_changes(commit_format, sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""CREATE TABLE test_pg_mount.fruits_copy AS SELECT * FROM test_pg_mount.fruits""")

    head = get_current_head(sg_pg_conn, PG_MNT)
    # Check that table addition has been detected
    assert diff(sg_pg_conn, PG_MNT, 'fruits_copy', snap_1=head, snap_2=None) is True

    head_1 = commit(sg_pg_conn, PG_MNT, storage_format=commit_format)
    # Checkout the old head and make sure the table doesn't exist in it
    checkout(sg_pg_conn, PG_MNT, head)
    assert not _table_exists(sg_pg_conn, PG_MNT, 'fruits_copy')

    # Make sure the table is reflected in the diff even if we're on a different commit
    assert diff(sg_pg_conn, PG_MNT, 'fruits_copy', snap_1=head, snap_2=head_1) is True

    # Go back and now delete a table
    checkout(sg_pg_conn, PG_MNT, head_1)
    assert _table_exists(sg_pg_conn, PG_MNT, 'fruits_copy')
    with sg_pg_conn.cursor() as cur:
        cur.execute("""DROP TABLE test_pg_mount.fruits""")
    assert not _table_exists(sg_pg_conn, PG_MNT, 'fruits')

    # Make sure the diff shows it's been removed and commit it
    assert diff(sg_pg_conn, PG_MNT, 'fruits', snap_1=head_1, snap_2=None) is False
    head_2 = commit(sg_pg_conn, PG_MNT, storage_format=commit_format)

    # Go through the 3 commits and ensure the table existence is maintained
    checkout(sg_pg_conn, PG_MNT, head)
    assert _table_exists(sg_pg_conn, PG_MNT, 'fruits')
    checkout(sg_pg_conn, PG_MNT, head_1)
    assert _table_exists(sg_pg_conn, PG_MNT, 'fruits')
    checkout(sg_pg_conn, PG_MNT, head_2)
    assert not _table_exists(sg_pg_conn, PG_MNT, 'fruits')


def test_empty_diff_reuses_object(sg_pg_conn):
    head = get_current_head(sg_pg_conn, PG_MNT)
    head_1 = commit(sg_pg_conn, PG_MNT, storage_format='DIFF')

    obj_1, pack_1 = get_table(sg_pg_conn, PG_MNT, 'fruits', head)
    obj_2, pack_2 = get_table(sg_pg_conn, PG_MNT, 'fruits', head_1)

    assert obj_1 == obj_2
    assert pack_1 == 'SNAP'
    assert pack_2 == 'SNAP'  # Even though we asked to store as pack, since the diff is empty, we just point to the
    # previous table.


def test_pull(sg_pg_conn):
    # This is going to get awkward: we're going to pull a schema from ourselves since there's only one
    # pgcache for now.
    pull(sg_pg_conn, '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB),
         PG_MNT, PG_MNT + '_pull')
    checkout(sg_pg_conn, PG_MNT + '_pull', get_current_head(sg_pg_conn, PG_MNT))

    # Do something to fruits
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")

    head_1 = commit(sg_pg_conn, PG_MNT, storage_format='DIFF')

    # Check that the fruits table changed on the original mount
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

    # ...and check it's unchanged on the pulled one.
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount_pull.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange')]
    assert head_1 not in [snap_id for snap_id, parent_id in get_all_snap_parents(sg_pg_conn, PG_MNT + '_pull')]

    # Since the pull procedure initializes a new connection, we have to commit our changes
    # in order to see them.
    sg_pg_conn.commit()
    pull(sg_pg_conn, '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB),
         PG_MNT, PG_MNT + '_pull')

    # Check out the newly-pulled commit and verify it has the same data.
    checkout(sg_pg_conn, PG_MNT + '_pull', head_1)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount_pull.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
    assert get_current_head(sg_pg_conn, PG_MNT + '_pull') == head_1


def test_push(sg_pg_conn):
    # First, pull from ourselves again like in the previous test.
    pull(sg_pg_conn, '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB),
         PG_MNT, PG_MNT + '_pull')
    checkout(sg_pg_conn, PG_MNT + '_pull', get_current_head(sg_pg_conn, PG_MNT))

    # Then, change our copy and commit.
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount_pull.fruits VALUES (3, 'mayonnaise')""")
    head_1 = commit(sg_pg_conn, PG_MNT + '_pull', storage_format='DIFF')
    # Since the pull procedure initializes a new connection, we have to commit our changes
    # in order to see them.
    sg_pg_conn.commit()

    # Now, push to remote.
    push(sg_pg_conn, '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB),
         PG_MNT, PG_MNT + '_pull')

    # Since we are connecting to ourselves, let's just reuse our connection to see if
    # the original mountpoint got updated.
    checkout(sg_pg_conn, PG_MNT, head_1)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
