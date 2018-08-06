# Integration tests that assume the whole infrastructure is running
import tempfile
from decimal import Decimal

import pytest

from splitgraph.commandline import _conn
from splitgraph.commands import *
from splitgraph.commands import unmount
from splitgraph.commands.misc import pg_table_exists
from splitgraph.constants import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PWD
from splitgraph.meta_handler import get_current_head, get_table, get_all_snap_parents, get_snap_parent, \
    get_downloaded_objects, get_existing_objects, get_external_object_locations
from splitgraph.pg_replication import has_pending_changes

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
    unmount(conn, PG_MNT + '_pull')
    _mount_postgres(conn, PG_MNT)
    yield conn
    unmount(conn, PG_MNT)
    unmount(conn, PG_MNT + '_pull')


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

    sg_pg_conn.commit()  # otherwise the WAL writer won't see this.
    head = get_current_head(sg_pg_conn, PG_MNT)
    change = diff(sg_pg_conn, PG_MNT, 'fruits', snap_1=head, snap_2=None)
    assert change == [(0,
                       '{"columnnames": ["fruit_id", "name"], "columntypes": ["integer", "character varying"], "columnvalues": [3, '
                       '"mayonnaise"]}'),
                      (1,
                       '{"oldkeys": {"keytypes": ["integer", "character varying"], "keyvalues": [1, "apple"], '
                       '"keynames": ["fruit_id", "name"]}}')]


@pytest.mark.parametrize("include_snap", [True, False])
def test_commit_diff(include_snap, sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")
        cur.execute("""DELETE FROM test_pg_mount.fruits WHERE name = 'apple'""")
        cur.execute("""UPDATE test_pg_mount.fruits SET name = 'guitar' WHERE fruit_id = 2""")

    head = get_current_head(sg_pg_conn, PG_MNT)
    new_head = commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

    # After commit, we should be switched to the new commit hash and there should be no differences.
    assert get_current_head(sg_pg_conn, PG_MNT) == new_head
    assert diff(sg_pg_conn, PG_MNT, 'fruits', snap_1=new_head, snap_2=None) == []

    # The diff between the old and the new snaps should be the same as in the previous test
    change = diff(sg_pg_conn, PG_MNT, 'fruits', snap_1=head, snap_2=new_head)
    assert change == [(0,
                       '{"columnnames": ["fruit_id", "name"], "columntypes": ["integer", "character varying"], '
                       '"columnvalues": [3, "mayonnaise"]}'),
                      (1,
                       '{"oldkeys": {"keytypes": ["integer", "character varying"], "keyvalues": [1, "apple"], '
                       '"keynames": ["fruit_id", "name"]}}'),
                      (2,
                       '{"columnnames": ["fruit_id", "name"], "columntypes": ["integer", "character varying"], '
                       '"oldkeys": {"keytypes": ["integer", "character varying"], "keyvalues": [2, "orange"], '
                       '"keynames": ["fruit_id", "name"]}, "columnvalues": [2, "guitar"]}')]
    assert diff(sg_pg_conn, PG_MNT, 'vegetables', snap_1=head, snap_2=new_head) == []


@pytest.mark.parametrize("include_snap", [True, False])
def test_multiple_mountpoint_commit_diff(include_snap, sg_pg_mg_conn):
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")
        cur.execute("""DELETE FROM test_pg_mount.fruits WHERE name = 'apple'""")
        cur.execute("""UPDATE test_pg_mount.fruits SET name = 'guitar' WHERE fruit_id = 2""")
        cur.execute("""UPDATE test_mg_mount.stuff SET duration = 11 WHERE name = 'James'""")
    # Both mountpoints have pending changes if we commit the PG connection.
    sg_pg_mg_conn.commit()
    assert has_pending_changes(sg_pg_mg_conn, MG_MNT) is True
    assert has_pending_changes(sg_pg_mg_conn, PG_MNT) is True

    head = get_current_head(sg_pg_mg_conn, PG_MNT)
    mongo_head = get_current_head(sg_pg_mg_conn, MG_MNT)
    new_head = commit(sg_pg_mg_conn, PG_MNT, include_snap=include_snap)

    change = diff(sg_pg_mg_conn, PG_MNT, 'fruits', snap_1=head, snap_2=new_head)
    assert change == [(0,
                       '{"columnnames": ["fruit_id", "name"], "columntypes": ["integer", "character varying"], '
                       '"columnvalues": [3, "mayonnaise"]}'),
                      (1,
                       '{"oldkeys": {"keytypes": ["integer", "character varying"], "keyvalues": [1, "apple"], '
                       '"keynames": ["fruit_id", "name"]}}'),
                      (2,
                       '{"columnnames": ["fruit_id", "name"], "columntypes": ["integer", "character varying"], '
                       '"oldkeys": {"keytypes": ["integer", "character varying"], "keyvalues": [2, "orange"], '
                       '"keynames": ["fruit_id", "name"]}, "columnvalues": [2, "guitar"]}')]

    # PG has no pending changes, Mongo does
    assert get_current_head(sg_pg_mg_conn, MG_MNT) == mongo_head
    assert has_pending_changes(sg_pg_mg_conn, MG_MNT) is True
    assert has_pending_changes(sg_pg_mg_conn, PG_MNT) is False

    # Discard the commit to the mongodb
    checkout(sg_pg_mg_conn, MG_MNT, mongo_head)
    assert has_pending_changes(sg_pg_mg_conn, MG_MNT) is False
    assert has_pending_changes(sg_pg_mg_conn, PG_MNT) is False
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT duration from test_mg_mount.stuff WHERE name = 'James'""")
        assert cur.fetchall() == [(Decimal(2),)]

    # Update and commit
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""UPDATE test_mg_mount.stuff SET duration = 15 WHERE name = 'James'""")
    sg_pg_mg_conn.commit()
    assert has_pending_changes(sg_pg_mg_conn, MG_MNT) is True
    new_mongo_head = commit(sg_pg_mg_conn, MG_MNT, include_snap=include_snap)
    assert has_pending_changes(sg_pg_mg_conn, MG_MNT) is False
    assert has_pending_changes(sg_pg_mg_conn, PG_MNT) is False

    checkout(sg_pg_mg_conn, MG_MNT, mongo_head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT duration from test_mg_mount.stuff WHERE name = 'James'""")
        assert cur.fetchall() == [(Decimal(2),)]

    checkout(sg_pg_mg_conn, MG_MNT, new_mongo_head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT duration from test_mg_mount.stuff WHERE name = 'James'""")
        assert cur.fetchall() == [(Decimal(15),)]
    assert has_pending_changes(sg_pg_mg_conn, MG_MNT) is False
    assert has_pending_changes(sg_pg_mg_conn, PG_MNT) is False


def test_delete_all_diff(sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""DELETE FROM test_pg_mount.fruits""")
    sg_pg_conn.commit()
    assert has_pending_changes(sg_pg_conn, PG_MNT) is True
    expected_diff = [(1,
    '{"oldkeys": {"keytypes": ["integer", "character varying"], "keyvalues": [1, "apple"], "keynames": ["fruit_id", "name"]}}'),
    (1,
    '{"oldkeys": {"keytypes": ["integer", "character varying"], "keyvalues": [2, "orange"], "keynames": ["fruit_id", "name"]}}')]
    assert diff(sg_pg_conn, PG_MNT, 'fruits', get_current_head(sg_pg_conn, PG_MNT), None) == expected_diff
    new_head = commit(sg_pg_conn, PG_MNT)
    assert has_pending_changes(sg_pg_conn, PG_MNT) is False
    assert diff(sg_pg_conn, PG_MNT, 'fruits', new_head, None) == []
    assert diff(sg_pg_conn, PG_MNT, 'fruits', get_snap_parent(sg_pg_conn, PG_MNT, new_head), new_head) == expected_diff



@pytest.mark.parametrize("include_snap", [True, False])
def test_diff_across_far_commits(include_snap, sg_pg_conn):
    head = get_current_head(sg_pg_conn, PG_MNT)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")
    commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

    with sg_pg_conn.cursor() as cur:
        cur.execute("""DELETE FROM test_pg_mount.fruits WHERE name = 'apple'""")
    commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

    with sg_pg_conn.cursor() as cur:
        cur.execute("""UPDATE test_pg_mount.fruits SET name = 'guitar' WHERE fruit_id = 2""")
    new_head = commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

    assert diff(sg_pg_conn, PG_MNT, 'fruits', head, new_head) == [
        (0,  # insert mayonnaise
         '{"columnnames": ["fruit_id", "name"], "columntypes": ["integer", "character varying"], "columnvalues": [3, "mayonnaise"]}'),
        (1,  # delete apple
         '{"oldkeys": {"keytypes": ["integer", "character varying"], "keyvalues": [1, "apple"], "keynames": ["fruit_id", "name"]}}'),
        (2,  # replace orange -> guitar
         '{"columnnames": ["fruit_id", "name"], "columntypes": ["integer", "character varying"], "oldkeys": {'
         '"keytypes": ["integer", "character varying"], "keyvalues": [2, "orange"], "keynames": ["fruit_id", '
         '"name"]}, "columnvalues": [2, "guitar"]}')]


@pytest.mark.parametrize("include_snap", [True, False])
def test_log_checkout(include_snap, sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")

    head = get_current_head(sg_pg_conn, PG_MNT)
    head_1 = commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

    with sg_pg_conn.cursor() as cur:
        cur.execute("""DELETE FROM test_pg_mount.fruits WHERE name = 'apple'""")

    head_2 = commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

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


@pytest.mark.parametrize("include_snap", [True, False])
def test_table_changes(include_snap, sg_pg_conn):
    with sg_pg_conn.cursor() as cur:
        cur.execute("""CREATE TABLE test_pg_mount.fruits_copy AS SELECT * FROM test_pg_mount.fruits""")

    head = get_current_head(sg_pg_conn, PG_MNT)
    # Check that table addition has been detected
    assert diff(sg_pg_conn, PG_MNT, 'fruits_copy', snap_1=head, snap_2=None) is True

    head_1 = commit(sg_pg_conn, PG_MNT, include_snap=include_snap)
    # Checkout the old head and make sure the table doesn't exist in it
    checkout(sg_pg_conn, PG_MNT, head)
    assert not pg_table_exists(sg_pg_conn, PG_MNT, 'fruits_copy')

    # Make sure the table is reflected in the diff even if we're on a different commit
    assert diff(sg_pg_conn, PG_MNT, 'fruits_copy', snap_1=head, snap_2=head_1) is True

    # Go back and now delete a table
    checkout(sg_pg_conn, PG_MNT, head_1)
    assert pg_table_exists(sg_pg_conn, PG_MNT, 'fruits_copy')
    with sg_pg_conn.cursor() as cur:
        cur.execute("""DROP TABLE test_pg_mount.fruits""")
    assert not pg_table_exists(sg_pg_conn, PG_MNT, 'fruits')

    # Make sure the diff shows it's been removed and commit it
    assert diff(sg_pg_conn, PG_MNT, 'fruits', snap_1=head_1, snap_2=None) is False
    head_2 = commit(sg_pg_conn, PG_MNT, include_snap=include_snap)

    # Go through the 3 commits and ensure the table existence is maintained
    checkout(sg_pg_conn, PG_MNT, head)
    assert pg_table_exists(sg_pg_conn, PG_MNT, 'fruits')
    checkout(sg_pg_conn, PG_MNT, head_1)
    assert pg_table_exists(sg_pg_conn, PG_MNT, 'fruits')
    checkout(sg_pg_conn, PG_MNT, head_2)
    assert not pg_table_exists(sg_pg_conn, PG_MNT, 'fruits')


def test_empty_diff_reuses_object(sg_pg_conn):
    head = get_current_head(sg_pg_conn, PG_MNT)
    head_1 = commit(sg_pg_conn, PG_MNT)

    table_meta_1 = get_table(sg_pg_conn, PG_MNT, 'fruits', head)
    table_meta_2 = get_table(sg_pg_conn, PG_MNT, 'fruits', head_1)

    assert table_meta_1 == table_meta_2
    assert len(table_meta_2) == 1  # Only SNAP stored even if we didn't ask for it, since this is just a pointer
    # to the previous version (which is a mount).
    assert table_meta_1[0][1] == 'SNAP'


@pytest.mark.parametrize("download_all", [True, False])
def test_pull(sg_pg_conn, download_all):
    # This is going to get awkward: we're going to pull a schema from ourselves since there's only one
    # pgcache for now.
    clone(sg_pg_conn, '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB),
         PG_MNT, PG_MNT + '_pull', download_all=download_all)
    checkout(sg_pg_conn, PG_MNT + '_pull', get_current_head(sg_pg_conn, PG_MNT))

    # Do something to fruits
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")

    head_1 = commit(sg_pg_conn, PG_MNT)

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
    pull(sg_pg_conn, PG_MNT + '_pull', remote='origin')

    # Check out the newly-pulled commit and verify it has the same data.
    checkout(sg_pg_conn, PG_MNT + '_pull', head_1)

    with sg_pg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM test_pg_mount_pull.fruits""")
        assert list(cur.fetchall()) == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]
    assert get_current_head(sg_pg_conn, PG_MNT + '_pull') == head_1


def test_pulls_with_lazy_object_downloads(sg_pg_conn):
    clone(sg_pg_conn, '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB),
         PG_MNT, PG_MNT + '_pull', download_all=False)
    # Make sure we haven't downloaded anything until checkout
    assert not get_downloaded_objects(sg_pg_conn, PG_MNT + '_pull')

    checkout(sg_pg_conn, PG_MNT + '_pull', get_current_head(sg_pg_conn, PG_MNT))
    assert len(get_downloaded_objects(sg_pg_conn, PG_MNT + '_pull')) == 2  # Original fruits and vegetables tables.
    assert get_downloaded_objects(sg_pg_conn, PG_MNT + '_pull') == get_existing_objects(sg_pg_conn, PG_MNT)

    # In the meantime, make two branches off of origin (a total of 3 commits)
    head = get_current_head(sg_pg_conn, PG_MNT)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')""")
    left = commit(sg_pg_conn, PG_MNT)

    checkout(sg_pg_conn, PG_MNT, head)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (3, 'mustard')""")
    right = commit(sg_pg_conn, PG_MNT)

    # Pull from origin.
    pull(sg_pg_conn, PG_MNT + '_pull', remote='origin', download_all=False)
    # Make sure we have the pointers to the three versions of the fruits table + the original vegetables
    assert len(get_existing_objects(sg_pg_conn, PG_MNT + '_pull')) == 4
    assert get_existing_objects(sg_pg_conn, PG_MNT + '_pull') == get_existing_objects(sg_pg_conn, PG_MNT)
    # Also make sure still only have the objects with the original fruits + vegetables tables
    assert len(get_downloaded_objects(sg_pg_conn, PG_MNT + '_pull')) == 2

    # Check out left commit: since it only depends on the root, we should download just the new version of fruits.
    checkout(sg_pg_conn, PG_MNT + '_pull', left)
    assert len(get_downloaded_objects(sg_pg_conn, PG_MNT + '_pull')) == 3 # now have 2 versions of fruits + 1 vegetables

    checkout(sg_pg_conn, PG_MNT + '_pull', right)
    assert len(get_downloaded_objects(sg_pg_conn, PG_MNT + '_pull')) == 4
    # Only now we actually have all the objects materialized.
    assert get_downloaded_objects(sg_pg_conn, PG_MNT + '_pull') == get_existing_objects(sg_pg_conn, PG_MNT)


def test_push(sg_pg_conn):
    # First, clone from ourselves again like in the previous test.
    clone(sg_pg_conn, '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB),
         PG_MNT, PG_MNT + '_pull')
    checkout(sg_pg_conn, PG_MNT + '_pull', get_current_head(sg_pg_conn, PG_MNT))

    # Then, change our copy and commit.
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount_pull.fruits VALUES (3, 'mayonnaise')""")
    head_1 = commit(sg_pg_conn, PG_MNT + '_pull')
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


def test_http_push_pull(sg_pg_conn):
    # Test pushing/pulling when the objects are uploaded to a remote storage instead of to the actual remote DB.
    # "Uploading" to a file for now because I can't seem to get uploading on an HTTP fixture working.

    remote_conn_string = '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, PG_HOST, PG_PORT, PG_DB)
    clone(sg_pg_conn, remote_conn_string, PG_MNT, PG_MNT + '_pull', download_all=False)
    # Add a couple of commits, this time on the cloned copy.
    head = get_current_head(sg_pg_conn, PG_MNT)
    checkout(sg_pg_conn, PG_MNT + '_pull', head)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount_pull.fruits VALUES (3, 'mayonnaise')""")
    left = commit(sg_pg_conn, PG_MNT + '_pull')
    checkout(sg_pg_conn, PG_MNT + '_pull', head)
    with sg_pg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount_pull.fruits VALUES (3, 'mustard')""")
    right = commit(sg_pg_conn, PG_MNT + '_pull')

    tmpdir = tempfile.mkdtemp()

    # Push to origin, but this time upload the actual objects instead.
    push(sg_pg_conn, remote_conn_string, PG_MNT, PG_MNT + '_pull', handler='FILE',
         handler_options={'path': tmpdir})

    # Check that the actual objects don't exist on the remote but are instead registered with an URL.
    assert len(get_downloaded_objects(sg_pg_conn, PG_MNT)) == 2  # just the two original tables on the remote
    objects = get_existing_objects(sg_pg_conn, PG_MNT)
    assert len(objects) == 4    # two original tables + two new versions of fruits
    # Both repos have two non-local objects
    ext_objects_orig = get_external_object_locations(sg_pg_conn, PG_MNT, list(objects))
    ext_objects_pull = get_external_object_locations(sg_pg_conn, PG_MNT + '_pull', list(objects))
    assert len(ext_objects_orig) == 2
    assert ext_objects_orig == ext_objects_pull

    # Destroy the pulled mountpoint and recreate it again.
    unmount(sg_pg_conn, PG_MNT + '_pull')
    clone(sg_pg_conn, remote_conn_string, PG_MNT, PG_MNT + '_pull', download_all=False)

    # Proceed as per the lazy checkout tests to make sure we don't download more than required.
    assert len(get_existing_objects(sg_pg_conn, PG_MNT + '_pull')) == 4
    assert get_existing_objects(sg_pg_conn, PG_MNT + '_pull') == get_existing_objects(sg_pg_conn, PG_MNT)
    assert len(get_downloaded_objects(sg_pg_conn, PG_MNT + '_pull')) == 2

    checkout(sg_pg_conn, PG_MNT + '_pull', left)
    assert len(get_downloaded_objects(sg_pg_conn, PG_MNT + '_pull')) == 3 # now have 2 versions of fruits + 1 vegetables

    checkout(sg_pg_conn, PG_MNT + '_pull', right)
    assert len(get_downloaded_objects(sg_pg_conn, PG_MNT + '_pull')) == 4
    assert get_downloaded_objects(sg_pg_conn, PG_MNT + '_pull') == get_existing_objects(sg_pg_conn, PG_MNT)
