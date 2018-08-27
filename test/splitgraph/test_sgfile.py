import os
import tempfile

import pytest

from splitgraph.commands import checkout, commit, push, unmount, clone, get_log
from splitgraph.commands.misc import cleanup_objects
from splitgraph.constants import SPLITGRAPH_META_SCHEMA, PG_USER, PG_PWD, PG_DB, SplitGraphException
from splitgraph.meta_handler import get_current_head, get_snap_parent, set_tag, get_current_mountpoints_hashes, \
    get_downloaded_objects, get_existing_objects, get_external_object_locations, get_table_with_format, get_tables_at
from splitgraph.pg_utils import pg_table_exists
from splitgraph.sgfile.execution import execute_commands
from splitgraph.sgfile.parsing import preprocess
from test.splitgraph.conftest import SNAPPER_HOST, SNAPPER_PORT

SGFILE_ROOT = os.path.join(os.path.dirname(__file__), '../resources/')


def _load_sgfile(name):
    with open(SGFILE_ROOT + name, 'r') as f:
        return f.read()


PARSING_TEST_SGFILE = _load_sgfile('import_remote_multiple.sgfile')


def test_sgfile_preprocessor_missing_params():
    with pytest.raises(SplitGraphException) as e:
        preprocess(PARSING_TEST_SGFILE, params={'SNAPPER': '127.0.0.1'})
    assert '${TAG}' in str(e.value)
    assert '${SNAPPER_PORT}' in str(e.value)
    assert '${SNAPPER}' not in str(e.value)
    assert '${ESCAPED}' not in str(e.value)


def test_sgfile_preprocessor_escaping():
    commands = preprocess(PARSING_TEST_SGFILE,
                          params={'SNAPPER': '127.0.0.1', 'SNAPPER_PORT': 5678, 'TAG': 'tag-v1-whatever'})
    print(commands)
    assert '${TAG}' not in commands
    assert '${SNAPPER}' not in commands
    assert '\\${ESCAPED}' not in commands
    assert '${ESCAPED}' in commands
    assert '127.0.0.1' in commands
    assert '5678' in commands
    assert 'tag-v1-whatever' in commands


def test_basic_sgfile(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, _load_sgfile('create_table.sgfile'), output='output')
    log = list(reversed(get_log(sg_pg_mg_conn, 'output', get_current_head(sg_pg_mg_conn, 'output'))))

    checkout(sg_pg_mg_conn, 'output', log[1])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.my_fruits""")
        assert cur.fetchall() == []

    checkout(sg_pg_mg_conn, 'output', log[2])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.my_fruits""")
        assert cur.fetchall() == [(1, 'pineapple')]

    checkout(sg_pg_mg_conn, 'output', log[3])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.my_fruits""")
        assert cur.fetchall() == [(1, 'pineapple'), (2, 'banana')]


def test_update_without_import_sgfile(sg_pg_mg_conn):
    # Test that correct commits are produced by executing an sgfile (both against newly created and already
    # existing tables on an existing mountpoint)
    execute_commands(sg_pg_mg_conn, _load_sgfile('update_without_import.sgfile'), output='output')
    log = get_log(sg_pg_mg_conn, 'output', get_current_head(sg_pg_mg_conn, 'output'))

    checkout(sg_pg_mg_conn, 'output', log[1])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.my_fruits""")
        assert cur.fetchall() == []

    checkout(sg_pg_mg_conn, 'output', log[0])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.my_fruits""")
        assert cur.fetchall() == [(1, 'pineapple')]


def test_local_import_sgfile(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local.sgfile'), output='output')
    head = get_current_head(sg_pg_mg_conn, 'output')
    old_head = get_snap_parent(sg_pg_mg_conn, 'output', head)

    checkout(sg_pg_mg_conn, 'output', old_head)
    assert not pg_table_exists(sg_pg_mg_conn, 'output', 'my_fruits')
    assert not pg_table_exists(sg_pg_mg_conn, 'output', 'fruits')

    checkout(sg_pg_mg_conn, 'output', head)
    assert pg_table_exists(sg_pg_mg_conn, 'output', 'my_fruits')
    assert not pg_table_exists(sg_pg_mg_conn, 'output', 'fruits')


def test_advanced_sgfile(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local_multiple_with_queries.sgfile'), output='output')
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
        cur.execute("""SELECT * FROM output.my_fruits""")
        assert cur.fetchall() == [(2, 'orange')]


def test_sgfile_cached(sg_pg_mg_conn):
    # Check that no new commits/snaps are created if we rerun the same sgfile
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local_multiple_with_queries.sgfile'), output='output')
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT snap_id FROM %s.snap_tree WHERE mountpoint = 'output'""" % SPLITGRAPH_META_SCHEMA)
        commits = [c[0] for c in cur.fetchall()]
    assert len(commits) == 4

    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local_multiple_with_queries.sgfile'), output='output')
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT snap_id FROM %s.snap_tree WHERE mountpoint = 'output'""" % SPLITGRAPH_META_SCHEMA)
        new_commits = [c[0] for c in cur.fetchall()]
    assert new_commits == commits


def _add_multitag_dataset_to_snapper(snapper_conn):
    set_tag(snapper_conn, 'test_pg_mount', get_current_head(snapper_conn, 'test_pg_mount'), 'v1')
    with snapper_conn.cursor() as cur:
        cur.execute("DELETE FROM test_pg_mount.fruits WHERE fruit_id = 1")
    new_head = commit(snapper_conn, 'test_pg_mount')
    set_tag(snapper_conn, 'test_pg_mount', new_head, 'v2')
    snapper_conn.commit()
    return new_head


def test_sgfile_remote(sg_pg_mg_conn, snapper_conn):
    new_head = _add_multitag_dataset_to_snapper(snapper_conn)

    # We use the v1 tag when importing from the remote, so fruit_id = 1 still exists there.
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_remote_multiple.sgfile'), params={'SNAPPER': SNAPPER_HOST,
                                                                                           'SNAPPER_PORT': SNAPPER_PORT,
                                                                                           'TAG': 'v1'},
                     output='output')
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]

    # Now run the commands against v2 and make sure the fruit_id = 1 has disappeared from the output.
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_remote_multiple.sgfile'), params={'SNAPPER': SNAPPER_HOST,
                                                                                           'SNAPPER_PORT': SNAPPER_PORT,
                                                                                           'TAG': 'v2'},
                     output='output')
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(2, 'orange', 'carrot')]


def test_sgfile_remote_hash(sg_pg_mg_conn, snapper_conn):
    head = get_current_head(snapper_conn, 'test_pg_mount')
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_remote_multiple.sgfile'), params={'SNAPPER': SNAPPER_HOST,
                                                                                           'SNAPPER_PORT': SNAPPER_PORT,
                                                                                           'TAG': head[:10]},
                     output='output')
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]


def test_import_updating_sgfile_with_uploading(empty_pg_conn, snapper_conn):
    execute_commands(empty_pg_conn, _load_sgfile('import_and_update.sgfile'), params={'SNAPPER': SNAPPER_HOST,
                                                                                      'SNAPPER_PORT': SNAPPER_PORT},
                     output='output')
    head = get_current_head(empty_pg_conn, 'output')

    assert len(get_existing_objects(empty_pg_conn)) == 4  # Two original tables + two updates

    with tempfile.TemporaryDirectory() as tmpdir:
        # Push with upload
        conn_string = '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, SNAPPER_HOST, SNAPPER_PORT, PG_DB)
        push(empty_pg_conn, conn_string, 'output', 'output',
             handler='FILE', handler_options={'path': tmpdir})
        # Unmount everything locally and cleanup
        unmount(empty_pg_conn, 'output')
        cleanup_objects(empty_pg_conn)
        assert not get_existing_objects(empty_pg_conn)

        clone(empty_pg_conn, conn_string, 'output', 'output', download_all=False)

        assert not get_downloaded_objects(empty_pg_conn)
        existing_objects = list(get_existing_objects(empty_pg_conn))
        assert len(existing_objects) == 4  # Two original tables + two updates
        # Only 2 objects are stored externally (the other two have been on the remote the whole time)
        assert len(get_external_object_locations(empty_pg_conn, existing_objects)) == 2

        checkout(empty_pg_conn, 'output', head)
        with empty_pg_conn.cursor() as cur:
            cur.execute("""SELECT fruit_id, name FROM output.my_fruits""")
            assert cur.fetchall() == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

            cur.execute("""SELECT vegetable_id, name FROM output.vegetables""")
            assert sorted(cur.fetchall()) == [(1, 'cucumber'), (2, 'carrot')]


def test_sgfile_end_to_end_with_uploading(sg_pg_mg_conn, snapper_conn):
    # An end-to-end test:
    #   * Create a derived dataset from some tables imported from the snapper
    #   * Push it back to the snapper, uploading all objects to "HTTP" (instead of the snapper itself)
    #   * Delete everything from pgcache
    #   * Run another sgfile that depends on the just-pushed dataset (and does lazy checkouts to
    #     get the required tables).

    # Do the same setting up first and run the sgfile against the remote data.
    new_head = _add_multitag_dataset_to_snapper(snapper_conn)
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_remote_multiple.sgfile'), params={'SNAPPER': SNAPPER_HOST,
                                                                                           'TAG': 'v1',
                                                                                           'SNAPPER_PORT': SNAPPER_PORT},
                     output='output')

    with tempfile.TemporaryDirectory() as tmpdir:
        # Push with upload
        push(sg_pg_mg_conn, '%s:%s@%s:%s/%s' % (PG_USER, PG_PWD, SNAPPER_HOST, SNAPPER_PORT, PG_DB), 'output', 'output',
             handler='FILE', handler_options={'path': tmpdir})
        # Unmount everything locally and cleanup
        for mountpoint, _ in get_current_mountpoints_hashes(sg_pg_mg_conn):
            unmount(sg_pg_mg_conn, mountpoint)
        cleanup_objects(sg_pg_mg_conn)

        execute_commands(sg_pg_mg_conn, _load_sgfile('import_from_preuploaded_remote.sgfile'),
                         params={'SNAPPER': SNAPPER_HOST,
                                 'SNAPPER_PORT': SNAPPER_PORT}, output='output_stage_2')

        with sg_pg_mg_conn.cursor() as cur:
            cur.execute("""SELECT id, name, fruit, vegetable FROM output_stage_2.diet""")
            assert cur.fetchall() == [(2, 'James', 'orange', 'carrot')]


def test_sgfile_schema_changes(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, _load_sgfile('schema_changes.sgfile'), output='output')
    old_output_head = get_current_head(sg_pg_mg_conn, 'output')

    # Then, alter the dataset and rerun the sgfile.
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""INSERT INTO test_pg_mount.fruits VALUES (12, 'mayonnaise')""")
    commit(sg_pg_mg_conn, 'test_pg_mount')
    execute_commands(sg_pg_mg_conn, _load_sgfile('schema_changes.sgfile'), output='output')
    new_output_head = get_current_head(sg_pg_mg_conn, 'output')

    checkout(sg_pg_mg_conn, 'output', old_output_head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.spirit_fruits""")
        assert cur.fetchall() == [('James', 'orange', 12)]

    checkout(sg_pg_mg_conn, 'output', new_output_head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.spirit_fruits""")
        # Mayonnaise joined with Alex, ID 12 + 10 = 22.
        assert cur.fetchall() == [('James', 'orange', 12), ('Alex', 'mayonnaise', 22)]


def test_import_with_custom_query(sg_pg_mg_conn):
    # Mostly a lazy way for the test to distinguish between the importer storing the results of a query as a snap
    # and pointing the commit history to the diff for unchanged objects.
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')")
        cur.execute("INSERT INTO test_pg_mount.vegetables VALUES (3, 'oregano')")
    commit(sg_pg_mg_conn, 'test_pg_mount')

    execute_commands(sg_pg_mg_conn, _load_sgfile('import_with_custom_query.sgfile'), output='output')
    head = get_current_head(sg_pg_mg_conn, 'output')
    old_head = get_snap_parent(sg_pg_mg_conn, 'output', head)

    # First two tables imported as snaps since they had a custom query, the other two are diffs (basically a commit
    # pointing to the same object as test_pg_mount has).
    tables = ['my_fruits', 'o_vegetables', 'vegetables', 'all_fruits']
    contents = [[(2, 'orange')], [(1, 'potato'), (3, 'oregano')],
                [(1, 'potato'), (2, 'carrot'), (3, 'oregano')], [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]]

    checkout(sg_pg_mg_conn, 'output', old_head)
    for t in tables:
        assert not pg_table_exists(sg_pg_mg_conn, 'output', t)

    checkout(sg_pg_mg_conn, 'output', head)
    with sg_pg_mg_conn.cursor() as cur:
        for t, c in zip(tables, contents):
            cur.execute("""SELECT * FROM output.%s""" % t)
            assert cur.fetchall() == c

    for t in tables:
        # Tables with queries stored as snaps, actually imported tables share objects with test_pg_mount.
        if t in ['my_fruits', 'o_vegetables']:
            assert get_table_with_format(sg_pg_mg_conn, 'output', t, head, 'SNAP') is not None
            assert get_table_with_format(sg_pg_mg_conn, 'output', t, head, 'DIFF') is None
        else:
            assert get_table_with_format(sg_pg_mg_conn, 'output', t, head, 'SNAP') is None
            assert get_table_with_format(sg_pg_mg_conn, 'output', t, head, 'DIFF') is not None


def test_import_mount(empty_pg_conn):
    execute_commands(empty_pg_conn, _load_sgfile('import_from_mounted_db.sgfile'), output='output')

    head = get_current_head(empty_pg_conn, 'output')
    old_head = get_snap_parent(empty_pg_conn, 'output', head)

    checkout(empty_pg_conn, 'output', old_head)
    tables = ['my_fruits', 'o_vegetables', 'vegetables', 'all_fruits']
    contents = [[(2, 'orange')], [(1, 'potato')], [(1, 'potato'), (2, 'carrot')], [(1, 'apple'), (2, 'orange')]]
    for t in tables:
        assert not pg_table_exists(empty_pg_conn, 'output', t)

    checkout(empty_pg_conn, 'output', head)
    with empty_pg_conn.cursor() as cur:
        for t, c in zip(tables, contents):
            cur.execute("""SELECT * FROM output.%s""" % t)
            assert cur.fetchall() == c

    # All imported tables stored as snapshots
    for t in tables:
        assert get_table_with_format(empty_pg_conn, 'output', t, head, 'SNAP') is not None
        assert get_table_with_format(empty_pg_conn, 'output', t, head, 'DIFF') is None


def test_import_all(empty_pg_conn):
    execute_commands(empty_pg_conn, _load_sgfile('import_all_from_mounted.sgfile'), output='output')

    head = get_current_head(empty_pg_conn, 'output')
    old_head = get_snap_parent(empty_pg_conn, 'output', head)

    checkout(empty_pg_conn, 'output', old_head)
    tables = ['vegetables', 'fruits']
    contents = [[(1, 'potato'), (2, 'carrot')], [(1, 'apple'), (2, 'orange')]]
    for t in tables:
        assert not pg_table_exists(empty_pg_conn, 'output', t)

    checkout(empty_pg_conn, 'output', head)
    with empty_pg_conn.cursor() as cur:
        for t, c in zip(tables, contents):
            cur.execute("""SELECT * FROM output.%s""" % t)
            assert cur.fetchall() == c


def test_from_remote(empty_pg_conn, snapper_conn):
    _add_multitag_dataset_to_snapper(snapper_conn)
    # Test running commands that base new datasets on a remote repository.
    execute_commands(empty_pg_conn, _load_sgfile('from_remote.sgfile'), output='output',
                     params={'SNAPPER': SNAPPER_HOST,
                             'SNAPPER_PORT': SNAPPER_PORT,
                             'TAG': 'v1'})

    new_head = get_current_head(empty_pg_conn, 'output')
    # Go back to the parent: the two source tables should exist there
    checkout(empty_pg_conn, 'output', get_snap_parent(empty_pg_conn, 'output', new_head))
    assert pg_table_exists(empty_pg_conn, 'output', 'fruits')
    assert pg_table_exists(empty_pg_conn, 'output', 'vegetables')
    assert not pg_table_exists(empty_pg_conn, 'output', 'join_table')

    checkout(empty_pg_conn, 'output', new_head)
    assert pg_table_exists(empty_pg_conn, 'output', 'fruits')
    assert pg_table_exists(empty_pg_conn, 'output', 'vegetables')
    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.join_table")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]

    # Now run the same sgfile but from the v2 of the remote (where row 1 has been removed from the fruits table)
    # First, remove the output mountpoint (the executor tries to fetch the commit 0000 from it otherwise which
    # doesn't exist).
    unmount(empty_pg_conn, 'output')
    execute_commands(empty_pg_conn, _load_sgfile('from_remote.sgfile'), output='output',
                     params={'SNAPPER': SNAPPER_HOST,
                             'SNAPPER_PORT': SNAPPER_PORT,
                             'TAG': 'v2'})

    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.join_table")
        assert cur.fetchall() == [(2, 'orange', 'carrot')]


def test_from_remote_hash(empty_pg_conn, snapper_conn):
    head = get_current_head(snapper_conn, 'test_pg_mount')
    # Test running commands that base new datasets on a remote repository.
    execute_commands(empty_pg_conn, _load_sgfile('from_remote.sgfile'), output='output',
                     params={'SNAPPER': SNAPPER_HOST,
                             'SNAPPER_PORT': SNAPPER_PORT,
                             'TAG': head[:10]})

    assert pg_table_exists(empty_pg_conn, 'output', 'fruits')
    assert pg_table_exists(empty_pg_conn, 'output', 'vegetables')
    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.join_table")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]


def test_from_multistage(empty_pg_conn, snapper_conn):
    _add_multitag_dataset_to_snapper(snapper_conn)

    # Produces two mountpoints: output and output_stage_2
    execute_commands(empty_pg_conn, _load_sgfile('from_remote_multistage.sgfile'),
                     params={'SNAPPER': SNAPPER_HOST,
                             'SNAPPER_PORT': SNAPPER_PORT,
                             'TAG': 'v1'})

    # Check the final output ('output_stage_2': it should only have one single table object (a SNAP with the join_table
    # from the first stage, 'output'.
    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output_stage_2.balanced_diet")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]
    head = get_current_head(empty_pg_conn, 'output_stage_2')
    # Check the commit is based on the original empty image.
    assert get_snap_parent(empty_pg_conn, 'output_stage_2', head) == '0' * 64
    assert get_tables_at(empty_pg_conn, 'output_stage_2', head) == ['balanced_diet']
    assert get_table_with_format(empty_pg_conn, 'output_stage_2', 'balanced_diet', head, 'SNAP') is not None
    assert get_table_with_format(empty_pg_conn, 'output_stage_2', 'balanced_diet', head, 'DIFF') is None


def test_from_local(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, _load_sgfile('from_local.sgfile'), output='output')

    new_head = get_current_head(sg_pg_mg_conn, 'output')
    # Go back to the parent: the two source tables should exist there
    checkout(sg_pg_mg_conn, 'output', get_snap_parent(sg_pg_mg_conn, 'output', new_head))
    assert pg_table_exists(sg_pg_mg_conn, 'output', 'fruits')
    assert pg_table_exists(sg_pg_mg_conn, 'output', 'vegetables')
    assert not pg_table_exists(sg_pg_mg_conn, 'output', 'join_table')

    checkout(sg_pg_mg_conn, 'output', new_head)
    assert pg_table_exists(sg_pg_mg_conn, 'output', 'fruits')
    assert pg_table_exists(sg_pg_mg_conn, 'output', 'vegetables')
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.join_table")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]


def test_sgfile_with_external_sql(sg_pg_mg_conn, snapper_conn):
    # Tests are running from root so we pass in the path to the SQL manually to the sgfile.
    execute_commands(sg_pg_mg_conn, _load_sgfile('external_sql.sgfile'),
                     params={'SNAPPER': SNAPPER_HOST,
                             'SNAPPER_PORT': SNAPPER_PORT,
                             'EXTERNAL_SQL_FILE': SGFILE_ROOT + 'external_sql.sql'},
                     output='output')
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]
