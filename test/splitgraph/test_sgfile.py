import os

import pytest
from splitgraph.commands import checkout, commit, push, unmount, clone, get_log
from splitgraph.commands.misc import cleanup_objects
from splitgraph.constants import SplitGraphException, to_repository as R
from splitgraph.meta_handler.images import get_image, get_all_images_parents
from splitgraph.meta_handler.misc import get_current_repositories
from splitgraph.meta_handler.objects import get_existing_objects, get_downloaded_objects, get_external_object_locations
from splitgraph.meta_handler.tables import get_tables_at, get_object_for_table
from splitgraph.meta_handler.tags import get_current_head, set_tag
from splitgraph.pg_utils import pg_table_exists
from splitgraph.sgfile.execution import execute_commands
from splitgraph.sgfile.parsing import preprocess
from test.splitgraph.conftest import REMOTE_CONN_STRING, OUTPUT, PG_MNT

SGFILE_ROOT = os.path.join(os.path.dirname(__file__), '../resources/')


def _load_sgfile(name):
    with open(SGFILE_ROOT + name, 'r') as f:
        return f.read()


PARSING_TEST_SGFILE = _load_sgfile('import_remote_multiple.sgfile')


def test_sgfile_preprocessor_missing_params():
    with pytest.raises(SplitGraphException) as e:
        preprocess(PARSING_TEST_SGFILE, params={})
    assert '${TAG}' in str(e.value)
    assert '${ESCAPED}' not in str(e.value)


def test_sgfile_preprocessor_escaping():
    commands = preprocess(PARSING_TEST_SGFILE,
                          params={'TAG': 'tag-v1-whatever'})
    print(commands)
    assert '${TAG}' not in commands
    assert '\\${ESCAPED}' not in commands
    assert '${ESCAPED}' in commands
    assert 'tag-v1-whatever' in commands


def test_basic_sgfile(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, _load_sgfile('create_table.sgfile'), output=OUTPUT)
    log = list(reversed(get_log(sg_pg_mg_conn, OUTPUT, get_current_head(sg_pg_mg_conn, OUTPUT))))

    checkout(sg_pg_mg_conn, OUTPUT, log[1])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.my_fruits""")
        assert cur.fetchall() == []

    checkout(sg_pg_mg_conn, OUTPUT, log[2])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.my_fruits""")
        assert cur.fetchall() == [(1, 'pineapple')]

    checkout(sg_pg_mg_conn, OUTPUT, log[3])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.my_fruits""")
        assert cur.fetchall() == [(1, 'pineapple'), (2, 'banana')]


def test_update_without_import_sgfile(sg_pg_mg_conn):
    # Test that correct commits are produced by executing an sgfile (both against newly created and already
    # existing tables on an existing mountpoint)
    execute_commands(sg_pg_mg_conn, _load_sgfile('update_without_import.sgfile'), output=OUTPUT)
    log = get_log(sg_pg_mg_conn, OUTPUT, get_current_head(sg_pg_mg_conn, OUTPUT))

    checkout(sg_pg_mg_conn, OUTPUT, log[1])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.my_fruits""")
        assert cur.fetchall() == []

    checkout(sg_pg_mg_conn, OUTPUT, log[0])
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.my_fruits""")
        assert cur.fetchall() == [(1, 'pineapple')]


def test_local_import_sgfile(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local.sgfile'), output=OUTPUT)
    head = get_current_head(sg_pg_mg_conn, OUTPUT)
    old_head = get_image(sg_pg_mg_conn, OUTPUT, head).parent_id

    checkout(sg_pg_mg_conn, OUTPUT, old_head)
    assert not pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'my_fruits')
    assert not pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'fruits')

    checkout(sg_pg_mg_conn, OUTPUT, head)
    assert pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'my_fruits')
    assert not pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'fruits')


def test_advanced_sgfile(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local_multiple_with_queries.sgfile'), output=OUTPUT)
    head = get_current_head(sg_pg_mg_conn, OUTPUT)

    assert pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'my_fruits')
    assert pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'vegetables')
    assert not pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'fruits')
    assert pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'join_table')

    old_head = get_image(sg_pg_mg_conn, OUTPUT, head).parent_id
    checkout(sg_pg_mg_conn, OUTPUT, old_head)
    assert not pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'join_table')
    checkout(sg_pg_mg_conn, OUTPUT, head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(2, 'orange', 'carrot')]
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.my_fruits""")
        assert cur.fetchall() == [(2, 'orange')]


def test_sgfile_cached(sg_pg_mg_conn):
    # Check that no new commits/snaps are created if we rerun the same sgfile
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local_multiple_with_queries.sgfile'), output=OUTPUT)
    images = get_all_images_parents(sg_pg_mg_conn, OUTPUT)
    assert len(images) == 4

    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local_multiple_with_queries.sgfile'), output=OUTPUT)
    new_images = get_all_images_parents(sg_pg_mg_conn, OUTPUT)
    assert new_images == images


def _add_multitag_dataset_to_remote_driver(remote_driver_conn):
    set_tag(remote_driver_conn, PG_MNT, get_current_head(remote_driver_conn, PG_MNT), 'v1')
    with remote_driver_conn.cursor() as cur:
        cur.execute("DELETE FROM \"test/pg_mount\".fruits WHERE fruit_id = 1")
    new_head = commit(remote_driver_conn, PG_MNT)
    set_tag(remote_driver_conn, PG_MNT, new_head, 'v2')
    remote_driver_conn.commit()
    return new_head


def test_sgfile_remote(empty_pg_conn, remote_driver_conn):
    new_head = _add_multitag_dataset_to_remote_driver(remote_driver_conn)

    # We use the v1 tag when importing from the remote, so fruit_id = 1 still exists there.
    execute_commands(empty_pg_conn, _load_sgfile('import_remote_multiple.sgfile'), params={'TAG': 'v1'},
                     output=OUTPUT)
    with empty_pg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]

    # Now run the commands against v2 and make sure the fruit_id = 1 has disappeared from the output.
    execute_commands(empty_pg_conn, _load_sgfile('import_remote_multiple.sgfile'), params={'TAG': 'v2'},
                     output=OUTPUT)
    with empty_pg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(2, 'orange', 'carrot')]


def test_sgfile_remote_hash(empty_pg_conn, remote_driver_conn):
    head = get_current_head(remote_driver_conn, PG_MNT)
    execute_commands(empty_pg_conn, _load_sgfile('import_remote_multiple.sgfile'), params={'TAG': head[:10]},
                     output=OUTPUT)
    with empty_pg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]


def test_import_updating_sgfile_with_uploading(empty_pg_conn, remote_driver_conn):
    execute_commands(empty_pg_conn, _load_sgfile('import_and_update.sgfile'), output=OUTPUT)
    head = get_current_head(empty_pg_conn, OUTPUT)

    assert len(get_existing_objects(empty_pg_conn)) == 4  # Two original tables + two updates

    # Push with upload. Have to specify the remote connection string since we are pushing a new repository.
    push(empty_pg_conn, OUTPUT, remote_conn_string=REMOTE_CONN_STRING, handler='S3', handler_options={})
    # Unmount everything locally and cleanup
    unmount(empty_pg_conn, OUTPUT)
    cleanup_objects(empty_pg_conn)
    assert not get_existing_objects(empty_pg_conn)

    clone(empty_pg_conn, OUTPUT, download_all=False)

    assert not get_downloaded_objects(empty_pg_conn)
    existing_objects = list(get_existing_objects(empty_pg_conn))
    assert len(existing_objects) == 4  # Two original tables + two updates
    # Only 2 objects are stored externally (the other two have been on the remote the whole time)
    assert len(get_external_object_locations(empty_pg_conn, existing_objects)) == 2

    checkout(empty_pg_conn, OUTPUT, head)
    with empty_pg_conn.cursor() as cur:
        cur.execute("""SELECT fruit_id, name FROM output.my_fruits""")
        assert cur.fetchall() == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]

        cur.execute("""SELECT vegetable_id, name FROM output.vegetables""")
        assert sorted(cur.fetchall()) == [(1, 'cucumber'), (2, 'carrot')]


def test_sgfile_end_to_end_with_uploading(empty_pg_conn, remote_driver_conn):
    # An end-to-end test:
    #   * Create a derived dataset from some tables imported from the remote driver
    #   * Push it back to the remote driver, uploading all objects to S3 (instead of the remote driver itself)
    #   * Delete everything from pgcache
    #   * Run another sgfile that depends on the just-pushed dataset (and does lazy checkouts to
    #     get the required tables).

    # Do the same setting up first and run the sgfile against the remote data.
    new_head = _add_multitag_dataset_to_remote_driver(remote_driver_conn)
    execute_commands(empty_pg_conn, _load_sgfile('import_remote_multiple.sgfile'), params={'TAG': 'v1'},
                     output=OUTPUT)

    # Push with upload
    push(empty_pg_conn, OUTPUT, remote_conn_string=REMOTE_CONN_STRING, handler='S3',
         handler_options={})
    # Unmount everything locally and cleanup
    for mountpoint, _ in get_current_repositories(empty_pg_conn):
        unmount(empty_pg_conn, mountpoint)
    cleanup_objects(empty_pg_conn)

    execute_commands(empty_pg_conn, _load_sgfile('import_from_preuploaded_remote.sgfile'),
                     output=R('output_stage_2'))

    with empty_pg_conn.cursor() as cur:
        cur.execute("""SELECT id, name, fruit, vegetable FROM output_stage_2.diet""")
        assert cur.fetchall() == [(2, 'James', 'orange', 'carrot')]


def test_sgfile_schema_changes(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, _load_sgfile('schema_changes.sgfile'), output=OUTPUT)
    old_output_head = get_current_head(sg_pg_mg_conn, OUTPUT)

    # Then, alter the dataset and rerun the sgfile.
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""INSERT INTO "test/pg_mount".fruits VALUES (12, 'mayonnaise')""")
    commit(sg_pg_mg_conn, PG_MNT)
    execute_commands(sg_pg_mg_conn, _load_sgfile('schema_changes.sgfile'), output=OUTPUT)
    new_output_head = get_current_head(sg_pg_mg_conn, OUTPUT)

    checkout(sg_pg_mg_conn, OUTPUT, old_output_head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.spirit_fruits""")
        assert cur.fetchall() == [('James', 'orange', 12)]

    checkout(sg_pg_mg_conn, OUTPUT, new_output_head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT * FROM output.spirit_fruits""")
        # Mayonnaise joined with Alex, ID 12 + 10 = 22.
        assert cur.fetchall() == [('James', 'orange', 12), ('Alex', 'mayonnaise', 22)]


def test_import_with_custom_query(sg_pg_mg_conn):
    # Mostly a lazy way for the test to distinguish between the importer storing the results of a query as a snap
    # and pointing the commit history to the diff for unchanged objects.
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')")
        cur.execute("INSERT INTO \"test/pg_mount\".vegetables VALUES (3, 'oregano')")
    commit(sg_pg_mg_conn, PG_MNT)

    execute_commands(sg_pg_mg_conn, _load_sgfile('import_with_custom_query.sgfile'), output=OUTPUT)
    head = get_current_head(sg_pg_mg_conn, OUTPUT)
    old_head = get_image(sg_pg_mg_conn, OUTPUT, head).parent_id

    # First two tables imported as snaps since they had a custom query, the other two are diffs (basically a commit
    # pointing to the same object as test/pg_mount has).
    tables = ['my_fruits', 'o_vegetables', 'vegetables', 'all_fruits']
    contents = [[(2, 'orange')], [(1, 'potato'), (3, 'oregano')],
                [(1, 'potato'), (2, 'carrot'), (3, 'oregano')], [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]]

    checkout(sg_pg_mg_conn, OUTPUT, old_head)
    for t in tables:
        assert not pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), t)

    checkout(sg_pg_mg_conn, OUTPUT, head)
    with sg_pg_mg_conn.cursor() as cur:
        for t, c in zip(tables, contents):
            cur.execute("""SELECT * FROM output.%s""" % t)
            assert cur.fetchall() == c

    for t in tables:
        # Tables with queries stored as snaps, actually imported tables share objects with test/pg_mount.
        if t in ['my_fruits', 'o_vegetables']:
            assert get_object_for_table(sg_pg_mg_conn, OUTPUT, t, head, 'SNAP') is not None
            assert get_object_for_table(sg_pg_mg_conn, OUTPUT, t, head, 'DIFF') is None
        else:
            assert get_object_for_table(sg_pg_mg_conn, OUTPUT, t, head, 'SNAP') is None
            assert get_object_for_table(sg_pg_mg_conn, OUTPUT, t, head, 'DIFF') is not None


def test_import_mount(empty_pg_conn):
    execute_commands(empty_pg_conn, _load_sgfile('import_from_mounted_db.sgfile'), output=OUTPUT)

    head = get_current_head(empty_pg_conn, OUTPUT)
    old_head = get_image(empty_pg_conn, OUTPUT, head).parent_id

    checkout(empty_pg_conn, OUTPUT, old_head)
    tables = ['my_fruits', 'o_vegetables', 'vegetables', 'all_fruits']
    contents = [[(2, 'orange')], [(1, 'potato')], [(1, 'potato'), (2, 'carrot')], [(1, 'apple'), (2, 'orange')]]
    for t in tables:
        assert not pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), t)

    checkout(empty_pg_conn, OUTPUT, head)
    with empty_pg_conn.cursor() as cur:
        for t, c in zip(tables, contents):
            cur.execute("""SELECT * FROM output.%s""" % t)
            assert cur.fetchall() == c

    # All imported tables stored as snapshots
    for t in tables:
        assert get_object_for_table(empty_pg_conn, OUTPUT, t, head, 'SNAP') is not None
        assert get_object_for_table(empty_pg_conn, OUTPUT, t, head, 'DIFF') is None


def test_import_all(empty_pg_conn):
    execute_commands(empty_pg_conn, _load_sgfile('import_all_from_mounted.sgfile'), output=OUTPUT)

    head = get_current_head(empty_pg_conn, OUTPUT)
    old_head = get_image(empty_pg_conn, OUTPUT, head).parent_id

    checkout(empty_pg_conn, OUTPUT, old_head)
    tables = ['vegetables', 'fruits']
    contents = [[(1, 'potato'), (2, 'carrot')], [(1, 'apple'), (2, 'orange')]]
    for t in tables:
        assert not pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), t)

    checkout(empty_pg_conn, OUTPUT, head)
    with empty_pg_conn.cursor() as cur:
        for t, c in zip(tables, contents):
            cur.execute("""SELECT * FROM output.%s""" % t)
            assert cur.fetchall() == c


def test_from_remote(empty_pg_conn, remote_driver_conn):
    _add_multitag_dataset_to_remote_driver(remote_driver_conn)
    # Test running commands that base new datasets on a remote repository.
    execute_commands(empty_pg_conn, _load_sgfile('from_remote.sgfile'), output=OUTPUT,
                     params={'TAG': 'v1'})

    new_head = get_current_head(empty_pg_conn, OUTPUT)
    # Go back to the parent: the two source tables should exist there
    checkout(empty_pg_conn, OUTPUT, get_image(empty_pg_conn, OUTPUT, new_head).parent_id)
    assert pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), 'fruits')
    assert pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), 'vegetables')
    assert not pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), 'join_table')

    checkout(empty_pg_conn, OUTPUT, new_head)
    assert pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), 'fruits')
    assert pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), 'vegetables')
    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.join_table")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]

    # Now run the same sgfile but from the v2 of the remote (where row 1 has been removed from the fruits table)
    # First, remove the output mountpoint (the executor tries to fetch the commit 0000 from it otherwise which
    # doesn't exist).
    unmount(empty_pg_conn, OUTPUT)
    execute_commands(empty_pg_conn, _load_sgfile('from_remote.sgfile'), output=OUTPUT, params={'TAG': 'v2'})

    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.join_table")
        assert cur.fetchall() == [(2, 'orange', 'carrot')]


def test_from_remote_hash(empty_pg_conn, remote_driver_conn):
    head = get_current_head(remote_driver_conn, PG_MNT)
    # Test running commands that base new datasets on a remote repository.
    execute_commands(empty_pg_conn, _load_sgfile('from_remote.sgfile'), output=OUTPUT, params={'TAG': head[:10]})

    assert pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), 'fruits')
    assert pg_table_exists(empty_pg_conn, OUTPUT.to_schema(), 'vegetables')
    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.join_table")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]


def test_from_multistage(empty_pg_conn, remote_driver_conn):
    _add_multitag_dataset_to_remote_driver(remote_driver_conn)
    OUTPUT_STAGE_2 = R('output_stage_2')

    # Produces two repositories: output and output_stage_2
    execute_commands(empty_pg_conn, _load_sgfile('from_remote_multistage.sgfile'), params={'TAG': 'v1'})

    # Check the final output ('output_stage_2'): it should only have one single table object (a SNAP with the join_table
    # from the first stage, OUTPUT.
    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output_stage_2.balanced_diet")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]
    head = get_current_head(empty_pg_conn, OUTPUT_STAGE_2)
    # Check the commit is based on the original empty image.
    assert get_image(empty_pg_conn, OUTPUT_STAGE_2, head).parent_id == '0' * 64
    assert get_tables_at(empty_pg_conn, OUTPUT_STAGE_2, head) == ['balanced_diet']
    assert get_object_for_table(empty_pg_conn, OUTPUT_STAGE_2, 'balanced_diet', head, 'SNAP') is not None
    assert get_object_for_table(empty_pg_conn, OUTPUT_STAGE_2, 'balanced_diet', head, 'DIFF') is None


def test_from_local(sg_pg_mg_conn):
    execute_commands(sg_pg_mg_conn, _load_sgfile('from_local.sgfile'), output=OUTPUT)

    new_head = get_current_head(sg_pg_mg_conn, OUTPUT)
    # Go back to the parent: the two source tables should exist there
    checkout(sg_pg_mg_conn, OUTPUT, get_image(sg_pg_mg_conn, OUTPUT, new_head).parent_id)
    assert pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'fruits')
    assert pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'vegetables')
    assert not pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'join_table')

    checkout(sg_pg_mg_conn, OUTPUT, new_head)
    assert pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'fruits')
    assert pg_table_exists(sg_pg_mg_conn, OUTPUT.to_schema(), 'vegetables')
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.join_table")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]


def test_sgfile_with_external_sql(sg_pg_mg_conn, remote_driver_conn):
    # Tests are running from root so we pass in the path to the SQL manually to the sgfile.
    execute_commands(sg_pg_mg_conn, _load_sgfile('external_sql.sgfile'),
                     params={'EXTERNAL_SQL_FILE': SGFILE_ROOT + 'external_sql.sql'}, output=OUTPUT)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]
