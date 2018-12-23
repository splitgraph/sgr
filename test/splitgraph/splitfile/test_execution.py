import pytest

from splitgraph._data.images import get_all_image_info
from splitgraph._data.objects import get_existing_objects, get_downloaded_objects, get_external_object_locations, \
    get_object_for_table
from splitgraph.core.engine import cleanup_objects, get_current_repositories
from splitgraph.core.repository import to_repository as R, clone
from splitgraph.exceptions import SplitGraphException
from splitgraph.splitfile._parsing import preprocess
from splitgraph.splitfile.execution import execute_commands
from test.splitgraph.conftest import OUTPUT, SPLITFILE_ROOT, load_splitfile, REMOTE_ENGINE

PARSING_TEST_SPLITFILE = load_splitfile('import_remote_multiple.splitfile')


def test_splitfile_preprocessor_missing_params():
    with pytest.raises(SplitGraphException) as e:
        preprocess(PARSING_TEST_SPLITFILE, params={})
    assert '${TAG}' in str(e.value)
    assert '${ESCAPED}' not in str(e.value)


def test_splitfile_preprocessor_escaping():
    commands = preprocess(PARSING_TEST_SPLITFILE,
                          params={'TAG': 'tag-v1-whatever'})
    print(commands)
    assert '${TAG}' not in commands
    assert '\\${ESCAPED}' not in commands
    assert '${ESCAPED}' in commands
    assert 'tag-v1-whatever' in commands


def test_basic_splitfile(pg_repo_local, mg_repo_local):
    execute_commands(load_splitfile('create_table.splitfile'), output=OUTPUT)
    log = list(reversed(OUTPUT.get_image(OUTPUT.get_head()).get_log()))

    OUTPUT.checkout(log[1])
    assert OUTPUT.run_sql("SELECT * FROM my_fruits") == []

    OUTPUT.checkout(log[2])
    assert OUTPUT.run_sql("SELECT * FROM my_fruits") == [(1, 'pineapple')]

    OUTPUT.checkout(log[3])
    assert OUTPUT.run_sql("SELECT * FROM my_fruits") == [(1, 'pineapple'), (2, 'banana')]


def test_update_without_import_splitfile(pg_repo_local, mg_repo_local):
    # Test that correct commits are produced by executing an splitfile (both against newly created and already
    # existing tables on an existing mountpoint)
    execute_commands(load_splitfile('update_without_import.splitfile'), output=OUTPUT)
    log = OUTPUT.get_image(OUTPUT.get_head()).get_log()

    OUTPUT.checkout(log[1])
    assert OUTPUT.run_sql("SELECT * FROM my_fruits") == []

    OUTPUT.checkout(log[0])
    assert OUTPUT.run_sql("SELECT * FROM my_fruits") == [(1, 'pineapple')]


def test_local_import_splitfile(pg_repo_local, mg_repo_local):
    execute_commands(load_splitfile('import_local.splitfile'), output=OUTPUT)
    head = OUTPUT.get_head()
    old_head = OUTPUT.get_image(head).parent_id

    OUTPUT.checkout(old_head)
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'my_fruits')
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'fruits')

    OUTPUT.checkout(head)
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'my_fruits')
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'fruits')


def test_advanced_splitfile(pg_repo_local, mg_repo_local):
    execute_commands(load_splitfile('import_local_multiple_with_queries.splitfile'), output=OUTPUT)
    head = OUTPUT.get_head()

    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'my_fruits')
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'vegetables')
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'fruits')
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'join_table')

    old_head = OUTPUT.get_image(head).parent_id
    OUTPUT.checkout(old_head)
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'join_table')
    OUTPUT.checkout(head)
    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM join_table") == [(2, 'orange', 'carrot')]
    assert OUTPUT.run_sql("SELECT * FROM my_fruits") == [(2, 'orange')]


def test_splitfile_cached(pg_repo_local, mg_repo_local):
    # Check that no new commits/snaps are created if we rerun the same splitfile
    execute_commands(load_splitfile('import_local_multiple_with_queries.splitfile'), output=OUTPUT)
    images = get_all_image_info(OUTPUT)
    assert len(images) == 4

    execute_commands(load_splitfile('import_local_multiple_with_queries.splitfile'), output=OUTPUT)
    new_images = get_all_image_info(OUTPUT)
    assert new_images == images


def test_splitfile_remote(local_engine_empty, pg_repo_remote_multitag):
    # We use the v1 tag when importing from the remote, so fruit_id = 1 still exists there.
    execute_commands(load_splitfile('import_remote_multiple.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM join_table") == \
           [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]

    # Now run the commands against v2 and make sure the fruit_id = 1 has disappeared from the output.
    execute_commands(load_splitfile('import_remote_multiple.splitfile'), params={'TAG': 'v2'}, output=OUTPUT)
    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM join_table") == [(2, 'orange', 'carrot')]


def test_splitfile_remote_hash(local_engine_empty, pg_repo_remote):
    head = pg_repo_remote.get_head()
    execute_commands(load_splitfile('import_remote_multiple.splitfile'), params={'TAG': head[:10]}, output=OUTPUT)
    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM output.join_table") == \
           [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]


def test_import_updating_splitfile_with_uploading(local_engine_empty, pg_repo_remote):
    execute_commands(load_splitfile('import_and_update.splitfile'), output=OUTPUT)
    head = OUTPUT.get_head()

    assert len(get_existing_objects()) == 4  # Two original tables + two updates

    # Push with upload. Have to specify the remote engine since we are pushing a new repository.
    OUTPUT.push(remote_engine=REMOTE_ENGINE, handler='S3', handler_options={})
    # Unmount everything locally and cleanup
    OUTPUT.rm()
    cleanup_objects()
    assert not get_existing_objects()

    clone(OUTPUT, download_all=False)

    assert not get_downloaded_objects()
    existing_objects = list(get_existing_objects())
    assert len(existing_objects) == 4  # Two original tables + two updates
    # Only 2 objects are stored externally (the other two have been on the remote the whole time)
    assert len(get_external_object_locations(existing_objects)) == 2

    OUTPUT.checkout(head)
    assert OUTPUT.run_sql("SELECT fruit_id, name FROM my_fruits") == [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]


def test_splitfile_end_to_end_with_uploading(local_engine_empty, pg_repo_remote_multitag, mg_repo_remote):
    # An end-to-end test:
    #   * Create a derived dataset from some tables imported from the remote engine
    #   * Push it back to the remote engine, uploading all objects to S3 (instead of the remote engine itself)
    #   * Delete everything from pgcache
    #   * Run another splitfile that depends on the just-pushed dataset (and does lazy checkouts to
    #     get the required tables).

    # Do the same setting up first and run the splitfile against the remote data.
    execute_commands(load_splitfile('import_remote_multiple.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)

    # Push with upload
    OUTPUT.push(remote_engine='remote_engine', handler='S3', handler_options={})
    # Unmount everything locally and cleanup
    for mountpoint, _ in get_current_repositories():
        mountpoint.rm()
    cleanup_objects()

    stage_2 = R('output_stage_2')
    execute_commands(load_splitfile('import_from_preuploaded_remote.splitfile'), output=stage_2)

    assert stage_2.run_sql("SELECT id, name, fruit, vegetable FROM diet") == [(2, 'James', 'orange', 'carrot')]


def test_splitfile_schema_changes(pg_repo_local, mg_repo_local):
    execute_commands(load_splitfile('schema_changes.splitfile'), output=OUTPUT)
    old_output_head = OUTPUT.get_head()

    # Then, alter the dataset and rerun the splitfile.
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (12, 'mayonnaise')")
    pg_repo_local.commit()
    execute_commands(load_splitfile('schema_changes.splitfile'), output=OUTPUT)
    new_output_head = OUTPUT.get_head()

    OUTPUT.checkout(old_output_head)
    assert OUTPUT.run_sql("SELECT * FROM spirit_fruits") == [('James', 'orange', 12)]

    OUTPUT.checkout(new_output_head)
    # Mayonnaise joined with Alex, ID 12 + 10 = 22.
    assert OUTPUT.run_sql("SELECT * FROM spirit_fruits") == [('James', 'orange', 12), ('Alex', 'mayonnaise', 22)]


def test_import_with_custom_query(pg_repo_local, mg_repo_local):
    # Mostly a lazy way for the test to distinguish between the importer storing the results of a query as a snap
    # and pointing the commit history to the diff for unchanged objects.
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise');"
                          "INSERT INTO vegetables VALUES (3, 'oregano')")
    pg_repo_local.commit()

    execute_commands(load_splitfile('import_with_custom_query.splitfile'), output=OUTPUT)
    head = OUTPUT.get_head()
    old_head = OUTPUT.get_image(head).parent_id

    # First two tables imported as snaps since they had a custom query, the other two are diffs (basically a commit
    # pointing to the same object as test/pg_mount has).
    tables = ['my_fruits', 'o_vegetables', 'vegetables', 'all_fruits']
    contents = [[(2, 'orange')], [(1, 'potato'), (3, 'oregano')],
                [(1, 'potato'), (2, 'carrot'), (3, 'oregano')], [(1, 'apple'), (2, 'orange'), (3, 'mayonnaise')]]

    OUTPUT.checkout(old_head)
    engine = OUTPUT.engine
    for t in tables:
        assert not engine.table_exists(OUTPUT.to_schema(), t)

    OUTPUT.checkout(head)
    for t, c in zip(tables, contents):
        assert OUTPUT.run_sql("SELECT * FROM %s" % t) == c

    for t in tables:
        # Tables with queries stored as snaps, actually imported tables share objects with test/pg_mount.
        if t in ['my_fruits', 'o_vegetables']:
            assert get_object_for_table(OUTPUT, t, head, 'SNAP') is not None
            assert get_object_for_table(OUTPUT, t, head, 'DIFF') is None
        else:
            assert get_object_for_table(OUTPUT, t, head, 'SNAP') is None
            assert get_object_for_table(OUTPUT, t, head, 'DIFF') is not None


def test_import_mount(local_engine_empty):
    execute_commands(load_splitfile('import_from_mounted_db.splitfile'), output=OUTPUT)

    head = OUTPUT.get_head()
    old_head = OUTPUT.get_image(head).parent_id

    OUTPUT.checkout(old_head)
    tables = ['my_fruits', 'o_vegetables', 'vegetables', 'all_fruits']
    contents = [[(2, 'orange')], [(1, 'potato')], [(1, 'potato'), (2, 'carrot')], [(1, 'apple'), (2, 'orange')]]
    for t in tables:
        assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), t)

    OUTPUT.checkout(head)
    for t, c in zip(tables, contents):
        assert OUTPUT.run_sql("SELECT * FROM %s" % t) == c

    # All imported tables stored as snapshots
    for t in tables:
        assert get_object_for_table(OUTPUT, t, head, 'SNAP') is not None
        assert get_object_for_table(OUTPUT, t, head, 'DIFF') is None


def test_import_all(local_engine_empty):
    execute_commands(load_splitfile('import_all_from_mounted.splitfile'), output=OUTPUT)

    head = OUTPUT.get_head()
    old_head = OUTPUT.get_image(head).parent_id

    OUTPUT.checkout(old_head)
    tables = ['vegetables', 'fruits']
    contents = [[(1, 'potato'), (2, 'carrot')], [(1, 'apple'), (2, 'orange')]]
    for t in tables:
        assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), t)

    OUTPUT.checkout(head)
    for t, c in zip(tables, contents):
        assert OUTPUT.run_sql("SELECT * FROM %s" % t) == c


def test_from_remote(local_engine_empty, pg_repo_remote_multitag):
    # Test running commands that base new datasets on a remote repository.
    execute_commands(load_splitfile('from_remote.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)

    new_head = OUTPUT.get_head()
    # Go back to the parent: the two source tables should exist there
    OUTPUT.checkout(OUTPUT.get_image(new_head).parent_id)
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'fruits')
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'vegetables')
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'join_table')

    OUTPUT.checkout(new_head)
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'fruits')
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'vegetables')
    assert OUTPUT.run_sql("SELECT * FROM join_table") == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]

    # Now run the same splitfile but from the v2 of the remote (where row 1 has been removed from the fruits table)
    # First, remove the output mountpoint (the executor tries to fetch the commit 0000 from it otherwise which
    # doesn't exist).
    OUTPUT.rm()
    execute_commands(load_splitfile('from_remote.splitfile'), params={'TAG': 'v2'}, output=OUTPUT)

    assert OUTPUT.run_sql("SELECT * FROM join_table") == [(2, 'orange', 'carrot')]


def test_from_remote_hash(local_engine_empty, pg_repo_remote):
    head = pg_repo_remote.get_head()
    # Test running commands that base new datasets on a remote repository.
    execute_commands(load_splitfile('from_remote.splitfile'), params={'TAG': head[:10]}, output=OUTPUT)

    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'fruits')
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'vegetables')
    assert OUTPUT.run_sql("SELECT * FROM join_table") == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]


def test_from_multistage(local_engine_empty, pg_repo_remote_multitag):
    stage_2 = R('output_stage_2')

    # Produces two repositories: output and output_stage_2
    execute_commands(load_splitfile('from_remote_multistage.splitfile'), params={'TAG': 'v1'})

    # Check the final output ('output_stage_2'): it should only have one single table object (a SNAP with the join_table
    # from the first stage, OUTPUT.
    assert stage_2.run_sql("SELECT * FROM balanced_diet") == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]
    head = stage_2.get_head()
    # Check the commit is based on the original empty image.
    assert stage_2.get_image(head).parent_id == '0' * 64
    assert stage_2.get_image(head).get_tables() == ['balanced_diet']
    assert get_object_for_table(stage_2, 'balanced_diet', head, 'SNAP') is not None
    assert get_object_for_table(stage_2, 'balanced_diet', head, 'DIFF') is None


def test_from_local(pg_repo_local):
    execute_commands(load_splitfile('from_local.splitfile'), output=OUTPUT)

    new_head = OUTPUT.get_head()
    # Go back to the parent: the two source tables should exist there
    OUTPUT.checkout(OUTPUT.get_image(new_head).parent_id)
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'fruits')
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'vegetables')
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'join_table')

    OUTPUT.checkout(new_head)
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'fruits')
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), 'vegetables')
    assert OUTPUT.run_sql("SELECT * FROM join_table") == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]


def test_splitfile_with_external_sql(pg_repo_local, pg_repo_remote, mg_repo_local):
    # Tests are running from root so we pass in the path to the SQL manually to the splitfile.
    execute_commands(load_splitfile('external_sql.splitfile'),
                     params={'EXTERNAL_SQL_FILE': SPLITFILE_ROOT + 'external_sql.sql'}, output=OUTPUT)
    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM join_table") == \
           [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]
