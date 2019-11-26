import pytest

from splitgraph.core.engine import get_current_repositories
from splitgraph.core.repository import clone, Repository
from splitgraph.exceptions import SplitfileError
from splitgraph.splitfile._parsing import preprocess
from splitgraph.splitfile.execution import execute_commands
from test.splitgraph.conftest import OUTPUT, SPLITFILE_ROOT, load_splitfile

PARSING_TEST_SPLITFILE = load_splitfile("import_remote_multiple.splitfile")
R = Repository.from_schema


def test_splitfile_preprocessor_missing_params():
    with pytest.raises(SplitfileError) as e:
        preprocess(PARSING_TEST_SPLITFILE, params={})
    assert "${TAG}" in str(e.value)
    assert "${ESCAPED}" not in str(e.value)


def test_splitfile_preprocessor_escaping():
    commands = preprocess(PARSING_TEST_SPLITFILE, params={"TAG": "tag-v1-whatever"})
    print(commands)
    assert "${TAG}" not in commands
    assert "\\${ESCAPED}" not in commands
    assert "${ESCAPED}" in commands
    assert "tag-v1-whatever" in commands


def test_basic_splitfile(pg_repo_local):
    execute_commands(load_splitfile("create_table.splitfile"), output=OUTPUT)
    log = list(reversed(OUTPUT.head.get_log()))

    log[1].checkout()
    assert OUTPUT.run_sql("SELECT * FROM my_fruits") == []

    log[2].checkout()
    assert OUTPUT.run_sql("SELECT * FROM my_fruits") == [(1, "pineapple")]

    log[3].checkout()
    assert OUTPUT.run_sql("SELECT * FROM my_fruits") == [(1, "pineapple"), (2, "banana")]


def test_update_without_import_splitfile(pg_repo_local):
    # Test that correct commits are produced by executing an splitfile (both against newly created and already
    # existing tables on an existing mountpoint)
    execute_commands(load_splitfile("update_without_import.splitfile"), output=OUTPUT)
    log = OUTPUT.head.get_log()

    log[1].checkout()
    assert OUTPUT.run_sql("SELECT * FROM my_fruits") == []

    log[0].checkout()
    assert OUTPUT.run_sql("SELECT * FROM my_fruits") == [(1, "pineapple")]


def test_local_import_splitfile(pg_repo_local):
    execute_commands(load_splitfile("import_local.splitfile"), output=OUTPUT)
    head = OUTPUT.head
    old_head = head.parent_id

    OUTPUT.images.by_hash(old_head).checkout()
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), "my_fruits")
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), "fruits")

    head.checkout()
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "my_fruits")
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), "fruits")


def test_advanced_splitfile(pg_repo_local):
    execute_commands(load_splitfile("import_local_multiple_with_queries.splitfile"), output=OUTPUT)

    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "my_fruits")
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "vegetables")
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), "fruits")
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "join_table")

    head = OUTPUT.head
    old_head = head.parent_id
    OUTPUT.images.by_hash(old_head).checkout()
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), "join_table")
    head.checkout()
    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM join_table") == [
        (2, "orange", "carrot")
    ]
    assert OUTPUT.run_sql("SELECT * FROM my_fruits") == [(2, "orange")]


def test_splitfile_cached(pg_repo_local):
    # Check that no new commits/snaps are created if we rerun the same splitfile
    execute_commands(load_splitfile("import_local_multiple_with_queries.splitfile"), output=OUTPUT)
    images = OUTPUT.images()
    assert len(images) == 4

    execute_commands(load_splitfile("import_local_multiple_with_queries.splitfile"), output=OUTPUT)
    new_images = OUTPUT.images()
    assert new_images == images


def test_splitfile_remote(local_engine_empty, pg_repo_remote_multitag):
    # We use the v1 tag when importing from the remote, so fruit_id = 1 still exists there.
    execute_commands(
        load_splitfile("import_remote_multiple.splitfile"), params={"TAG": "v1"}, output=OUTPUT
    )
    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM join_table") == [
        (1, "apple", "potato"),
        (2, "orange", "carrot"),
    ]

    # Now run the commands against v2 and make sure the fruit_id = 1 has disappeared from the output.
    execute_commands(
        load_splitfile("import_remote_multiple.splitfile"), params={"TAG": "v2"}, output=OUTPUT
    )
    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM join_table") == [
        (2, "orange", "carrot")
    ]


def test_splitfile_remote_hash(local_engine_empty, pg_repo_remote):
    head = pg_repo_remote.head.image_hash
    execute_commands(
        load_splitfile("import_remote_multiple.splitfile"), params={"TAG": head[:10]}, output=OUTPUT
    )
    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM output.join_table") == [
        (1, "apple", "potato"),
        (2, "orange", "carrot"),
    ]


def test_import_updating_splitfile_with_uploading(
    local_engine_empty, remote_engine, pg_repo_remote
):
    execute_commands(load_splitfile("import_and_update.splitfile"), output=OUTPUT)
    head = OUTPUT.head

    assert len(OUTPUT.objects.get_all_objects()) == 4  # Two original tables + two updates

    # Push with upload. Have to specify the remote repo.
    remote_output = Repository(OUTPUT.namespace, OUTPUT.repository, remote_engine)
    OUTPUT.push(remote_output, handler="S3", handler_options={})
    # Unmount everything locally and cleanup
    OUTPUT.delete()

    # OUTPUT doesn't exist but we use its ObjectManager reference to access the global object
    # manager for the engine (maybe should inject it into local_engine/remote_engine instead)
    OUTPUT.objects.cleanup()
    assert not OUTPUT.objects.get_all_objects()

    clone(OUTPUT.to_schema(), download_all=False)

    assert not OUTPUT.objects.get_downloaded_objects()
    existing_objects = list(OUTPUT.objects.get_all_objects())
    assert len(existing_objects) == 4  # Two original tables + two updates
    # Only 2 objects are stored externally (the other two have been on the remote the whole time)
    assert len(OUTPUT.objects.get_external_object_locations(existing_objects)) == 2

    head.checkout()
    assert OUTPUT.run_sql("SELECT fruit_id, name FROM my_fruits") == [
        (1, "apple"),
        (2, "orange"),
        (3, "mayonnaise"),
    ]


@pytest.mark.mounting
def test_splitfile_end_to_end_with_uploading(
    local_engine_empty, remote_engine, pg_repo_remote_multitag, mg_repo_remote
):
    # An end-to-end test:
    #   * Create a derived dataset from some tables imported from the remote engine
    #   * Push it back to the remote engine, uploading all objects to S3 (instead of the remote engine itself)
    #   * Delete everything from pgcache
    #   * Run another splitfile that depends on the just-pushed dataset (and does lazy checkouts to
    #     get the required tables).

    # Do the same setting up first and run the splitfile against the remote data.
    execute_commands(
        load_splitfile("import_remote_multiple.splitfile"), params={"TAG": "v1"}, output=OUTPUT
    )

    remote_output = Repository(OUTPUT.namespace, OUTPUT.repository, remote_engine)

    # Push with upload
    OUTPUT.push(remote_repository=remote_output, handler="S3", handler_options={})
    # Unmount everything locally and cleanup
    for mountpoint, _ in get_current_repositories(local_engine_empty):
        mountpoint.delete()
    OUTPUT.objects.cleanup()

    stage_2 = R("output_stage_2")
    execute_commands(load_splitfile("import_from_preuploaded_remote.splitfile"), output=stage_2)

    assert stage_2.run_sql("SELECT id, name, fruit, vegetable FROM diet") == [
        (2, "James", "orange", "carrot")
    ]


@pytest.mark.mounting
def test_splitfile_schema_changes(pg_repo_local, mg_repo_local):
    execute_commands(load_splitfile("schema_changes.splitfile"), output=OUTPUT)
    old_output_head = OUTPUT.head

    # Then, alter the dataset and rerun the splitfile.
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (12, 'mayonnaise')")
    pg_repo_local.commit()
    execute_commands(load_splitfile("schema_changes.splitfile"), output=OUTPUT)
    new_output_head = OUTPUT.head

    old_output_head.checkout()
    assert OUTPUT.run_sql("SELECT * FROM spirit_fruits") == [("James", "orange", 12)]

    new_output_head.checkout()
    # Mayonnaise joined with Alex, ID 12 + 10 = 22.
    assert OUTPUT.run_sql("SELECT * FROM spirit_fruits") == [
        ("James", "orange", 12),
        ("Alex", "mayonnaise", 22),
    ]


def test_import_with_custom_query(pg_repo_local):
    # Test that importing with a custom query creates a new object
    pg_repo_local.run_sql(
        "INSERT INTO fruits VALUES (3, 'mayonnaise');"
        "INSERT INTO vegetables VALUES (3, 'oregano')"
    )
    pg_repo_local.commit()

    all_current_objects = pg_repo_local.objects.get_all_objects()

    execute_commands(load_splitfile("import_with_custom_query.splitfile"), output=OUTPUT)
    head = OUTPUT.head
    old_head = OUTPUT.images.by_hash(head.parent_id)

    # First two tables imported as new objects since they had a custom query, the other two get pointed
    # to the old pg_repo_local objects.
    tables = ["my_fruits", "o_vegetables", "vegetables", "all_fruits"]
    contents = [
        [(2, "orange")],
        [(1, "potato"), (3, "oregano")],
        [(1, "potato"), (2, "carrot"), (3, "oregano")],
        [(1, "apple"), (2, "orange"), (3, "mayonnaise")],
    ]

    old_head.checkout()
    engine = OUTPUT.engine
    for t in tables:
        assert not engine.table_exists(OUTPUT.to_schema(), t)

    head.checkout()
    for t, c in zip(tables, contents):
        assert sorted(OUTPUT.run_sql("SELECT * FROM %s" % t)) == sorted(c)

    for t in tables:
        objects = head.get_table(t).objects
        if t in ["my_fruits", "o_vegetables"]:
            assert all(o not in all_current_objects for o in objects)
        else:
            assert all(o in all_current_objects for o in objects)


@pytest.mark.mounting
def test_import_mount(local_engine_empty):
    execute_commands(load_splitfile("import_from_mounted_db.splitfile"), output=OUTPUT)

    head = OUTPUT.head
    old_head = OUTPUT.images.by_hash(head.parent_id)

    old_head.checkout()
    tables = ["my_fruits", "o_vegetables", "vegetables", "all_fruits"]
    contents = [
        [(2, "orange")],
        [(1, "potato")],
        [(1, "potato"), (2, "carrot")],
        [(1, "apple"), (2, "orange")],
    ]
    for t in tables:
        assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), t)

    head.checkout()
    for t, c in zip(tables, contents):
        assert OUTPUT.run_sql("SELECT * FROM %s" % t) == c


@pytest.mark.mounting
def test_import_all(local_engine_empty):
    execute_commands(load_splitfile("import_all_from_mounted.splitfile"), output=OUTPUT)

    head = OUTPUT.head
    old_head = OUTPUT.images.by_hash(head.parent_id)

    old_head.checkout()
    tables = ["vegetables", "fruits"]
    contents = [[(1, "potato"), (2, "carrot")], [(1, "apple"), (2, "orange")]]
    for t in tables:
        assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), t)

    head.checkout()
    for t, c in zip(tables, contents):
        assert OUTPUT.run_sql("SELECT * FROM %s" % t) == c


def test_from_remote(local_engine_empty, pg_repo_remote_multitag):
    # Test running commands that base new datasets on a remote repository.
    execute_commands(load_splitfile("from_remote.splitfile"), params={"TAG": "v1"}, output=OUTPUT)

    new_head = OUTPUT.head
    parent = OUTPUT.images.by_hash(new_head.parent_id)
    # Go back to the parent: the two source tables should exist there
    parent.checkout()
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "fruits")
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "vegetables")
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), "join_table")

    new_head.checkout()
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "fruits")
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "vegetables")
    assert OUTPUT.run_sql("SELECT * FROM join_table") == [
        (1, "apple", "potato"),
        (2, "orange", "carrot"),
    ]

    # Now run the same splitfile but from the v2 of the remote (where row 1 has been removed from the fruits table)
    # First, remove the output mountpoint (the executor tries to fetch the commit 0000 from it otherwise which
    # doesn't exist).
    OUTPUT.delete()
    execute_commands(load_splitfile("from_remote.splitfile"), params={"TAG": "v2"}, output=OUTPUT)

    assert OUTPUT.run_sql("SELECT * FROM join_table") == [(2, "orange", "carrot")]


def test_from_remote_hash(local_engine_empty, pg_repo_remote):
    head = pg_repo_remote.head.image_hash
    # Test running commands that base new datasets on a remote repository.
    execute_commands(
        load_splitfile("from_remote.splitfile"), params={"TAG": head[:10]}, output=OUTPUT
    )

    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "fruits")
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "vegetables")
    assert OUTPUT.run_sql("SELECT * FROM join_table") == [
        (1, "apple", "potato"),
        (2, "orange", "carrot"),
    ]


def test_from_multistage(local_engine_empty, pg_repo_remote_multitag):
    stage_2 = R("output_stage_2")

    # Produces two repositories: output and output_stage_2
    execute_commands(load_splitfile("from_remote_multistage.splitfile"), params={"TAG": "v1"})

    # Check the final output ('output_stage_2'): it should only have one single fragment (join_table
    # from the first stage, OUTPUT.
    assert stage_2.run_sql("SELECT * FROM balanced_diet") == [
        (1, "apple", "potato"),
        (2, "orange", "carrot"),
    ]
    # Check the commit is based on the original empty image.
    assert stage_2.head.parent_id == "0" * 64
    assert stage_2.head.get_tables() == ["balanced_diet"]


def test_from_local(pg_repo_local):
    execute_commands(load_splitfile("from_local.splitfile"), output=OUTPUT)

    new_head = OUTPUT.head
    # Go back to the parent: the two source tables should exist there
    OUTPUT.images.by_hash(new_head.parent_id).checkout()
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "fruits")
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "vegetables")
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), "join_table")

    new_head.checkout()
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "fruits")
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "vegetables")
    assert OUTPUT.run_sql("SELECT * FROM join_table") == [
        (1, "apple", "potato"),
        (2, "orange", "carrot"),
    ]


@pytest.mark.registry
def test_splitfile_with_external_sql(readonly_pg_repo):

    # Tests are running from root so we pass in the path to the SQL manually to the splitfile.
    execute_commands(
        load_splitfile("external_sql.splitfile"),
        params={"EXTERNAL_SQL_FILE": SPLITFILE_ROOT + "external_sql.sql"},
        output=OUTPUT,
    )

    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM join_table") == [
        (1, "apple", "potato"),
        (2, "orange", "carrot"),
    ]
