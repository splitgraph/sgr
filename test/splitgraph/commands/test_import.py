import pytest

from splitgraph.core.engine import get_current_repositories
from splitgraph.core.repository import import_table_from_remote
from splitgraph.engine import get_engine
from test.splitgraph.conftest import OUTPUT


def _setup_dataset():
    OUTPUT.init()
    OUTPUT.run_sql(
        """CREATE TABLE test (id integer, name varchar);
        INSERT INTO test VALUES (1, 'test')"""
    )
    OUTPUT.commit()
    OUTPUT.run_sql("INSERT INTO test VALUES (2, 'test2')")
    return OUTPUT.commit()


def test_import_basic(pg_repo_local):
    # Create a new schema and import 'fruits' from the mounted PG table.
    OUTPUT.init()
    head = OUTPUT.head

    OUTPUT.import_tables(
        tables=["imported_fruits"], source_repository=pg_repo_local, source_tables=["fruits"]
    )

    assert OUTPUT.run_sql("SELECT * FROM imported_fruits") == pg_repo_local.run_sql(
        "SELECT * FROM fruits"
    )
    new_head = OUTPUT.head

    assert new_head != head
    assert new_head.parent_id == head.image_hash


def test_import_preserves_existing_tables(pg_repo_local):
    # Create a new schema and import 'fruits' from the mounted PG table.
    head = _setup_dataset()
    OUTPUT.import_tables(
        tables=["imported_fruits"], source_repository=pg_repo_local, source_tables=["fruits"]
    )
    new_head = OUTPUT.head

    head.checkout()
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "test")
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), "imported_fruits")

    new_head.checkout()
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "test")
    assert OUTPUT.engine.table_exists(OUTPUT.to_schema(), "imported_fruits")


def test_import_preserves_pending_changes(pg_repo_local):
    OUTPUT.init()
    OUTPUT.run_sql(
        """CREATE TABLE test (id integer, name varchar);
            INSERT INTO test VALUES (1, 'test')"""
    )
    head = OUTPUT.commit()
    OUTPUT.run_sql("INSERT INTO test VALUES (2, 'test2')")
    changes = get_engine().get_pending_changes(OUTPUT.to_schema(), "test")

    OUTPUT.import_tables(
        tables=["imported_fruits"], source_repository=pg_repo_local, source_tables=["fruits"]
    )

    assert OUTPUT.head.parent_id == head.image_hash
    assert changes == OUTPUT.engine.get_pending_changes(OUTPUT.to_schema(), "test")


def test_import_multiple_tables(pg_repo_local):
    OUTPUT.init()
    head = OUTPUT.head
    OUTPUT.import_tables(tables=[], source_repository=pg_repo_local, source_tables=[])

    for table_name in ["fruits", "vegetables"]:
        assert OUTPUT.run_sql("SELECT * FROM %s" % table_name) == pg_repo_local.run_sql(
            "SELECT * FROM %s" % table_name
        )

    new_head = OUTPUT.head
    assert new_head != head
    assert new_head.parent_id == head.image_hash


@pytest.mark.registry
def test_import_from_remote(local_engine_empty, unprivileged_pg_repo):
    # Start with a clean repo -- add a table to output to see if it's preserved.
    head = _setup_dataset()

    local_objects = OUTPUT.objects

    assert len(local_objects.get_downloaded_objects()) == 2
    assert len(local_objects.get_all_objects()) == 2
    assert local_engine_empty.get_all_tables(OUTPUT.to_schema()) == ["test"]

    # Import the 'fruits' table from the origin.
    remote_head = unprivileged_pg_repo.images["latest"]
    import_table_from_remote(
        unprivileged_pg_repo, ["fruits"], remote_head.image_hash, OUTPUT, target_tables=[]
    )
    new_head = OUTPUT.head

    # Check that the table now exists in the output, is committed and there's no trace of the cloned repo.
    # Also clean up the unused objects to make sure that the newly cloned table is still recorded.
    assert sorted(local_engine_empty.get_all_tables(OUTPUT.to_schema())) == ["fruits", "test"]
    local_objects.cleanup()
    assert len(get_current_repositories(local_engine_empty)) == 1
    head.checkout()
    assert local_engine_empty.table_exists(OUTPUT.to_schema(), "test")
    assert not local_engine_empty.table_exists(OUTPUT.to_schema(), "fruits")

    new_head.checkout()
    assert local_engine_empty.table_exists(OUTPUT.to_schema(), "test")
    assert local_engine_empty.table_exists(OUTPUT.to_schema(), "fruits")

    assert OUTPUT.run_sql("SELECT * FROM fruits") == [(1, "apple"), (2, "orange")]


@pytest.mark.registry
def test_import_and_update(local_engine_empty, unprivileged_pg_repo):
    OUTPUT.init()
    head = OUTPUT.head
    remote_head = unprivileged_pg_repo.images["latest"]
    # Import the 'fruits' table from the origin.
    import_table_from_remote(
        unprivileged_pg_repo, ["fruits"], remote_head.image_hash, OUTPUT, target_tables=[]
    )
    new_head = OUTPUT.head

    OUTPUT.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    new_head_2 = OUTPUT.commit()

    head.checkout()
    assert not OUTPUT.engine.table_exists(OUTPUT.to_schema(), "fruits")

    new_head.checkout()
    assert OUTPUT.run_sql("SELECT * FROM fruits") == [(1, "apple"), (2, "orange")]

    new_head_2.checkout()
    assert OUTPUT.run_sql("SELECT * FROM fruits") == [
        (1, "apple"),
        (2, "orange"),
        (3, "mayonnaise"),
    ]


def test_import_bare(pg_repo_local):
    # Check import without checking anything out, just by manipulating metadata and running LQs against
    # source images.

    # Create a new schema and import 'fruits'
    OUTPUT.init()
    # Make sure the existing table is preserved.
    OUTPUT.run_sql("CREATE TABLE sentinel (key INTEGER)")
    OUTPUT.commit()
    pg_repo_local.uncheckout()
    OUTPUT.uncheckout()

    OUTPUT.import_tables(
        tables=["imported_fruits"],
        source_repository=pg_repo_local,
        image_hash=pg_repo_local.images["latest"].image_hash,
        source_tables=["SELECT * FROM fruits WHERE fruit_id = 1"],
        parent_hash=OUTPUT.images["latest"].image_hash,
        do_checkout=False,
        table_queries=[True],
    )

    assert OUTPUT.head is None
    assert pg_repo_local.head is None

    assert sorted(OUTPUT.images["latest"].get_tables()) == ["imported_fruits", "sentinel"]
    assert list(
        OUTPUT.images["latest"].get_table("imported_fruits").query(columns=["name"], quals=[])
    ) == [{"name": "apple"}]
