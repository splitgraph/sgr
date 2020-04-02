"""
Tests for various internal errors (e.g. missing objects) not breaking the executor
"""
import psycopg2
import pytest
from parsimonious import IncompleteParseError
from psycopg2.sql import SQL, Identifier
from test.splitgraph.conftest import OUTPUT, load_splitfile

from splitgraph.engine import ResultShape
from splitgraph.exceptions import ObjectCacheError
from splitgraph.splitfile._parsing import parse_commands
from splitgraph.splitfile.execution import execute_commands


def _get_table_count(repo):
    return repo.engine.run_sql(
        "SELECT COUNT(*) FROM splitgraph_meta.tables WHERE namespace = %s AND repository = %s",
        (repo.namespace, repo.repository),
        return_shape=ResultShape.ONE_ONE,
    )


def test_splitfile_object_download_failure(local_engine_empty, pg_repo_remote_multitag):
    # Simulate an object download failure (that happens inside of the engine during IMPORT
    # execution) propagating to the caller and not leaving the engine in an inconsistent state.

    object_id = pg_repo_remote_multitag.images["v1"].get_table("fruits").objects[0]
    assert object_id == "o0e742bd2ea4927f5193a2c68f8d4c51ea018b1ef3e3005a50727147d2cf57b"
    tmp_object_id = "o" + "0" * 62

    pg_repo_remote_multitag.engine.run_sql(
        SQL("ALTER TABLE splitgraph_meta.{} RENAME TO {}").format(
            Identifier(object_id), Identifier(tmp_object_id)
        )
    )

    assert len(OUTPUT.images()) == 0
    assert _get_table_count(OUTPUT) == 0

    with pytest.raises(ObjectCacheError) as e:
        execute_commands(
            load_splitfile("import_remote_multiple.splitfile"), params={"TAG": "v1"}, output=OUTPUT
        )
    assert "Missing 1 object (%s)" % object_id in str(e.value)

    # Check the execution didn't create the image
    assert len(OUTPUT.images()) == 0
    assert _get_table_count(OUTPUT) == 0

    # Rename the object back and retry the Splitfile
    pg_repo_remote_multitag.engine.run_sql(
        SQL("ALTER TABLE splitgraph_meta.{} RENAME TO {}").format(
            Identifier(tmp_object_id), Identifier(object_id)
        )
    )

    execute_commands(
        load_splitfile("import_remote_multiple.splitfile"), params={"TAG": "v1"}, output=OUTPUT
    )
    OUTPUT.head.checkout()
    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM join_table") == [
        (1, "apple", "potato"),
        (2, "orange", "carrot"),
    ]

    assert len(OUTPUT.images()) == 3
    # 2 tables in the first non-empty image, 3 tables in the second image
    # (previous 2 + joined data).
    assert _get_table_count(OUTPUT) == 5


def test_splitfile_sql_failure(local_engine_empty, pg_repo_remote_multitag):
    assert len(OUTPUT.images()) == 0
    assert _get_table_count(OUTPUT) == 0

    with pytest.raises(psycopg2.errors.UndefinedTable) as e:
        execute_commands(load_splitfile("import_remote_broken_stage_2.splitfile"), output=OUTPUT)
    assert 'relation "nonexistent_fruits_table" does not exist' in str(e.value)

    # Check the execution created the first dummy (000...) image and the second image
    # with IMPORT results
    assert len(OUTPUT.images()) == 2
    assert _get_table_count(OUTPUT) == 2
    assert sorted(OUTPUT.images["latest"].get_tables()) == ["my_fruits", "vegetables"]


def test_splitfile_various_parse_errors():
    # Check common mistypings of official commands don't get parsed as FROM + custom command
    with pytest.raises(IncompleteParseError):
        parse_commands("FROM some/repo:12345 IMPORT {SELECT * FROM table}")
