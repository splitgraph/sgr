from datetime import datetime
from unittest import mock

from click.testing import CliRunner
from test.splitgraph.conftest import OUTPUT

from splitgraph.commandline import (
    status_c,
    sql_c,
    diff_c,
    commit_c,
    log_c,
    show_c,
    table_c,
    object_c,
    objects_c,
)
from splitgraph.core.metadata_manager import OBJECT_COLS
from splitgraph.core.repository import Repository
from splitgraph.core.sql import insert
from splitgraph.core.types import TableColumn


def test_commandline_basics(pg_repo_local):
    runner = CliRunner()
    old_head = pg_repo_local.head

    # sgr status
    result = runner.invoke(status_c, [])
    assert """test/pg_mount         2       0  """ in result.output
    assert pg_repo_local.head.image_hash[:10] in result.output

    # sgr status with LQ etc
    old_head.checkout(layered=True)
    pg_repo_local.upstream = Repository("some", "upstream", engine=pg_repo_local.engine)
    result = runner.invoke(status_c, [])
    assert old_head.image_hash[:10] + " (LQ)" in result.output
    assert "some/upstream (LOCAL)" in result.output

    # sgr sql
    pg_repo_local.head.checkout()
    runner.invoke(sql_c, ["INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')"])
    runner.invoke(
        sql_c, ['CREATE TABLE "test/pg_mount".mushrooms (mushroom_id integer, name varchar)']
    )
    runner.invoke(sql_c, ['DROP TABLE "test/pg_mount".vegetables'])
    runner.invoke(sql_c, ['DELETE FROM "test/pg_mount".fruits WHERE fruit_id = 1'])
    runner.invoke(sql_c, ["COMMENT ON COLUMN \"test/pg_mount\".fruits.name IS 'Name of the fruit'"])
    result = runner.invoke(sql_c, ['SELECT * FROM "test/pg_mount".fruits'])
    assert "mayonnaise" in result.output
    assert "apple" not in result.output

    result = runner.invoke(sql_c, ["--json", 'SELECT * FROM "test/pg_mount".fruits'])
    assert result.output == '[[2, "orange"], [3, "mayonnaise"]]\n'

    # Test schema search_path
    result = runner.invoke(sql_c, ["--schema", "test/pg_mount", "SELECT * FROM fruits"])
    assert "mayonnaise" in result.output
    assert "apple" not in result.output

    # Test sql using LQ against current HEAD (without the changes we've made)
    result = runner.invoke(sql_c, ["--image", "test/pg_mount:latest", "SELECT * FROM fruits"])
    assert result.exit_code == 0
    assert "apple" in result.output
    assert "orange" in result.output
    assert "mayonnaise" not in result.output

    def check_diff(args):
        result = runner.invoke(diff_c, [str(a) for a in args])
        assert "added 1 row" in result.output
        assert "removed 1 row" in result.output
        assert "vegetables: table removed"
        assert "mushrooms: table added"
        result = runner.invoke(diff_c, [str(a) for a in args] + ["-v"])
        assert "+ (3, 'mayonnaise')" in result.output
        assert "- (1, 'apple')" in result.output

    # sgr diff, HEAD -> current staging (0-param)
    check_diff([pg_repo_local])

    # sgr commit as a full snapshot
    # This is weird: at this point, the pgcrypto extension exists
    #   (from this same connection (pg_repo_local.engine.connection) doing CREATE EXTENSION causes an error) but
    #   calling digest() fails saying the function doesn't exist (ultimately `calculate_content_hash` fails, but also
    #   reproducible by setting a breakpoint here and doing
    #       pg_repo_local.engine.run_sql("SELECT digest('bla', 'sha256')")).
    #  * This happens unless a rollback() is issued (even if it's issued straight after a connection commit).
    #  * `pg_repo_local.engine.connection` is the same connection object as the one used by `calculate_content_hash`.
    #  * If there's a set_trace() here and the commit is done from the commandline in a different shell
    #    (after committing this connection), that commit succeeds but invoking the next line here fails anyway.
    #  * This happens if the commit is done using the API instead of the click invoker as well (doing
    #    pg_repo_local.commit(comment='Test commit', snap_only=True))
    #  * This is not because this is the first test in the splitgraph suite that uses the engine (running
    #    pytest -k test_commandline_commit_chunk works)
    #
    # It seems like something's wrong with this connection object. As a temporary (?) workaround, commit the connection
    # and issue a rollback here, which seems to fix things.

    pg_repo_local.commit_engines()
    pg_repo_local.engine.rollback()
    result = runner.invoke(commit_c, [str(pg_repo_local), "-m", "Test commit", "--snap"])
    assert result.exit_code == 0
    new_head = pg_repo_local.head
    assert new_head != old_head
    assert new_head.parent_id == old_head.image_hash
    assert new_head.image_hash[:10] in result.output

    # sgr diff, old head -> new head (2-param), truncated hashes
    # technically these two hashes have a 2^(-20*4) = a 8e-25 chance of clashing but let's not dwell on that
    check_diff([pg_repo_local, old_head.image_hash[:20], new_head.image_hash[:20]])

    # sgr diff, just the new head -- assumes the diff on top of the old head.
    check_diff([pg_repo_local, new_head.image_hash[:20]])

    # sgr diff, just the new head -- assumes the diff on top of the old head.
    check_diff([pg_repo_local, new_head.image_hash[:20]])

    # sgr diff, reverse order -- actually materializes two tables to compare them
    result = runner.invoke(
        diff_c, [str(pg_repo_local), new_head.image_hash[:20], old_head.image_hash[:20]]
    )
    assert "added 1 row" in result.output
    assert "removed 1 row" in result.output
    assert "vegetables: table removed"
    assert "mushrooms: table added"
    result = runner.invoke(
        diff_c, [str(pg_repo_local), new_head.image_hash[:20], old_head.image_hash[:20], "-v"]
    )
    # Since the images were flipped, here the result is that, since the row that was added
    # didn't exist in the first image, diff() thinks it was _removed_ and vice versa for the other row.
    assert "- (3, 'mayonnaise')" in result.output
    assert "+ (1, 'apple')" in result.output

    # sgr status with the new commit
    result = runner.invoke(status_c, [str(pg_repo_local)])
    assert "test/pg_mount" in result.output
    assert "Parent: " + old_head.image_hash in result.output
    assert new_head.image_hash in result.output

    # sgr log
    result = runner.invoke(log_c, [str(pg_repo_local)])
    assert old_head.image_hash[:10] in result.output
    assert new_head.image_hash[:10] in result.output
    assert "Test commit" in result.output

    # sgr log (tree)
    result = runner.invoke(log_c, [str(pg_repo_local), "-t"])
    assert old_head.image_hash[:5] in result.output
    assert new_head.image_hash[:5] in result.output

    # sgr show the new commit
    result = runner.invoke(show_c, [str(pg_repo_local) + ":" + new_head.image_hash[:20]])
    assert "Test commit" in result.output
    assert "Parent: " + old_head.image_hash in result.output
    size = new_head.get_size()
    assert ("Size: %d.00 B" % size) in result.output
    assert "fruits" in result.output
    assert "mushrooms" in result.output

    # sgr show the table's metadata
    assert new_head.get_table("fruits").get_size() == size
    result = runner.invoke(table_c, [str(pg_repo_local) + ":" + new_head.image_hash[:20], "fruits"])
    assert ("Size: %d.00 B" % size) in result.output
    assert "Rows: 2" in result.output
    assert "fruit_id (integer)" in result.output
    assert new_head.get_table("fruits").objects[0] in result.output
    assert "Name of the fruit" in result.output


def test_commandline_show_empty_image(local_engine_empty):
    # Check size calculations etc in an empty image don't cause errors.
    runner = CliRunner()
    OUTPUT.init()
    assert OUTPUT.images["latest"].get_size() == 0
    result = runner.invoke(show_c, [str(OUTPUT) + ":" + "000000000000"], catch_exceptions=False)
    assert "Size: 0.00 B" in result.output


def test_object_info(local_engine_empty, remote_engine_registry, unprivileged_remote_engine):
    runner = CliRunner()

    base_1 = "o" + "0" * 62
    patch_1 = "o" + "0" * 61 + "1"
    patch_2 = "o" + "0" * 61 + "2"
    dt = datetime(2019, 1, 1)

    q = insert("objects", OBJECT_COLS)
    base_1_meta = (
        base_1,
        "FRAG",
        "ns1",
        12345,
        dt,
        "0" * 64,
        "0" * 64,
        {
            "range": {"col_1": [10, 20]},
            "bloom": {"col_1": [7, "uSP6qzHHDqVq/qHMlqrAoHhpxEuZ08McrB0J6c9M"]},
        },
        1000,
        500,
    )
    local_engine_empty.run_sql(q, base_1_meta)
    # Add this object to the remote engine too to check that we can run
    # sgr object -r remote_engine o...
    remote_engine_registry.run_sql(q, base_1_meta)
    remote_engine_registry.commit()

    local_engine_empty.run_sql(
        q,
        (
            patch_1,
            "FRAG",
            "ns1",
            6789,
            dt,
            "0" * 64,
            "0" * 64,
            {"range": {"col_1": [10, 20]}},
            1000,
            500,
        ),
    )
    local_engine_empty.run_sql(
        q,
        (
            patch_2,
            "FRAG",
            "ns1",
            1011,
            dt,
            "0" * 64,
            "0" * 64,
            {"range": {"col_1": [10, 20], "col_2": ["bla", "ble"]}},
            420,
            0,
        ),
    )
    # base_1: external, cached locally
    local_engine_empty.run_sql(
        insert("object_locations", ("object_id", "protocol", "location")),
        (base_1, "HTTP", "example.com/objects/base_1.tgz"),
    )
    local_engine_empty.run_sql(insert("object_cache_status", ("object_id",)), (base_1,))
    local_engine_empty.mount_object(
        base_1, schema_spec=[TableColumn(1, "col_1", "integer", False, None)]
    )

    # patch_1: on the engine, uncached locally
    # patch_2: created here, cached locally
    local_engine_empty.mount_object(
        patch_2, schema_spec=[TableColumn(1, "col_1", "integer", False, None)]
    )

    result = runner.invoke(object_c, [base_1], catch_exceptions=False)
    assert result.exit_code == 0
    assert (
        result.output
        == f"""Object ID: {base_1}

Namespace: ns1
Format: FRAG
Size: 12.06 KiB
Created: 2019-01-01 00:00:00
Rows inserted: 1000
Insertion hash: {"0" * 64}
Rows deleted: 500
Deletion hash: {"0" * 64}
Column index:
  col_1: [10, 20]
Bloom index: 
  col_1: k=7, size 40.00 B, approx. 23 item(s), false positive probability 0.7%

Location: cached locally
Original location: example.com/objects/base_1.tgz (HTTP)
"""
    )

    result = runner.invoke(object_c, [base_1, "-r", unprivileged_remote_engine.name])
    assert result.exit_code == 0
    assert "Size: 12.06 KiB" in result.output
    # Since we're talking to a registry, don't try to find the object's location.
    assert "Location: " not in result.output

    result = runner.invoke(object_c, [patch_1])
    assert result.exit_code == 0
    assert "Location: remote engine" in result.output

    with mock.patch(
        "splitgraph.core.object_manager.ObjectManager.get_downloaded_objects",
        return_value=[base_1, patch_2],
    ):
        result = runner.invoke(object_c, [patch_2])
        assert result.exit_code == 0
        assert "Location: created locally" in result.output

        result = runner.invoke(objects_c)
        assert result.exit_code == 0
        assert result.output == "\n".join([base_1, patch_1, patch_2]) + "\n"

        result = runner.invoke(objects_c, ["--local"])
        assert result.exit_code == 0
        assert result.output == base_1 + "\n" + patch_2 + "\n"
