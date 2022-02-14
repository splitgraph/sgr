from click.testing import CliRunner

from splitgraph.commandline import checkout_c, commit_c, sql_c, tag_c
from splitgraph.engine.config import get_engine


def test_commandline_commit_chunk(pg_repo_local):
    runner = CliRunner()

    # Make sure to set PKs for tables, otherwise chunking will fail.
    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    pg_repo_local.run_sql("ALTER TABLE vegetables ADD PRIMARY KEY (vegetable_id)")

    result = runner.invoke(
        commit_c,
        [
            str(pg_repo_local),
            "--snap",
            "--chunk-size=1",
            "--chunk-sort-keys",
            '{"fruits": ["fruit_id", "name"], "vegetables": ["vegetable_id", "name"]}',
        ],
    )
    assert result.exit_code == 0

    original_objects = pg_repo_local.head.get_table("fruits").objects
    assert len(original_objects) == 2

    runner.invoke(
        sql_c,
        [
            "UPDATE \"test/pg_mount\".fruits SET name = 'banana' WHERE fruit_id = 1;"
            "INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')"
        ],
    )

    # Commit with no chunking
    result = runner.invoke(commit_c, [str(pg_repo_local)])
    assert result.exit_code == 0
    new_objects = pg_repo_local.head.get_table("fruits").objects

    # New change appended to the objects' list
    assert len(new_objects) == 3
    assert new_objects[0] == original_objects[0]
    assert new_objects[1] == original_objects[1]

    runner.invoke(
        sql_c,
        [
            "UPDATE \"test/pg_mount\".fruits SET name = 'tomato' WHERE fruit_id = 1;"
            "INSERT INTO \"test/pg_mount\".fruits VALUES (4, 'kunquat')"
        ],
    )
    result = runner.invoke(commit_c, [str(pg_repo_local), "--split-changesets"])
    assert result.exit_code == 0
    new_new_objects = pg_repo_local.head.get_table("fruits").objects
    assert len(new_new_objects) == 5


def test_commandline_commit_bloom(pg_repo_local):
    runner = CliRunner()

    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    pg_repo_local.run_sql("ALTER TABLE vegetables ADD PRIMARY KEY (vegetable_id)")

    result = runner.invoke(
        commit_c,
        [
            str(pg_repo_local),
            "--snap",
            "--index-options",
            '{"fruits": {"bloom": {"name": {"probability": 0.001}}}}',
        ],
    )
    assert result.exit_code == 0

    fruit_objects = pg_repo_local.head.get_table("fruits").objects
    assert len(fruit_objects) == 1
    vegetable_objects = pg_repo_local.head.get_table("vegetables").objects
    assert len(vegetable_objects) == 1
    object_meta = pg_repo_local.objects.get_object_meta(fruit_objects + vegetable_objects)
    assert "bloom" in object_meta[fruit_objects[0]].object_index
    assert "bloom" not in object_meta[vegetable_objects[0]].object_index


def test_commandline_tag_checkout(pg_repo_local):
    runner = CliRunner()
    # Do the quick setting up with the same commit structure
    old_head = pg_repo_local.head.image_hash
    runner.invoke(sql_c, ["INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')"])
    runner.invoke(
        sql_c, ['CREATE TABLE "test/pg_mount".mushrooms (mushroom_id integer, name varchar)']
    )
    runner.invoke(sql_c, ['DROP TABLE "test/pg_mount".vegetables'])
    runner.invoke(sql_c, ['DELETE FROM "test/pg_mount".fruits WHERE fruit_id = 1'])
    runner.invoke(sql_c, ['SELECT * FROM "test/pg_mount".fruits'])
    result = runner.invoke(commit_c, [str(pg_repo_local), "-m", "Test commit"])
    assert result.exit_code == 0

    new_head = pg_repo_local.head.image_hash

    # sgr tag <repo> <tag>: tags the current HEAD
    result = runner.invoke(tag_c, [str(pg_repo_local), "v2"])
    assert result.exit_code == 0
    assert pg_repo_local.images["v2"].image_hash == new_head

    # sgr tag <repo>:imagehash <tag>:
    result = runner.invoke(tag_c, [str(pg_repo_local) + ":" + old_head[:10], "v1"])
    assert result.exit_code == 0
    assert pg_repo_local.images["v1"].image_hash == old_head

    # sgr tag <mountpoint> with the same tag -- should move the tag to current HEAD again
    result = runner.invoke(tag_c, [str(pg_repo_local), "v1"])
    assert result.exit_code == 0
    assert pg_repo_local.images["v1"].image_hash == new_head

    # Tag the old head again
    result = runner.invoke(tag_c, [str(pg_repo_local) + ":" + old_head[:10], "v1"])
    assert result.exit_code == 0

    # list tags
    result = runner.invoke(tag_c, [str(pg_repo_local)])
    assert old_head[:12] + ": v1" in result.output
    assert new_head[:12] + ": HEAD, v2" in result.output

    # List tags on a single image
    result = runner.invoke(tag_c, [str(pg_repo_local) + ":" + old_head[:20]])
    assert "v1" in result.output
    assert "HEAD, v2" not in result.output

    # Checkout by tag
    runner.invoke(checkout_c, [str(pg_repo_local) + ":v1"])
    assert pg_repo_local.head.image_hash == old_head

    # Checkout by hash
    runner.invoke(checkout_c, [str(pg_repo_local) + ":" + new_head[:20]])
    assert pg_repo_local.head.image_hash == new_head

    # Checkout with uncommitted changes
    runner.invoke(sql_c, ["INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')"])
    result = runner.invoke(checkout_c, [str(pg_repo_local) + ":v1"])
    assert result.exit_code != 0
    assert "test/pg_mount has pending changes!" in str(result.exc_info)

    result = runner.invoke(checkout_c, [str(pg_repo_local) + ":v1", "-f"])
    assert result.exit_code == 0
    assert not pg_repo_local.has_pending_changes()

    # uncheckout with uncommitted changes
    runner.invoke(sql_c, ["INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')"])
    result = runner.invoke(checkout_c, [str(pg_repo_local), "-u"])
    assert result.exit_code != 0
    assert "test/pg_mount has pending changes!" in str(result.exc_info)

    # uncheckout
    result = runner.invoke(checkout_c, [str(pg_repo_local), "-u", "-f"])
    assert result.exit_code == 0
    assert pg_repo_local.head is None
    assert not get_engine().schema_exists(str(pg_repo_local))

    # Delete the tag -- check the help entry correcting the command
    result = runner.invoke(tag_c, ["--delete", str(pg_repo_local), "v1"])
    assert result.exit_code != 0
    assert "--delete test/pg_mount:TAG_TO_DELETE" in result.output

    result = runner.invoke(tag_c, ["--delete", str(pg_repo_local) + ":" + "v1"])
    assert result.exit_code == 0
    assert pg_repo_local.images.by_tag("v1", raise_on_none=False) is None


def test_commandline_lq_checkout(pg_repo_local):
    runner = CliRunner()
    # Uncheckout first
    result = runner.invoke(checkout_c, [str(pg_repo_local), "-u", "-f"])
    assert result.exit_code == 0
    assert pg_repo_local.head is None
    assert not get_engine().schema_exists(str(pg_repo_local))

    result = runner.invoke(checkout_c, [str(pg_repo_local) + ":latest", "-l"])
    assert result.exit_code == 0
    assert pg_repo_local.head is not None
    assert get_engine().schema_exists(str(pg_repo_local))
    assert get_engine().get_table_type(str(pg_repo_local), "fruits") in ("FOREIGN TABLE", "FOREIGN")
