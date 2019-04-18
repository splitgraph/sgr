import json
import os
import subprocess
from decimal import Decimal

import pytest

from splitgraph import ResultShape, get_engine
from splitgraph.commandline import *
from splitgraph.commandline._common import ImageType
from splitgraph.commandline.example import generate_c, alter_c, splitfile_c
from splitgraph.commandline.image_info import object_c
from splitgraph.config import PG_PWD, PG_USER
from splitgraph.core._common import parse_connection_string, serialize_connection_string, insert
from splitgraph.core.engine import repository_exists, init_engine
from splitgraph.core.metadata_manager import OBJECT_COLS
from splitgraph.core.registry import get_published_info
from splitgraph.core.repository import Repository
from splitgraph.hooks.mount_handlers import get_mount_handlers
from test.splitgraph.conftest import OUTPUT, SPLITFILE_ROOT, \
    MG_MNT

try:
    # python 3.4+ should use builtin unittest.mock not mock package
    from unittest.mock import patch
except ImportError:
    from mock import patch

from click.testing import CliRunner


def test_image_spec_parsing():
    assert ImageType()('test/pg_mount') == (Repository('test', 'pg_mount'), 'latest')
    assert ImageType(default='HEAD')('test/pg_mount') == (Repository('test', 'pg_mount'), 'HEAD')
    assert ImageType()('test/pg_mount:some_tag') == (Repository('test', 'pg_mount'), 'some_tag')
    assert ImageType()('pg_mount') == (Repository('', 'pg_mount'), 'latest')
    assert ImageType()('pg_mount:some_tag') == (Repository('', 'pg_mount'), 'some_tag')
    assert ImageType(default='HEAD')('pg_mount:some_tag') == (Repository('', 'pg_mount'), 'some_tag')


def test_conn_string_parsing():
    assert parse_connection_string("user:pwd@host.com:1234/db") == ("host.com", 1234, "user", "pwd", "db")
    with pytest.raises(ValueError):
        parse_connection_string("abcdef@blabla/blabla")


def test_conn_string_serialization():
    assert serialize_connection_string("host.com", 1234, "user", "pwd", "db") == "user:pwd@host.com:1234/db"


def test_commandline_basics(pg_repo_local, mg_repo_local):
    runner = CliRunner()

    # sgr status
    result = runner.invoke(status_c, [])
    assert pg_repo_local.to_schema() in result.output
    assert mg_repo_local.to_schema() in result.output
    old_head = pg_repo_local.head
    assert old_head.image_hash in result.output

    # sgr sql
    runner.invoke(sql_c, ["INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')"])
    runner.invoke(sql_c, ["CREATE TABLE \"test/pg_mount\".mushrooms (mushroom_id integer, name varchar)"])
    runner.invoke(sql_c, ["DROP TABLE \"test/pg_mount\".vegetables"])
    runner.invoke(sql_c, ["DELETE FROM \"test/pg_mount\".fruits WHERE fruit_id = 1"])
    result = runner.invoke(sql_c, ["SELECT * FROM \"test/pg_mount\".fruits"])
    assert "(3, 'mayonnaise')" in result.output
    assert "(1, 'apple')" not in result.output
    # Test schema search_path
    result = runner.invoke(sql_c, ["--schema", "test/pg_mount", "SELECT * FROM fruits"])
    assert "(3, 'mayonnaise')" in result.output
    assert "(1, 'apple')" not in result.output

    def check_diff(args):
        result = runner.invoke(diff_c, [str(a) for a in args])
        assert "added 1 row" in result.output
        assert "removed 1 row" in result.output
        assert "vegetables: table removed"
        assert "mushrooms: table added"
        result = runner.invoke(diff_c, [str(a) for a in args] + ['-v'])
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
    result = runner.invoke(commit_c, [str(pg_repo_local), '-m', 'Test commit', '--snap'])
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
    result = runner.invoke(diff_c, [str(pg_repo_local), new_head.image_hash[:20], old_head.image_hash[:20]])
    assert "added 1 row" in result.output
    assert "removed 1 row" in result.output
    assert "vegetables: table removed"
    assert "mushrooms: table added"
    result = runner.invoke(diff_c, [str(pg_repo_local), new_head.image_hash[:20], old_head.image_hash[:20], '-v'])
    # Since the images were flipped, here the result is that, since the row that was added
    # didn't exist in the first image, diff() thinks it was _removed_ and vice versa for the other row.
    assert "- (3, 'mayonnaise')" in result.output
    assert "+ (1, 'apple')" in result.output

    # sgr status with the new commit
    result = runner.invoke(status_c, [str(pg_repo_local)])
    assert 'test/pg_mount' in result.output
    assert 'Parent: ' + old_head.image_hash in result.output
    assert new_head.image_hash in result.output

    # sgr log
    result = runner.invoke(log_c, [str(pg_repo_local)])
    assert old_head.image_hash in result.output
    assert new_head.image_hash in result.output
    assert "Test commit" in result.output

    # sgr log (tree)
    result = runner.invoke(log_c, [str(pg_repo_local), '-t'])
    assert old_head.image_hash[:5] in result.output
    assert new_head.image_hash[:5] in result.output

    # sgr show the new commit
    result = runner.invoke(show_c, [str(pg_repo_local) + ':' + new_head.image_hash[:20], '-v'])
    assert "Test commit" in result.output
    assert "Parent: " + old_head.image_hash in result.output
    fruit_objs = new_head.get_table('fruits').objects
    mushroom_objs = new_head.get_table('mushrooms').objects

    # Check verbose show also has the actual object IDs
    for o in fruit_objs + mushroom_objs:
        assert o in result.output


def test_commandline_commit_chunk(pg_repo_local):
    runner = CliRunner()
    result = runner.invoke(commit_c, [str(pg_repo_local), '--snap', '--chunk-size=1'])
    assert result.exit_code == 0

    original_objects = pg_repo_local.head.get_table('fruits').objects
    assert len(original_objects) == 2

    runner.invoke(sql_c, ["UPDATE \"test/pg_mount\".fruits SET name = 'banana' WHERE fruit_id = 1;"
                          "INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')"])

    # Commit with no chunking
    result = runner.invoke(commit_c, [str(pg_repo_local)])
    assert result.exit_code == 0
    new_objects = pg_repo_local.head.get_table('fruits').objects

    # First object the same, second object is new
    assert len(new_objects) == 2
    assert new_objects[0] == original_objects[0]
    assert new_objects[1] != original_objects[1]

    runner.invoke(sql_c, ["UPDATE \"test/pg_mount\".fruits SET name = 'tomato' WHERE fruit_id = 1;"
                          "INSERT INTO \"test/pg_mount\".fruits VALUES (4, 'kunquat')"])
    result = runner.invoke(commit_c, [str(pg_repo_local), '--split-changesets'])
    assert result.exit_code == 0
    new_new_objects = pg_repo_local.head.get_table('fruits').objects
    assert len(new_new_objects) == 3


def test_object_info(local_engine_empty):
    runner = CliRunner()

    q = insert("objects", OBJECT_COLS)
    local_engine_empty.run_sql(q, ("base_1", "FRAG", None, "ns1", 12345, 'HASH1', 'HASH2', {"col_1": [10, 20]}))
    local_engine_empty.run_sql(q, ("patch_1", "FRAG", "base_1", "ns1", 6789, 'HASH1', 'HASH2', {"col_1": [10, 20]}))
    local_engine_empty.run_sql(q, ("patch_2", "FRAG", "base_1", "ns1", 1011, 'HASH1', 'HASH2', {"col_1": [10, 20],
                                                                                                "col_2": ['bla',
                                                                                                          'ble']}))
    # base_1: external, cached locally
    local_engine_empty.run_sql(insert("object_locations", ("object_id", "protocol", "location")),
                               ("base_1", "HTTP", "example.com/objects/base_1.tgz"))
    local_engine_empty.run_sql(insert("object_cache_status", ("object_id",)), ("base_1",))
    local_engine_empty.run_sql("CREATE TABLE splitgraph_meta.base_1 (col_1 INTEGER)")

    # patch_1: on the engine, uncached locally
    # patch_2: created here, cached locally
    local_engine_empty.run_sql("CREATE TABLE splitgraph_meta.patch_2 (col_1 INTEGER)")

    result = runner.invoke(object_c, ["base_1"])
    assert result.exit_code == 0
    assert result.output == """Object ID: base_1

Parent: None
Namespace: ns1
Format: FRAG
Size: 12.06 KiB
Insertion hash: HASH1
Deletion hash: HASH2
Column index:
  col_1: [10, 20]

Location: cached locally
Original location: example.com/objects/base_1.tgz (HTTP)
"""

    result = runner.invoke(object_c, ["patch_1"])
    assert result.exit_code == 0
    assert "Location: remote engine" in result.output

    result = runner.invoke(object_c, ["patch_2"])
    assert result.exit_code == 0
    assert "Location: created locally" in result.output


def test_upstream_management(pg_repo_local):
    runner = CliRunner()

    # sgr upstream test/pg_mount
    result = runner.invoke(upstream_c, ["test/pg_mount"])
    assert result.exit_code == 0
    assert "has no upstream" in result.output

    # Set to nonexistent engine
    result = runner.invoke(upstream_c, ["test/pg_mount", "--set", "dummy_engine", "test/pg_mount"])
    assert result.exit_code == 1
    assert "Remote engine 'dummy_engine' does not exist" in result.output

    # Set to existing engine (should we check the repo actually exists?)
    result = runner.invoke(upstream_c, ["test/pg_mount", "--set", "remote_engine", "test/pg_mount"])
    assert result.exit_code == 0
    assert "set to track remote_engine:test/pg_mount" in result.output

    # Get upstream again
    result = runner.invoke(upstream_c, ["test/pg_mount"])
    assert result.exit_code == 0
    assert "is tracking remote_engine:test/pg_mount" in result.output

    # Reset it
    result = runner.invoke(upstream_c, ["test/pg_mount", "--reset"])
    assert result.exit_code == 0
    assert "Deleted upstream for test/pg_mount" in result.output
    assert pg_repo_local.upstream is None

    # Reset it again
    result = runner.invoke(upstream_c, ["test/pg_mount", "--reset"])
    assert result.exit_code == 1
    assert "has no upstream" in result.output


def test_commandline_tag_checkout(pg_repo_local):
    runner = CliRunner()
    # Do the quick setting up with the same commit structure
    old_head = pg_repo_local.head.image_hash
    runner.invoke(sql_c, ["INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')"])
    runner.invoke(sql_c, ["CREATE TABLE \"test/pg_mount\".mushrooms (mushroom_id integer, name varchar)"])
    runner.invoke(sql_c, ["DROP TABLE \"test/pg_mount\".vegetables"])
    runner.invoke(sql_c, ["DELETE FROM \"test/pg_mount\".fruits WHERE fruit_id = 1"])
    runner.invoke(sql_c, ["SELECT * FROM \"test/pg_mount\".fruits"])
    result = runner.invoke(commit_c, [str(pg_repo_local), '-m', 'Test commit'])
    assert result.exit_code == 0

    new_head = pg_repo_local.head.image_hash

    # sgr tag <repo> <tag>: tags the current HEAD
    result = runner.invoke(tag_c, [str(pg_repo_local), 'v2'])
    assert result.exit_code == 0
    assert pg_repo_local.images['v2'].image_hash == new_head

    # sgr tag <repo>:imagehash <tag>:
    result = runner.invoke(tag_c, [str(pg_repo_local) + ':' + old_head[:10], 'v1'])
    assert result.exit_code == 0
    assert pg_repo_local.images['v1'].image_hash == old_head

    # sgr tag <mountpoint> with the same tag -- should move the tag to current HEAD again
    result = runner.invoke(tag_c, [str(pg_repo_local), 'v1'])
    assert result.exit_code == 0
    assert pg_repo_local.images['v1'].image_hash == new_head

    # Tag the old head again
    result = runner.invoke(tag_c, [str(pg_repo_local) + ':' + old_head[:10], 'v1'])
    assert result.exit_code == 0

    # list tags
    result = runner.invoke(tag_c, [str(pg_repo_local)])
    assert old_head[:12] + ': v1' in result.output
    assert new_head[:12] + ': HEAD, v2' in result.output

    # List tags on a single image
    result = runner.invoke(tag_c, [str(pg_repo_local) + ':' + old_head[:20]])
    assert 'v1' in result.output
    assert 'HEAD, v2' not in result.output

    # Checkout by tag
    runner.invoke(checkout_c, [str(pg_repo_local) + ':v1'])
    assert pg_repo_local.head.image_hash == old_head

    # Checkout by hash
    runner.invoke(checkout_c, [str(pg_repo_local) + ':' + new_head[:20]])
    assert pg_repo_local.head.image_hash == new_head

    # Checkout with uncommitted changes
    runner.invoke(sql_c, ["INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')"])
    result = runner.invoke(checkout_c, [str(pg_repo_local) + ':v1'])
    assert result.exit_code != 0
    assert "test/pg_mount has pending changes!" in str(result.exc_info)

    result = runner.invoke(checkout_c, [str(pg_repo_local) + ':v1', '-f'])
    assert result.exit_code == 0
    assert not pg_repo_local.has_pending_changes()

    # uncheckout with uncommitted changes
    runner.invoke(sql_c, ["INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')"])
    result = runner.invoke(checkout_c, [str(pg_repo_local), '-u'])
    assert result.exit_code != 0
    assert "test/pg_mount has pending changes!" in str(result.exc_info)

    # uncheckout
    result = runner.invoke(checkout_c, [str(pg_repo_local), '-u', '-f'])
    assert result.exit_code == 0
    assert pg_repo_local.head is None
    assert not get_engine().schema_exists(str(pg_repo_local))

    # Delete the tag -- check the help entry correcting the command
    result = runner.invoke(tag_c, ['--remove', str(pg_repo_local), 'v1'])
    assert result.exit_code != 0
    assert '--remove test/pg_mount:TAG_TO_DELETE' in result.output

    result = runner.invoke(tag_c, ['--remove', str(pg_repo_local) + ':' + 'v1'])
    assert result.exit_code == 0
    assert pg_repo_local.images.by_tag('v1', raise_on_none=False) is None


@pytest.mark.mounting
def test_misc_mountpoint_management(pg_repo_local, mg_repo_local):
    runner = CliRunner()

    result = runner.invoke(status_c)
    assert str(pg_repo_local) in result.output
    assert str(mg_repo_local) in result.output

    # sgr rm -y test/pg_mount (no prompting)
    result = runner.invoke(rm_c, [str(mg_repo_local), '-y'])
    assert result.exit_code == 0
    assert not repository_exists(mg_repo_local)

    # sgr cleanup
    result = runner.invoke(cleanup_c)
    assert "Deleted 1 physical object(s)" in result.output

    # sgr init
    result = runner.invoke(init_c, ['output'])
    assert "Initialized empty repository output" in result.output
    assert repository_exists(OUTPUT)

    # sgr mount
    result = runner.invoke(mount_c, ['mongo_fdw', str(mg_repo_local), '-c', 'originro:originpass@mongoorigin:27017',
                                     '-o', json.dumps({"stuff": {
            "db": "origindb",
            "coll": "stuff",
            "schema": {
                "name": "text",
                "duration": "numeric",
                "happy": "boolean"
            }}})])
    assert result.exit_code == 0
    assert mg_repo_local.run_sql("SELECT duration from stuff WHERE name = 'James'") == [(Decimal(2),)]


@pytest.mark.mounting
def test_import(pg_repo_local, mg_repo_local):
    runner = CliRunner()
    head = pg_repo_local.head

    # sgr import mountpoint, table, target_mountpoint (3-arg)
    result = runner.invoke(import_c, [str(mg_repo_local), 'stuff', str(pg_repo_local)])
    assert result.exit_code == 0
    new_head = pg_repo_local.head
    assert new_head.get_table('stuff')
    assert not head.get_table('stuff')

    # sgr import with alias
    result = runner.invoke(import_c, [str(mg_repo_local), 'stuff', str(pg_repo_local), 'stuff_copy'])
    assert result.exit_code == 0
    new_new_head = pg_repo_local.head
    assert new_new_head.get_table('stuff_copy')
    assert not new_head.get_table('stuff_copy')

    # sgr import with alias and custom image hash
    mg_repo_local.run_sql("DELETE FROM stuff")
    new_mg_head = mg_repo_local.commit()

    result = runner.invoke(import_c,
                           [str(mg_repo_local) + ':' + new_mg_head.image_hash,
                            'stuff', str(pg_repo_local), 'stuff_empty'])
    assert result.exit_code == 0
    new_new_new_head = pg_repo_local.head
    assert new_new_new_head.get_table('stuff_empty')
    assert not new_new_head.get_table('stuff_empty')
    assert pg_repo_local.run_sql("SELECT * FROM stuff_empty") == []

    # sgr import with query, no alias
    result = runner.invoke(import_c,
                           [str(mg_repo_local) + ':' + new_mg_head.image_hash,
                            'SELECT * FROM stuff', str(pg_repo_local)])
    assert result.exit_code != 0
    assert 'TARGET_TABLE is required' in str(result.stdout)


def test_pull_push(pg_repo_local, pg_repo_remote):
    runner = CliRunner()

    result = runner.invoke(clone_c, [str(pg_repo_local)])
    assert result.exit_code == 0
    assert repository_exists(pg_repo_local)

    pg_repo_remote.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    remote_engine_head = pg_repo_remote.commit()

    result = runner.invoke(pull_c, [str(pg_repo_local)])
    assert result.exit_code == 0
    pg_repo_local.images.by_hash(remote_engine_head.image_hash).checkout()

    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'mustard')")
    local_head = pg_repo_local.commit()

    assert not pg_repo_remote.images.by_hash(local_head.image_hash, raise_on_none=False)
    result = runner.invoke(push_c, [str(pg_repo_local), '-h', 'DB'])
    assert result.exit_code == 0
    assert pg_repo_local.head.get_table('fruits')

    pg_repo_local.head.tag('v1')
    pg_repo_local.commit_engines()
    result = runner.invoke(publish_c, [str(pg_repo_local), 'v1', '-r', SPLITFILE_ROOT + 'README.md'])
    assert result.exit_code == 0
    image_hash, published_dt, deps, readme, schemata, previews = get_published_info(pg_repo_remote, 'v1')
    assert image_hash == local_head.image_hash
    assert deps == []
    assert readme == "Test readme for a test dataset."
    assert schemata == {'fruits': [['fruit_id', 'integer', False],
                                   ['name', 'character varying', False]],
                        'vegetables': [['vegetable_id', 'integer', False],
                                       ['name', 'character varying', False]]}
    assert previews == {'fruits': [[1, 'apple'], [2, 'orange'], [3, 'mayonnaise'], [4, 'mustard']],
                        'vegetables': [[1, 'potato'], [2, 'carrot']]}


def test_splitfile(local_engine_empty, pg_repo_remote):
    runner = CliRunner()

    result = runner.invoke(build_c, [SPLITFILE_ROOT + 'import_remote_multiple.splitfile',
                                     '-a', 'TAG', 'latest', '-o', 'output'])
    assert result.exit_code == 0
    assert OUTPUT.run_sql("SELECT id, fruit, vegetable FROM join_table") \
           == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]

    # Test the sgr provenance command. First, just list the dependencies of the new image.
    result = runner.invoke(provenance_c, ['output:latest'])
    assert 'test/pg_mount:%s' % pg_repo_remote.images['latest'].image_hash in result.output

    # Second, output the full splitfile (-f)
    result = runner.invoke(provenance_c, ['output:latest', '-f'])
    assert 'FROM test/pg_mount:%s IMPORT' % pg_repo_remote.images['latest'].image_hash in result.output
    assert 'SQL CREATE TABLE join_table AS SELECT' in result.output


def test_splitfile_rebuild_update(local_engine_empty, pg_repo_remote_multitag):
    runner = CliRunner()

    result = runner.invoke(build_c, [SPLITFILE_ROOT + 'import_remote_multiple.splitfile',
                                     '-a', 'TAG', 'v1', '-o', 'output'])
    assert result.exit_code == 0

    # Rerun the output:latest against v2 of the test/pg_mount
    result = runner.invoke(rebuild_c, ['output:latest', '--against', 'test/pg_mount:v2'])
    output_v2 = OUTPUT.head
    assert result.exit_code == 0
    v2 = pg_repo_remote_multitag.images['v2']
    assert output_v2.provenance() == [(pg_repo_remote_multitag, v2.image_hash)]

    # Now rerun the output:latest against the latest version of everything.
    # In this case, this should all resolve to the same version of test/pg_mount (v2) and not produce
    # any extra commits.
    curr_commits = OUTPUT.images()
    result = runner.invoke(rebuild_c, ['output:latest', '-u'])
    assert result.exit_code == 0
    assert output_v2 == OUTPUT.head
    assert OUTPUT.images() == curr_commits


@pytest.mark.mounting
def test_mount_and_import(local_engine_empty):
    runner = CliRunner()
    try:
        # sgr mount
        result = runner.invoke(mount_c, ['mongo_fdw', 'tmp', '-c', 'originro:originpass@mongoorigin:27017',
                                         '-o', json.dumps({"stuff": {
                "db": "origindb",
                "coll": "stuff",
                "schema": {
                    "name": "text",
                    "duration": "numeric",
                    "happy": "boolean"
                }}})])
        assert result.exit_code == 0

        result = runner.invoke(import_c, ['tmp', 'stuff', str(MG_MNT)])
        assert result.exit_code == 0
        assert MG_MNT.head.get_table('stuff')

        result = runner.invoke(import_c, ['tmp', 'SELECT * FROM stuff WHERE duration > 10', str(MG_MNT),
                                          'stuff_query'])
        assert result.exit_code == 0
        assert MG_MNT.head.get_table('stuff_query')
    finally:
        Repository('', 'tmp').delete()


def test_rm_repositories(pg_repo_local, pg_repo_remote):
    runner = CliRunner()

    # sgr rm test/pg_mount, say "no"
    result = runner.invoke(rm_c, [str(pg_repo_local)], input='n\n')
    assert result.exit_code == 1
    assert "Repository test/pg_mount will be deleted" in result.output
    assert repository_exists(pg_repo_local)

    # sgr rm test/pg_mount, say "yes"
    result = runner.invoke(rm_c, [str(pg_repo_local)], input='y\n')
    assert result.exit_code == 0
    assert not repository_exists(pg_repo_local)

    # sgr rm test/pg_mount -r remote_engine
    result = runner.invoke(rm_c, [str(pg_repo_remote), '-r', 'remote_engine'], input='y\n')
    assert result.exit_code == 0
    assert not repository_exists(pg_repo_remote)


def test_rm_images(pg_repo_local_multitag, pg_repo_remote_multitag):
    # Play around with both engines for simplicity -- both have 2 images with 2 tags
    runner = CliRunner()
    local_v1 = pg_repo_local_multitag.images['v1'].image_hash
    local_v2 = pg_repo_local_multitag.images['v2'].image_hash

    # Test deleting checked out image causes an error
    result = runner.invoke(rm_c, [str(pg_repo_local_multitag) + ':v2'])
    assert result.exit_code != 0
    assert "do sgr checkout -u test/pg_mount" in str(result.exc_info)

    pg_repo_local_multitag.uncheckout()

    # sgr rm test/pg_mount:v2, say "no"
    result = runner.invoke(rm_c, [str(pg_repo_local_multitag) + ':v2'], input='n\n')
    assert result.exit_code == 1
    # Specify most of the output verbatim here to make sure it's not proposing
    # to delete more than needed (just the single image and the single v2 tag)
    assert "Images to be deleted:\n" + local_v2 + '\nTotal: 1\n\nTags to be deleted:\nv2\nTotal: 1' \
           in result.output
    # Since we cancelled the operation, 'v2' still remains.
    assert pg_repo_local_multitag.images['v2'].image_hash == local_v2
    assert pg_repo_local_multitag.images[local_v2] is not None

    # Uncheckout the remote too (it's supposed to be bare anyway)
    remote_v2 = pg_repo_remote_multitag.images['v2'].image_hash
    pg_repo_remote_multitag.uncheckout()

    # sgr rm test/pg_mount:v2 -r remote_engine, say "yes"
    result = runner.invoke(rm_c, [str(pg_repo_remote_multitag) + ':v2', '-r', 'remote_engine'], input='y\n')
    assert result.exit_code == 0
    assert pg_repo_remote_multitag.images.by_tag('v2', raise_on_none=False) is None
    assert pg_repo_remote_multitag.images.by_hash(remote_v2, raise_on_none=False) is None

    # sgr rm test/pg_mount:v1 -y
    # Should delete both images since v2 depends on v1
    result = runner.invoke(rm_c, [str(pg_repo_local_multitag) + ':v1', '-y'])
    assert result.exit_code == 0
    assert local_v2 in result.output
    assert local_v1 in result.output
    assert 'v1' in result.output
    assert 'v2' in result.output
    # One image remaining (the 00000.. base image)
    assert len(pg_repo_local_multitag.images()) == 1


def test_mount_docstring_generation():
    runner = CliRunner()

    # General mount help: should have all the handlers autoregistered and listed
    result = runner.invoke(mount_c, ['--help'])
    assert result.exit_code == 0
    for handler_name in get_mount_handlers():
        assert handler_name in result.output

    # Test the reserved params (that we parse separately) don't make it into the help text
    # and that other function args from the docstring do.
    result = runner.invoke(mount_c, ['postgres_fdw', '--help'])
    assert result.exit_code == 0
    assert "mountpoint" not in result.output
    assert "remote_schema" in result.output


def test_prune(pg_repo_local_multitag, pg_repo_remote_multitag):
    runner = CliRunner()
    # Two engines, two repos, two images in each (tagged v1 and v2, v1 is the parent of v2).
    pg_repo_remote_multitag.uncheckout()

    # sgr prune test/pg_mount -- all images are tagged, nothing to do.
    result = runner.invoke(prune_c, [str(pg_repo_local_multitag)])
    assert result.exit_code == 0
    assert "Nothing to do" in result.output

    # Delete tag v2 and run sgr prune -r remote_engine test/pg_mount, say "no": the image
    # that used to be 'v2' now isn't tagged so it will be a candidate for removal (but not the v1 image).
    remote_v2 = pg_repo_remote_multitag.images['v2']
    remote_v2.delete_tag('v2')
    pg_repo_remote_multitag.commit_engines()

    result = runner.invoke(prune_c, [str(pg_repo_remote_multitag), '-r', 'remote_engine'], input='n\n')
    assert result.exit_code == 1  # Because "n" aborted the command
    assert remote_v2.image_hash in result.output
    assert 'Total: 1' in result.output
    # Make sure the image still exists
    assert pg_repo_remote_multitag.images.by_hash(remote_v2.image_hash)

    # Delete tag v1 and run sgr prune -r remote_engine -y test_pg_mount:
    # now both images aren't tagged so will get removed.
    remote_v1 = pg_repo_remote_multitag.images['v1']
    remote_v1.delete_tag('v1')
    pg_repo_remote_multitag.commit_engines()
    result = runner.invoke(prune_c, [str(pg_repo_remote_multitag), '-r', 'remote_engine', '-y'])
    assert result.exit_code == 0
    assert remote_v2.image_hash in result.output
    assert remote_v1.image_hash in result.output
    # 2 images + the 000... image
    assert 'Total: 3' in result.output
    assert not pg_repo_remote_multitag.images()

    # Finally, delete both tags from the local engine and prune. Since there's still
    # a HEAD tag pointing to the ex-v2, nothing will actually happen.
    result = runner.invoke(prune_c, [str(pg_repo_local_multitag), '-y'])
    assert "Nothing to do." in result.output
    # 2 images + the 000.. image
    assert len(pg_repo_local_multitag.images()) == 3
    assert len(pg_repo_local_multitag.get_all_hashes_tags()) == 3


def test_config_dumping():
    runner = CliRunner()

    # sgr config (normal, with passwords shielded)
    result = runner.invoke(config_c)
    assert result.exit_code == 0
    assert PG_PWD not in result.output
    assert "remote_engine:" in result.output
    assert ("SG_ENGINE_USER=%s" % PG_USER) in result.output
    assert "DUMMY=test.splitgraph.splitfile" in result.output
    assert "S3=splitgraph.hooks.s3" in result.output

    # sgr config -s (no password shielding)
    result = runner.invoke(config_c, ['-s'])
    assert result.exit_code == 0
    assert ("SG_ENGINE_USER=%s" % PG_USER) in result.output
    assert ("SG_ENGINE_PWD=%s" % PG_PWD) in result.output
    assert "remote_engine:" in result.output

    # sgr config -sc (no password shielding, output in config format)
    result = runner.invoke(config_c, ['-sc'])
    assert result.exit_code == 0
    assert ("SG_ENGINE_USER=%s" % PG_USER) in result.output
    assert ("SG_ENGINE_PWD=%s" % PG_PWD) in result.output
    assert "[remote: remote_engine]" in result.output
    assert "[defaults]" in result.output
    assert "[commands]" in result.output
    assert "[external_handlers]" in result.output
    assert "[mount_handlers]" in result.output
    assert "S3=splitgraph.hooks.s3" in result.output


def test_init_new_db():
    try:
        get_engine().delete_database('testdb')

        # CliRunner doesn't run in a brand new process and by that point PG_DB has propagated
        # through a few modules that are difficult to patch out, so let's just shell out.
        output = subprocess.check_output("SG_LOGLEVEL=INFO SG_ENGINE_DB_NAME=testdb sgr init", shell=True,
                                         stderr=subprocess.STDOUT)
        output = output.decode('utf-8')
        assert "Creating database testdb" in output
        assert "Installing the audit trigger" in output
    finally:
        get_engine().delete_database('testdb')


def test_init_skip_audit():
    # Test engine initialization where we don't install an audit trigger
    runner = CliRunner()
    engine = get_engine()
    try:
        engine.run_sql("DROP SCHEMA IF EXISTS audit CASCADE")
        assert not engine.schema_exists('audit')
        result = runner.invoke(init_c, ['--skip-audit'])
        assert result.exit_code == 0
        assert not engine.schema_exists('audit')
    finally:
        init_engine(skip_audit=False)
        assert engine.schema_exists('audit')


def test_init_override_engine():
    # Doesn't really test that all of the overridden engine's config makes it into the Engine object that
    # initialize() is called on but that's tested implicitly throughout the rest of the suite: here, since
    # initialize() logs the engine it uses, check that the remote engine is being initialized.

    # Inject the config here. If this check_output breaks (with something like "KeyError: 'remotes' not in CONFIG"),
    # this path is probably the culprit.
    output = subprocess.check_output("SG_CONFIG_FILE=%s SG_LOGLEVEL=INFO SG_ENGINE=remote_engine sgr init"
                                     % os.path.join(os.path.dirname(__file__), '../resources/.sgconfig'), shell=True,
                                     stderr=subprocess.STDOUT)
    output = output.decode('utf-8')
    assert str(get_engine('remote_engine')) in output


def test_examples(local_engine_empty):
    # Test the example-generating commands used in the quickstart

    runner = CliRunner()
    result = runner.invoke(generate_c, ['example/repo_1'])
    assert result.exit_code == 0

    repo = Repository.from_schema("example/repo_1")
    assert len(repo.images()) == 2
    assert repo.run_sql("SELECT COUNT(*) FROM demo", return_shape=ResultShape.ONE_ONE) == 10
    assert repo.diff('demo', repo.head, None, aggregate=True) == (0, 0, 0)

    result = runner.invoke(alter_c, ['example/repo_1'])
    assert result.exit_code == 0
    assert len(repo.images()) == 2
    assert repo.diff('demo', repo.head, None, aggregate=True) == (2, 2, 2)

    result = runner.invoke(splitfile_c, ['example/repo_1', 'example/repo_2'])
    assert result.exit_code == 0
    assert 'FROM example/repo_1 IMPORT demo AS table_1' in result.stdout
    assert 'FROM example/repo_2:${IMAGE_2} IMPORT demo AS table_2' in result.stdout


def test_commandline_lq_checkout(pg_repo_local):
    runner = CliRunner()
    # Uncheckout first
    result = runner.invoke(checkout_c, [str(pg_repo_local), '-u', '-f'])
    assert result.exit_code == 0
    assert pg_repo_local.head is None
    assert not get_engine().schema_exists(str(pg_repo_local))

    result = runner.invoke(checkout_c, [str(pg_repo_local) + ':latest', '-l'])
    assert result.exit_code == 0
    assert pg_repo_local.head is not None
    assert get_engine().schema_exists(str(pg_repo_local))
    assert get_engine().get_table_type(str(pg_repo_local), 'fruits') in ('FOREIGN TABLE', 'FOREIGN')


def test_commandline_dump_load(pg_repo_local):
    pg_repo_local.run_sql("ALTER TABLE fruits ADD PRIMARY KEY (fruit_id)")
    pg_repo_local.commit()
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    pg_repo_local.commit()
    pg_repo_local.run_sql("UPDATE fruits SET name = 'banana' WHERE fruit_id = 1")
    pg_repo_local.commit()
    pg_repo_local.head.tag('test_tag')

    runner = CliRunner()
    result = runner.invoke(dump_c, [str(pg_repo_local)], catch_exceptions=False)
    assert result.exit_code == 0

    dump = result.stdout

    # Now delete the repo and try loading the dump to test it actually works.
    pg_repo_local.delete()
    pg_repo_local.objects.cleanup()

    pg_repo_local.engine.run_sql(dump)

    pg_repo_local.images['test_tag'].checkout()

    assert pg_repo_local.run_sql("SELECT * FROM fruits ORDER BY fruit_id") \
           == [(1, 'banana'), (2, 'orange'), (3, 'mayonnaise')]
