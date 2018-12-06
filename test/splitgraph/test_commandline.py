import json
import subprocess
from decimal import Decimal

try:
    # python 3.4+ should use builtin unittest.mock not mock package
    from unittest.mock import patch
except ImportError:
    from mock import patch

import psycopg2
from click.testing import CliRunner

from splitgraph import to_repository, rm, repository_exists, Repository, get_upstream, get_all_hashes_tags, PG_PWD, \
    PG_USER, CONFIG, PG_PORT, PG_HOST
from splitgraph._data.images import get_all_image_info
from splitgraph._data.registry import get_published_info
from splitgraph.commandline import status_c, sql_c, diff_c, commit_c, log_c, show_c, tag_c, checkout_c, rm_c, \
    cleanup_c, init_c, mount_c, import_c, clone_c, pull_c, push_c, build_c, provenance_c, rebuild_c, publish_c
from splitgraph.commandline.common import image_spec_parser
from splitgraph.commandline.misc import prune_c, config_c
from splitgraph.commandline.push_pull import upstream_c
from splitgraph.commands import commit, checkout
from splitgraph.commands.diff import has_pending_changes
from splitgraph.commands.info import get_image, get_table
from splitgraph.commands.misc import table_exists_at
from splitgraph.commands.provenance import provenance
from splitgraph.commands.tagging import get_current_head, get_tagged_id, set_tag, delete_tag
from splitgraph.connection import override_driver_connection
from splitgraph.hooks.mount_handlers import get_mount_handlers
from test.splitgraph.conftest import PG_MNT, MG_MNT, OUTPUT, add_multitag_dataset_to_remote_driver, SPLITFILE_ROOT


def test_image_spec_parsing():
    assert image_spec_parser()('test/pg_mount') == (Repository('test', 'pg_mount'), 'latest')
    assert image_spec_parser(default='HEAD')('test/pg_mount') == (Repository('test', 'pg_mount'), 'HEAD')
    assert image_spec_parser()('test/pg_mount:some_tag') == (Repository('test', 'pg_mount'), 'some_tag')
    assert image_spec_parser()('pg_mount') == (Repository('', 'pg_mount'), 'latest')
    assert image_spec_parser()('pg_mount:some_tag') == (Repository('', 'pg_mount'), 'some_tag')
    assert image_spec_parser(default='HEAD')('pg_mount:some_tag') == (Repository('', 'pg_mount'), 'some_tag')


def test_commandline_basics(sg_pg_mg_conn):
    runner = CliRunner()

    # sgr status
    result = runner.invoke(status_c, [])
    assert PG_MNT.to_schema() in result.output
    assert MG_MNT.to_schema() in result.output
    old_head = get_current_head(PG_MNT)
    assert old_head in result.output

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
        assert "(3, 'mayonnaise'): +" in result.output
        assert "(1, 'apple'): -" in result.output

    # sgr diff, HEAD -> current staging (0-param)
    check_diff([PG_MNT])

    # sgr commit (with an extra snapshot
    result = runner.invoke(commit_c, [str(PG_MNT), '-m', 'Test commit', '-s'])
    assert result.exit_code == 0
    new_head = get_current_head(PG_MNT)
    assert new_head != old_head
    assert get_image(PG_MNT, new_head).parent_id == old_head
    assert new_head[:10] in result.output

    # sgr diff, old head -> new head (2-param), truncated hashes
    # technically these two hashes have a 2^(-20*4) = a 8e-25 chance of clashing but let's not dwell on that
    check_diff([PG_MNT, old_head[:20], new_head[:20]])

    # sgr diff, just the new head -- assumes the diff on top of the old head.
    check_diff([PG_MNT, new_head[:20]])

    # sgr diff, just the new head -- assumes the diff on top of the old head.
    check_diff([PG_MNT, new_head[:20]])
    #
    # # sgr diff, reverse order -- actually checks the two tables out and materializes them since there isn't a
    # # path of DIFF objects between them.
    # check_diff([PG_MNT, new_head[:20], old_head[:20]])

    # sgr status with the new commit
    result = runner.invoke(status_c, [str(PG_MNT)])
    assert 'test/pg_mount' in result.output
    assert 'Parent: ' + old_head in result.output
    assert new_head in result.output

    # sgr log
    result = runner.invoke(log_c, [str(PG_MNT)])
    assert old_head in result.output
    assert new_head in result.output
    assert "Test commit" in result.output

    # sgr log (tree)
    result = runner.invoke(log_c, [str(PG_MNT), '-t'])
    assert old_head[:5] in result.output
    assert new_head[:5] in result.output

    # sgr show the new commit
    result = runner.invoke(show_c, [str(PG_MNT) + ':' + new_head[:20], '-v'])
    assert "Test commit" in result.output
    assert "Parent: " + old_head in result.output
    fruit_objs = get_table(PG_MNT, 'fruits', new_head[:20])
    mushroom_objs = get_table(PG_MNT, 'mushrooms', new_head[:20])

    # Check verbose show also has the actual object IDs
    for o, of in fruit_objs + mushroom_objs:
        assert o in result.output


def test_upstream_management(sg_pg_conn):
    runner = CliRunner()

    # sgr upstream test/pg_mount
    result = runner.invoke(upstream_c, ["test/pg_mount"])
    assert result.exit_code == 0
    assert "has no upstream" in result.output

    # Set to nonexistent driver
    result = runner.invoke(upstream_c, ["test/pg_mount", "--set", "dummy_driver", "test/pg_mount"])
    assert result.exit_code == 1
    assert "Remote driver 'dummy_driver' does not exist" in result.output

    # Set to existing driver (should we check the repo actually exists?)
    result = runner.invoke(upstream_c, ["test/pg_mount", "--set", "remote_driver", "test/pg_mount"])
    assert result.exit_code == 0
    assert "set to track remote_driver:test/pg_mount" in result.output

    # Get upstream again
    result = runner.invoke(upstream_c, ["test/pg_mount"])
    assert result.exit_code == 0
    assert "is tracking remote_driver:test/pg_mount" in result.output

    # Reset it
    result = runner.invoke(upstream_c, ["test/pg_mount", "--reset"])
    assert result.exit_code == 0
    assert "Deleted upstream for test/pg_mount" in result.output
    assert get_upstream(PG_MNT) is None

    # Reset it again
    result = runner.invoke(upstream_c, ["test/pg_mount", "--reset"])
    assert result.exit_code == 1
    assert "has no upstream" in result.output


def test_commandline_tag_checkout(sg_pg_mg_conn):
    runner = CliRunner()
    # Do the quick setting up with the same commit structure
    old_head = get_current_head(PG_MNT)
    runner.invoke(sql_c, ["INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')"])
    runner.invoke(sql_c, ["CREATE TABLE \"test/pg_mount\".mushrooms (mushroom_id integer, name varchar)"])
    runner.invoke(sql_c, ["DROP TABLE \"test/pg_mount\".vegetables"])
    runner.invoke(sql_c, ["DELETE FROM \"test/pg_mount\".fruits WHERE fruit_id = 1"])
    runner.invoke(sql_c, ["SELECT * FROM \"test/pg_mount\".fruits"])
    result = runner.invoke(commit_c, [str(PG_MNT), '-m', 'Test commit'])
    assert result.exit_code == 0

    new_head = get_current_head(PG_MNT)

    # sgr tag <repo> <tag>: tags the current HEAD
    runner.invoke(tag_c, [str(PG_MNT), 'v2'])
    assert get_tagged_id(PG_MNT, 'v2') == new_head

    # sgr tag <repo>:imagehash <tag>:
    runner.invoke(tag_c, [str(PG_MNT) + ':' + old_head[:10], 'v1'])
    assert get_tagged_id(PG_MNT, 'v1') == old_head

    # sgr tag <mountpoint> with the same tag -- expect an error
    result = runner.invoke(tag_c, [str(PG_MNT), 'v1'])
    assert result.exit_code != 0
    assert 'Tag v1 already exists' in str(result.exc_info)

    # list tags
    result = runner.invoke(tag_c, [str(PG_MNT)])
    assert old_head[:12] + ': v1' in result.output
    assert new_head[:12] + ': HEAD, v2' in result.output

    # List tags on a single image
    result = runner.invoke(tag_c, [str(PG_MNT) + ':' + old_head[:20]])
    assert 'v1' in result.output
    assert 'HEAD, v2' not in result.output

    # Checkout by tag
    runner.invoke(checkout_c, [str(PG_MNT) + ':v1'])
    assert get_current_head(PG_MNT) == old_head

    # Checkout by hash
    runner.invoke(checkout_c, [str(PG_MNT) + ':' + new_head[:20]])
    assert get_current_head(PG_MNT) == new_head

    # Checkout with uncommitted changes
    runner.invoke(sql_c, ["INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')"])
    result = runner.invoke(checkout_c, [str(PG_MNT) + ':v1'])
    assert result.exit_code != 0
    assert "test/pg_mount has pending changes!" in str(result.exc_info)

    result = runner.invoke(checkout_c, [str(PG_MNT) + ':v1', '-f'])
    assert result.exit_code == 0
    assert not has_pending_changes(PG_MNT)

    # Delete the tag -- check the help entry correcting the command
    result = runner.invoke(tag_c, ['--remove', str(PG_MNT), 'v1'])
    assert result.exit_code != 0
    assert '--remove test/pg_mount:TAG_TO_DELETE' in result.output

    result = runner.invoke(tag_c, ['--remove', str(PG_MNT) + ':' + 'v1'])
    assert result.exit_code == 0
    assert get_tagged_id(PG_MNT, 'v1', raise_on_none=False) is None


def test_misc_mountpoint_management(sg_pg_mg_conn):
    runner = CliRunner()

    result = runner.invoke(status_c)
    assert str(PG_MNT) in result.output
    assert str(MG_MNT) in result.output

    # sgr rm -y test/pg_mount (no prompting)
    result = runner.invoke(rm_c, [str(MG_MNT), '-y'])
    assert result.exit_code == 0
    assert not repository_exists(MG_MNT)

    # sgr cleanup
    result = runner.invoke(cleanup_c)
    assert "Deleted 1 physical object(s)" in result.output

    # sgr init
    result = runner.invoke(init_c, ['output'])
    assert "Initialized empty repository output" in result.output
    assert repository_exists(OUTPUT)

    # sgr mount
    result = runner.invoke(mount_c, ['mongo_fdw', str(MG_MNT), '-c', 'originro:originpass@mongoorigin:27017',
                                     '-o', json.dumps({"stuff": {
            "db": "origindb",
            "coll": "stuff",
            "schema": {
                "name": "text",
                "duration": "numeric",
                "happy": "boolean"
            }}})])
    assert result.exit_code == 0
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT duration from test_mg_mount.stuff WHERE name = 'James'""")
        assert cur.fetchall() == [(Decimal(2),)]


def test_import(sg_pg_mg_conn):
    runner = CliRunner()
    head = get_current_head(PG_MNT)

    # sgr import mountpoint, table, target_mountpoint (3-arg)
    result = runner.invoke(import_c, [str(MG_MNT), 'stuff', str(PG_MNT)])
    assert result.exit_code == 0
    new_head = get_current_head(PG_MNT)
    assert table_exists_at(PG_MNT, 'stuff', new_head)
    assert not table_exists_at(PG_MNT, 'stuff', head)

    # sgr import with alias
    result = runner.invoke(import_c, [str(MG_MNT), 'stuff', str(PG_MNT), 'stuff_copy'])
    assert result.exit_code == 0
    new_new_head = get_current_head(PG_MNT)
    assert table_exists_at(PG_MNT, 'stuff_copy', new_new_head)
    assert not table_exists_at(PG_MNT, 'stuff_copy', new_head)

    # sgr import with alias and custom image hash
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("DELETE FROM test_mg_mount.stuff")
    new_mg_head = commit(MG_MNT)

    result = runner.invoke(import_c, [str(MG_MNT) + ':' + new_mg_head, 'stuff', str(PG_MNT), 'stuff_empty'])
    assert result.exit_code == 0
    new_new_new_head = get_current_head(PG_MNT)
    assert table_exists_at(PG_MNT, 'stuff_empty', new_new_new_head)
    assert not table_exists_at(PG_MNT, 'stuff_empty', new_new_head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("SELECT * FROM \"test/pg_mount\".stuff_empty")
        assert cur.fetchall() == []

    # sgr import with query, no alias
    result = runner.invoke(import_c, [str(MG_MNT) + ':' + new_mg_head, 'SELECT * FROM stuff', str(PG_MNT)])
    assert result.exit_code != 0
    assert 'TARGET_TABLE is required' in str(result.stdout)


def test_pull_push(empty_pg_conn, remote_driver_conn):
    runner = CliRunner()

    result = runner.invoke(clone_c, [str(PG_MNT)])
    assert result.exit_code == 0
    assert repository_exists(PG_MNT)

    with remote_driver_conn.cursor() as cur:
        cur.execute("INSERT INTO \"test/pg_mount\".fruits VALUES (3, 'mayonnaise')")
    with override_driver_connection(remote_driver_conn):
        remote_driver_head = commit(PG_MNT)

    result = runner.invoke(pull_c, [str(PG_MNT)])
    assert result.exit_code == 0
    checkout(PG_MNT, remote_driver_head)

    with empty_pg_conn.cursor() as cur:
        cur.execute("INSERT INTO \"test/pg_mount\".fruits VALUES (4, 'mustard')")
    local_head = commit(PG_MNT)

    with override_driver_connection(remote_driver_conn):
        assert not table_exists_at(PG_MNT, 'fruits', local_head)
    result = runner.invoke(push_c, [str(PG_MNT), '-h', 'DB'])
    assert result.exit_code == 0
    assert table_exists_at(PG_MNT, 'fruits', local_head)

    set_tag(PG_MNT, local_head, 'v1')
    empty_pg_conn.commit()
    result = runner.invoke(publish_c, [str(PG_MNT), 'v1', '-r', SPLITFILE_ROOT + 'README.md'])
    assert result.exit_code == 0
    with override_driver_connection(remote_driver_conn):
        image_hash, published_dt, deps, readme, schemata, previews = get_published_info(PG_MNT, 'v1')
    assert image_hash == local_head
    assert deps == []
    assert readme == "Test readme for a test dataset."
    assert schemata == {'fruits': [['fruit_id', 'integer', False],
                                   ['name', 'character varying', False]],
                        'vegetables': [['vegetable_id', 'integer', False],
                                       ['name', 'character varying', False]]}
    assert previews == {'fruits': [[1, 'apple'], [2, 'orange'], [3, 'mayonnaise'], [4, 'mustard']],
                        'vegetables': [[1, 'potato'], [2, 'carrot']]}


def test_splitfile(empty_pg_conn, remote_driver_conn):
    runner = CliRunner()

    result = runner.invoke(build_c, [SPLITFILE_ROOT + 'import_remote_multiple.splitfile',
                                    '-a', 'TAG', 'latest', '-o', 'output'])
    assert result.exit_code == 0
    with empty_pg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]

    # Test the sgr provenance command. First, just list the dependencies of the new image.
    result = runner.invoke(provenance_c, ['output:latest'])
    with override_driver_connection(remote_driver_conn):
        assert 'test/pg_mount:%s' % get_tagged_id(PG_MNT, 'latest') in result.output

    # Second, output the full splitfile (-f)
    result = runner.invoke(provenance_c, ['output:latest', '-f'])
    with override_driver_connection(remote_driver_conn):
        assert 'FROM test/pg_mount:%s IMPORT' % get_tagged_id(PG_MNT, 'latest') in result.output
    assert 'SQL CREATE TABLE join_table AS SELECT' in result.output


def test_splitfile_rebuild_update(empty_pg_conn, remote_driver_conn):
    add_multitag_dataset_to_remote_driver(remote_driver_conn)
    runner = CliRunner()

    result = runner.invoke(build_c, [SPLITFILE_ROOT + 'import_remote_multiple.splitfile',
                                    '-a', 'TAG', 'v1', '-o', 'output'])
    assert result.exit_code == 0

    # Rerun the output:latest against v2 of the test/pg_mount
    result = runner.invoke(rebuild_c, ['output:latest', '--against', 'test/pg_mount:v2'])
    output_v2 = get_current_head(OUTPUT)
    assert result.exit_code == 0
    with override_driver_connection(remote_driver_conn):
        v2 = get_tagged_id(PG_MNT, 'v2')
    assert provenance(OUTPUT, output_v2) == [(PG_MNT, v2)]

    # Now rerun the output:latest against the latest version of everything.
    # In this case, this should all resolve to the same version of test/pg_mount (v2) and not produce
    # any extra commits.
    curr_commits = get_all_image_info(OUTPUT)
    result = runner.invoke(rebuild_c, ['output:latest', '-u'])
    assert result.exit_code == 0
    assert output_v2 == get_current_head(OUTPUT)
    assert get_all_image_info(OUTPUT) == curr_commits


def test_mount_and_import(empty_pg_conn):
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
        assert table_exists_at(MG_MNT, 'stuff', get_current_head(MG_MNT))

        result = runner.invoke(import_c, ['tmp', 'SELECT * FROM stuff WHERE duration > 10', str(MG_MNT),
                                          'stuff_query'])
        assert result.exit_code == 0
        assert table_exists_at(MG_MNT, 'stuff_query', get_current_head(MG_MNT))
    finally:
        rm(to_repository('tmp'))


def test_rm_repositories(sg_pg_conn, remote_driver_conn):
    runner = CliRunner()

    # sgr rm test/pg_mount, say "no"
    result = runner.invoke(rm_c, [str(PG_MNT)], input='n\n')
    assert result.exit_code == 1
    assert "Repository test/pg_mount will be deleted" in result.output
    assert repository_exists(PG_MNT)

    # sgr rm test/pg_mount, say "yes"
    result = runner.invoke(rm_c, [str(PG_MNT)], input='y\n')
    assert result.exit_code == 0
    assert not repository_exists(PG_MNT)

    # sgr rm test/pg_mount -r remote_driver
    result = runner.invoke(rm_c, [str(PG_MNT), '-r', 'remote_driver'], input='y\n')
    assert result.exit_code == 0
    with override_driver_connection(remote_driver_conn):
        assert not repository_exists(PG_MNT)


def test_rm_images(sg_pg_conn, remote_driver_conn):
    runner = CliRunner()

    # Play around with both drivers for simplicity -- both have 2 images with 2 tags
    add_multitag_dataset_to_remote_driver(remote_driver_conn)
    add_multitag_dataset_to_remote_driver(sg_pg_conn)

    local_v1 = get_tagged_id(PG_MNT, 'v1')
    local_v2 = get_tagged_id(PG_MNT, 'v2')
    with override_driver_connection(remote_driver_conn):
        remote_v2 = get_tagged_id(PG_MNT, 'v2')

    # sgr rm test/pg_mount:v2, say "no"
    result = runner.invoke(rm_c, [str(PG_MNT) + ':v2'], input='n\n')
    assert result.exit_code == 1
    # Specify most of the output verbatim here to make sure it's not proposing
    # to delete more than needed (just the single image and the single v2 tag)
    assert "Images to be deleted:\n" + local_v2 + '\nTotal: 1\n\nTags to be deleted:\nv2\nTotal: 1' \
           in result.output
    # Since we cancelled the operation, 'v2' still remains.
    assert get_tagged_id(PG_MNT, 'v2') == local_v2
    assert get_image(PG_MNT, local_v2) is not None

    # sgr rm test/pg_mount:v2 -r remote_driver, say "yes"
    result = runner.invoke(rm_c, [str(PG_MNT) + ':v2', '-r', 'remote_driver'], input='y\n')
    assert result.exit_code == 0
    with override_driver_connection(remote_driver_conn):
        assert get_tagged_id(PG_MNT, 'v2', raise_on_none=False) is None
        assert get_image(PG_MNT, remote_v2) is None

    # sgr rm test/pg_mount:v1 -y
    # Should delete both images since v2 depends on v1
    result = runner.invoke(rm_c, [str(PG_MNT) + ':v1', '-y'])
    assert result.exit_code == 0
    assert local_v2 in result.output
    assert local_v1 in result.output
    assert 'v1' in result.output
    assert 'v2' in result.output
    assert len(get_all_image_info(PG_MNT)) == 0


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


def test_prune(sg_pg_conn, remote_driver_conn):
    runner = CliRunner()
    # Two drivers, two repos, two images in each (tagged v1 and v2, v1 is the parent of v2).
    add_multitag_dataset_to_remote_driver(remote_driver_conn)
    add_multitag_dataset_to_remote_driver(sg_pg_conn)

    # sgr prune test/pg_mount -- all images are tagged, nothing to do.
    result = runner.invoke(prune_c, [str(PG_MNT)])
    assert result.exit_code == 0
    assert "Nothing to do" in result.output

    # Delete tag v2 and run sgr prune -r remote_driver test/pg_mount, say "no": the image
    # that used to be 'v2' now isn't tagged so it will be a candidate for removal (but not the v1 image).
    with override_driver_connection(remote_driver_conn):
        remote_v2 = get_tagged_id(PG_MNT, 'v2')
        delete_tag(PG_MNT, 'v2')

        # Technically we shouldn't be able to do this, but we assume the remote is a bare repo and
        # doesn't actually have anything checked out.
        delete_tag(PG_MNT, 'HEAD')
        remote_driver_conn.commit()
    result = runner.invoke(prune_c, [str(PG_MNT), '-r', 'remote_driver'], input='n\n')
    assert result.exit_code == 1  # Because "n" aborted the command
    assert remote_v2 in result.output
    assert 'Total: 1' in result.output
    # Make sure the image still exists
    with override_driver_connection(remote_driver_conn):
        assert get_image(PG_MNT, remote_v2)

    # Delete tag v1 and run sgr prune -r remote_driver -y test_pg_mount:
    # now both images aren't tagged so will get removed.
    with override_driver_connection(remote_driver_conn):
        remote_v1 = get_tagged_id(PG_MNT, 'v1')
        delete_tag(PG_MNT, 'v1')
        remote_driver_conn.commit()
    result = runner.invoke(prune_c, [str(PG_MNT), '-r', 'remote_driver', '-y'])
    assert result.exit_code == 0
    assert remote_v2 in result.output
    assert remote_v1 in result.output
    assert 'Total: 2' in result.output
    with override_driver_connection(remote_driver_conn):
        assert not get_all_image_info(PG_MNT)

    # Finally, delete both tags from the local driver and prune. Since there's still
    # a HEAD tag pointing to the ex-v2, nothing will actually happen.
    result = runner.invoke(prune_c, [str(PG_MNT), '-y'])
    assert "Nothing to do." in result.output
    assert len(get_all_image_info(PG_MNT)) == 2
    assert len(get_all_hashes_tags(PG_MNT)) == 3


def test_config_dumping():
    runner = CliRunner()

    # sgr config (normal, with passwords shielded)
    result = runner.invoke(config_c)
    assert result.exit_code == 0
    assert PG_PWD not in result.output
    assert "remote_driver:" in result.output
    assert ("SG_DRIVER_USER=%s" % PG_USER) in result.output

    # sgr config -s (no password shielding)
    result = runner.invoke(config_c, ['-s'])
    assert result.exit_code == 0
    assert ("SG_DRIVER_USER=%s" % PG_USER) in result.output
    assert ("SG_DRIVER_PWD=%s" % PG_PWD) in result.output
    assert "remote_driver:" in result.output

    # sgr config -sc (no password shielding, output in config format)
    result = runner.invoke(config_c, ['-sc'])
    assert result.exit_code == 0
    assert ("SG_DRIVER_USER=%s" % PG_USER) in result.output
    assert ("SG_DRIVER_PWD=%s" % PG_PWD) in result.output
    assert "[remote: remote_driver]" in result.output
    assert "[defaults]" in result.output


def test_init_new_db():
    try:
        # CliRunner doesn't run in a brand new process and by that point PG_DB has propagated
        # through a few modules that are difficult to patch out, so let's just shell out.
        output = subprocess.check_output("SG_DRIVER_DB_NAME=testdb sgr init", shell=True, stderr=subprocess.STDOUT)
        output = output.decode('utf-8')
        assert "Creating database testdb" in output
        assert "Installing the audit trigger" in output
    finally:
        with psycopg2.connect(dbname=CONFIG['SG_DRIVER_POSTGRES_DB_NAME'],
                              user=CONFIG['SG_DRIVER_ADMIN_USER'],
                              password=CONFIG['SG_DRIVER_ADMIN_PWD'],
                              host=PG_HOST,
                              port=PG_PORT) as admin_conn:
            admin_conn.autocommit = True
            with admin_conn.cursor() as cur:
                cur.execute("DROP DATABASE IF EXISTS testdb")
