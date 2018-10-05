import json
from decimal import Decimal

from click.testing import CliRunner
from splitgraph.commandline import status_c, sql_c, diff_c, commit_c, log_c, show_c, tag_c, checkout_c, unmount_c, \
    cleanup_c, init_c, mount_c, import_c, clone_c, pull_c, push_c, file_c, provenance_c, rerun_c, publish_c
from splitgraph.commands import commit, checkout
from splitgraph.commands.misc import table_exists_at
from splitgraph.commands.provenance import provenance
from splitgraph.meta_handler.images import get_image_parent, get_all_images_parents
from splitgraph.meta_handler.misc import mountpoint_exists
from splitgraph.meta_handler.tables import get_table
from splitgraph.meta_handler.tags import get_current_head, get_tagged_id, set_tag
from splitgraph.registry_meta_handler import get_published_info
from test.splitgraph.conftest import PG_MNT, MG_MNT
from test.splitgraph.test_sgfile import SGFILE_ROOT, _add_multitag_dataset_to_snapper


def test_commandline_basics(sg_pg_mg_conn):
    runner = CliRunner()

    # sgr status
    result = runner.invoke(status_c, [])
    assert PG_MNT in result.output
    assert MG_MNT in result.output
    old_head = get_current_head(sg_pg_mg_conn, PG_MNT)
    assert old_head in result.output

    # sgr sql
    runner.invoke(sql_c, ["INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')"])
    runner.invoke(sql_c, ["CREATE TABLE test_pg_mount.mushrooms (mushroom_id integer, name varchar)"])
    runner.invoke(sql_c, ["DROP TABLE test_pg_mount.vegetables"])
    runner.invoke(sql_c, ["DELETE FROM test_pg_mount.fruits WHERE fruit_id = 1"])
    result = runner.invoke(sql_c, ["SELECT * FROM test_pg_mount.fruits"])
    assert "(3, 'mayonnaise')" in result.output
    assert "(1, 'apple')" not in result.output

    def check_diff(args):
        result = runner.invoke(diff_c, args)
        assert "added 1 rows" in result.output
        assert "removed 1 rows" in result.output
        assert "vegetables: table removed"
        assert "mushrooms: table added"
        result = runner.invoke(diff_c, args + ['-v'])
        assert "(3, 'mayonnaise'): +" in result.output
        assert "(1, 'apple'): -" in result.output

    # sgr diff, HEAD -> current staging (0-param)
    check_diff([PG_MNT])

    # sgr commit (with an extra snapshot
    result = runner.invoke(commit_c, [PG_MNT, '-m', 'Test commit', '-s'])
    new_head = get_current_head(sg_pg_mg_conn, PG_MNT)
    assert new_head != old_head
    assert get_image_parent(sg_pg_mg_conn, PG_MNT, new_head) == old_head
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
    result = runner.invoke(status_c, [PG_MNT])
    assert 'test_pg_mount' in result.output
    assert 'Parent: ' + old_head in result.output
    assert new_head in result.output

    # sgr log
    result = runner.invoke(log_c, [PG_MNT])
    assert old_head in result.output
    assert new_head in result.output
    assert "Test commit" in result.output

    # sgr log (tree)
    result = runner.invoke(log_c, [PG_MNT, '-t'])
    assert old_head[:5] in result.output
    assert new_head[:5] in result.output

    # sgr show the new commit
    result = runner.invoke(show_c, [PG_MNT, new_head[:20], '-v'])
    assert "Test commit" in result.output
    assert "Parent: " + old_head in result.output
    fruit_objs = get_table(sg_pg_mg_conn, PG_MNT, 'fruits', new_head[:20])
    mushroom_objs = get_table(sg_pg_mg_conn, PG_MNT, 'mushrooms', new_head[:20])

    # Check verbose show also has the actual object IDs
    for o, of in fruit_objs + mushroom_objs:
        assert o in result.output


def test_commandline_tag_checkout(sg_pg_mg_conn):
    runner = CliRunner()
    # Do the quick setting up with the same commit structure
    old_head = get_current_head(sg_pg_mg_conn, PG_MNT)
    runner.invoke(sql_c, ["INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')"])
    runner.invoke(sql_c, ["CREATE TABLE test_pg_mount.mushrooms (mushroom_id integer, name varchar)"])
    runner.invoke(sql_c, ["DROP TABLE test_pg_mount.vegetables"])
    runner.invoke(sql_c, ["DELETE FROM test_pg_mount.fruits WHERE fruit_id = 1"])
    runner.invoke(sql_c, ["SELECT * FROM test_pg_mount.fruits"])
    runner.invoke(commit_c, [PG_MNT, '-m', 'Test commit'])
    new_head = get_current_head(sg_pg_mg_conn, PG_MNT)

    # sgr tag <mountpoint> <tag>: tags the current HEAD
    runner.invoke(tag_c, [PG_MNT, 'v2'])
    assert get_tagged_id(sg_pg_mg_conn, PG_MNT, 'v2') == new_head

    # sgr tag <mountpoint> <tag> -i (imagehash):
    runner.invoke(tag_c, [PG_MNT, 'v1', '-i', old_head[:10]])
    assert get_tagged_id(sg_pg_mg_conn, PG_MNT, 'v1') == old_head

    # sgr tag <mountpoint> with the same tag -- expect an error
    result = runner.invoke(tag_c, [PG_MNT, 'v1'])
    assert result.exit_code != 0
    assert 'Tag v1 already exists' in str(result.exc_info)

    # list tags
    result = runner.invoke(tag_c, [PG_MNT])
    assert old_head[:12] + ': v1' in result.output
    assert new_head[:12] + ': HEAD, v2' in result.output

    # List tags on a single image
    result = runner.invoke(tag_c, [PG_MNT, '-i', old_head[:20]])
    assert 'v1' in result.output
    assert 'HEAD, v2' not in result.output

    # Checkout by tag
    runner.invoke(checkout_c, [PG_MNT, 'v1'])
    assert get_current_head(sg_pg_mg_conn, PG_MNT) == old_head

    # Checkout by hash
    runner.invoke(checkout_c, [PG_MNT, new_head[:20]])
    assert get_current_head(sg_pg_mg_conn, PG_MNT) == new_head


def test_misc_mountpoint_management(sg_pg_mg_conn):
    runner = CliRunner()

    result = runner.invoke(status_c)
    assert PG_MNT in result.output
    assert MG_MNT in result.output

    # sgr unmount
    result = runner.invoke(unmount_c, [MG_MNT])
    assert result.exit_code == 0
    assert not mountpoint_exists(sg_pg_mg_conn, MG_MNT)

    # sgr cleanup
    result = runner.invoke(cleanup_c)
    assert "Deleted 1 physical object(s)" in result.output

    # sgr init
    result = runner.invoke(init_c, ['output'])
    assert "Initialized empty mountpoint output" in result.output
    assert mountpoint_exists(sg_pg_mg_conn, 'output')

    # sgr mount
    result = runner.invoke(mount_c, [MG_MNT, '-c', 'originro:originpass@mongoorigin:27017',
                                     '-h', 'mongo_fdw', '-o', json.dumps({"stuff": {
            "db": "origindb",
            "coll": "stuff",
            "schema": {
                "name": "text",
                "duration": "numeric",
                "happy": "boolean"
            }}})])
    assert result.exit_code == 0
    assert mountpoint_exists(sg_pg_mg_conn, MG_MNT)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("""SELECT duration from test_mg_mount.stuff WHERE name = 'James'""")
        assert cur.fetchall() == [(Decimal(2),)]


def test_import(sg_pg_mg_conn):
    runner = CliRunner()
    head = get_current_head(sg_pg_mg_conn, PG_MNT)

    # sgr import mountpoint, table, target_mountpoint (3-arg)
    result = runner.invoke(import_c, [MG_MNT, 'stuff', PG_MNT])
    assert result.exit_code == 0
    new_head = get_current_head(sg_pg_mg_conn, PG_MNT)
    assert table_exists_at(sg_pg_mg_conn, PG_MNT, 'stuff', new_head)
    assert not table_exists_at(sg_pg_mg_conn, PG_MNT, 'stuff', head)

    # sgr import with alias
    result = runner.invoke(import_c, [MG_MNT, 'stuff', PG_MNT, 'stuff_copy'])
    assert result.exit_code == 0
    new_new_head = get_current_head(sg_pg_mg_conn, PG_MNT)
    assert table_exists_at(sg_pg_mg_conn, PG_MNT, 'stuff_copy', new_new_head)
    assert not table_exists_at(sg_pg_mg_conn, PG_MNT, 'stuff_copy', new_head)

    # sgr import with alias and custom image hash
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("DELETE FROM test_mg_mount.stuff")
    new_mg_head = commit(sg_pg_mg_conn, MG_MNT)

    result = runner.invoke(import_c, [MG_MNT, 'stuff', PG_MNT, 'stuff_empty', new_mg_head])
    assert result.exit_code == 0
    new_new_new_head = get_current_head(sg_pg_mg_conn, PG_MNT)
    assert table_exists_at(sg_pg_mg_conn, PG_MNT, 'stuff_empty', new_new_new_head)
    assert not table_exists_at(sg_pg_mg_conn, PG_MNT, 'stuff_empty', new_new_head)
    with sg_pg_mg_conn.cursor() as cur:
        cur.execute("SELECT * FROM test_pg_mount.stuff_empty")
        assert cur.fetchall() == []


def test_pull_push(empty_pg_conn, snapper_conn):
    runner = CliRunner()

    result = runner.invoke(clone_c, [PG_MNT])
    assert result.exit_code == 0
    assert mountpoint_exists(empty_pg_conn, PG_MNT)

    with snapper_conn.cursor() as cur:
        cur.execute("INSERT INTO test_pg_mount.fruits VALUES (3, 'mayonnaise')")
    snapper_head = commit(snapper_conn, PG_MNT)

    result = runner.invoke(pull_c, [PG_MNT, 'origin'])
    assert result.exit_code == 0
    checkout(empty_pg_conn, PG_MNT, snapper_head)

    with empty_pg_conn.cursor() as cur:
        cur.execute("INSERT INTO test_pg_mount.fruits VALUES (4, 'mustard')")
    local_head = commit(empty_pg_conn, PG_MNT)

    assert not table_exists_at(snapper_conn, PG_MNT, 'fruits', local_head)
    result = runner.invoke(push_c, [PG_MNT, 'origin', '-h', 'DB'])
    assert result.exit_code == 0
    assert table_exists_at(snapper_conn, PG_MNT, 'fruits', local_head)

    set_tag(empty_pg_conn, PG_MNT, local_head, 'v1')
    empty_pg_conn.commit()
    result = runner.invoke(publish_c, [PG_MNT, 'v1', '-r', SGFILE_ROOT + 'README.md'])
    assert result.exit_code == 0
    image_hash, published_dt, deps, readme, schemata, previews = get_published_info(snapper_conn, PG_MNT, 'v1')
    assert image_hash == local_head
    assert deps == []
    assert readme == "Test readme for a test dataset."
    assert schemata == {'fruits': [['fruit_id', 'integer', False],
                                   ['name', 'character varying', False]],
                        'vegetables': [['vegetable_id', 'integer', False],
                                       ['name', 'character varying', False]]}
    assert previews == {'fruits': [[1, 'apple'], [2, 'orange'], [3, 'mayonnaise'], [4, 'mustard']],
                        'vegetables': [[1, 'potato'], [2, 'carrot']]}


def test_sgfile(empty_pg_conn, snapper_conn):
    runner = CliRunner()

    result = runner.invoke(file_c, [SGFILE_ROOT + 'import_remote_multiple.sgfile',
                                    '-a', 'TAG', 'latest', '-o', 'output'])
    assert result.exit_code == 0
    with empty_pg_conn.cursor() as cur:
        cur.execute("""SELECT id, fruit, vegetable FROM output.join_table""")
        assert cur.fetchall() == [(1, 'apple', 'potato'), (2, 'orange', 'carrot')]

    # Test the sgr provenance command. First, just list the dependencies of the new image.
    result = runner.invoke(provenance_c, ['output', 'latest'])
    assert 'test_pg_mount:%s' % get_tagged_id(snapper_conn, 'test_pg_mount', 'latest') in result.output

    # Second, output the full sgfile (-f)
    result = runner.invoke(provenance_c, ['output', 'latest', '-f'])
    assert 'FROM test_pg_mount:%s IMPORT' % get_tagged_id(snapper_conn, 'test_pg_mount', 'latest') in result.output
    assert 'SQL CREATE TABLE join_table AS SELECT' in result.output


def test_sgfile_rerun_update(empty_pg_conn, snapper_conn):
    _add_multitag_dataset_to_snapper(snapper_conn)
    runner = CliRunner()

    result = runner.invoke(file_c, [SGFILE_ROOT + 'import_remote_multiple.sgfile',
                                    '-a', 'TAG', 'v1', '-o', 'output'])
    assert result.exit_code == 0

    # Rerun the output:latest against v2 of the test_pg_mount
    result = runner.invoke(rerun_c, ['output', 'latest', '-i', 'test_pg_mount', 'v2'])
    output_v2 = get_current_head(empty_pg_conn, 'output')
    assert result.exit_code == 0
    assert provenance(empty_pg_conn, 'output', output_v2) == \
           [('test_pg_mount', get_tagged_id(snapper_conn, 'test_pg_mount', 'v2'))]

    # Now rerun the output:latest against the latest version of everything.
    # In this case, this should all resolve to the same version of test_pg_mount (v2) and not produce
    # any extra commits.
    curr_commits = get_all_images_parents(empty_pg_conn, 'output')
    result = runner.invoke(rerun_c, ['output', 'latest', '-u'])
    assert result.exit_code == 0
    assert output_v2 == get_current_head(empty_pg_conn, 'output')
    assert get_all_images_parents(empty_pg_conn, 'output') == curr_commits
