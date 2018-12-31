from splitgraph.splitfile import execute_commands
from splitgraph.splitfile.execution import rebuild_image
from test.splitgraph.conftest import OUTPUT, load_splitfile


def test_provenance(local_engine_empty, pg_repo_remote_multitag):
    execute_commands(load_splitfile('import_remote_multiple.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    dependencies = OUTPUT.head.provenance()

    assert dependencies == [(pg_repo_remote_multitag,
                             pg_repo_remote_multitag.images['v1'].image_hash)]


def test_provenance_with_from(local_engine_empty, pg_repo_remote_multitag):
    execute_commands(load_splitfile('from_remote.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    dependencies = OUTPUT.head.provenance()

    assert dependencies == [(pg_repo_remote_multitag,
                             pg_repo_remote_multitag.images['v1'].image_hash)]


def test_splitfile_recreate(local_engine_empty, pg_repo_remote_multitag):
    execute_commands(load_splitfile('import_with_custom_query_and_sql.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    recreated_commands = OUTPUT.head.to_splitfile(err_on_end=False)
    assert recreated_commands == ["FROM test/pg_mount:%s IMPORT {SELECT * FROM fruits WHERE name = 'orange'} AS " \
                                  % pg_repo_remote_multitag.images['v1'].image_hash +
                                  "my_fruits, {SELECT * FROM vegetables WHERE name LIKE '%o'} AS o_vegetables, "
                                  "vegetables AS vegetables, fruits AS all_fruits",
                                  "SQL CREATE TABLE test_table AS SELECT * FROM all_fruits"]


def test_splitfile_recreate_custom_from(local_engine_empty, pg_repo_remote_multitag):
    execute_commands(load_splitfile('from_remote.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    recreated_commands = OUTPUT.head.to_splitfile(err_on_end=False)

    # Parser strips newlines in sql but not the whitespace, so we have to reproduce the query here verbatim.
    assert recreated_commands == ["FROM test/pg_mount:%s" % pg_repo_remote_multitag.images['v1'].image_hash,
                                  "SQL CREATE TABLE join_table AS SELECT fruit_id AS id, fruits.name AS fruit, "
                                  "vegetables.name AS vegetable                                 FROM fruits "
                                  "JOIN vegetables                                ON fruit_id = vegetable_id"]


def test_splitfile_incomplete_provenance(local_engine_empty, pg_repo_remote_multitag):
    # This splitfile has a MOUNT as its first command. We're expecting image_hash_to_splitfile to base the result
    # on the image created by MOUNT and not regenerate the MOUNT command.
    execute_commands(load_splitfile('import_from_mounted_db_with_sql.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    head_img = OUTPUT.head
    image_with_mount = head_img.get_log()[-2]
    recreated_commands = head_img.to_splitfile(err_on_end=False)

    assert recreated_commands == ["FROM output:%s" % image_with_mount.image_hash,
                                  "SQL CREATE TABLE new_table AS SELECT * FROM all_fruits"]


def test_rerun_with_new_version(local_engine_empty, pg_repo_remote_multitag):
    # Test running commands that base new datasets on a remote repository.
    execute_commands(load_splitfile('from_remote.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)

    output_v1 = OUTPUT.head
    # Do a logical rebase of the newly created image on the V2 of the remote repository

    rebuild_image(output_v1, {pg_repo_remote_multitag: 'v2'})
    output_v2 = OUTPUT.head
    assert local_engine_empty.run_sql("SELECT * FROM output.join_table") == [(2, 'orange', 'carrot')]

    # Do some checks on the structure of the final output repo. In particular, make sure that the two derived versions
    # still exist and depend only on the respective tags of the source.
    v1 = pg_repo_remote_multitag.images['v1']
    v2 = pg_repo_remote_multitag.images['v2']
    assert output_v1.provenance() == [(pg_repo_remote_multitag, v1.image_hash)]
    assert output_v2.provenance() == [(pg_repo_remote_multitag, v2.image_hash)]

    ov1_log = output_v1.get_log()
    ov2_log = output_v2.get_log()

    # ov1_log: CREATE TABLE commit, then FROM v1
    # ov2_log: CREATE TABLE commit, then FROM v2 which is based on FROM v1 (since we cloned both from test/pg_mount),
    # then as previously.
    assert ov1_log[1:] == ov2_log[2:]


def test_rerun_with_from_import(local_engine_empty, pg_repo_remote_multitag):
    execute_commands(load_splitfile('import_remote_multiple.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)

    output_v1 = OUTPUT.head
    # Do a logical rebase of the newly created image on the V2 of the remote repository

    rebuild_image(output_v1, {pg_repo_remote_multitag: 'v2'})
    output_v2 = OUTPUT.head

    # Do some checks on the structure of the final output repo. In particular, make sure that the two derived versions
    # still exist and depend only on the respective tags of the source.
    v1 = pg_repo_remote_multitag.images['v1']
    v2 = pg_repo_remote_multitag.images['v2']
    assert output_v1.provenance() == [(pg_repo_remote_multitag, v1.image_hash)]
    assert output_v2.provenance() == [(pg_repo_remote_multitag, v2.image_hash)]

    ov1_log = output_v1.get_log()
    ov2_log = output_v2.get_log()

    # ov1_log: CREATE TABLE commit, then IMPORT from v1, then the 00000 commit
    # ov2_log: CREATE TABLE commit, then FROM v1, then the 00000.. commit
    assert ov1_log[2:] == ov2_log[2:]
    assert len(ov1_log) == 3
