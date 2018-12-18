from splitgraph.commands import get_log
from splitgraph.commands.provenance import provenance, image_hash_to_splitfile
from splitgraph.commands.tagging import get_current_head, get_tagged_id
from splitgraph.engine import switch_engine
from splitgraph.splitfile import execute_commands
from splitgraph.splitfile.execution import rerun_image_with_replacement
from test.splitgraph.conftest import OUTPUT, PG_MNT, add_multitag_dataset_to_engine, load_splitfile, REMOTE_ENGINE


def test_provenance(local_engine_empty, remote_engine):
    add_multitag_dataset_to_engine(remote_engine)
    execute_commands(load_splitfile('import_remote_multiple.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    dependencies = provenance(OUTPUT, get_current_head(OUTPUT))

    with switch_engine(REMOTE_ENGINE):
        assert dependencies == [(PG_MNT, get_tagged_id(PG_MNT, 'v1'))]


def test_provenance_with_from(local_engine_empty, remote_engine):
    add_multitag_dataset_to_engine(remote_engine)
    execute_commands(load_splitfile('from_remote.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    dependencies = provenance(OUTPUT, get_current_head(OUTPUT))

    with switch_engine(REMOTE_ENGINE):
        assert dependencies == [(PG_MNT, get_tagged_id(PG_MNT, 'v1'))]


def test_splitfile_recreate(local_engine_empty, remote_engine):
    add_multitag_dataset_to_engine(remote_engine)
    execute_commands(load_splitfile('import_with_custom_query_and_sql.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    recreated_commands = image_hash_to_splitfile(OUTPUT, get_current_head(OUTPUT), err_on_end=False)
    with switch_engine(REMOTE_ENGINE):
        assert recreated_commands == ["FROM test/pg_mount:%s IMPORT {SELECT * FROM fruits WHERE name = 'orange'} AS " \
                                      % get_tagged_id(PG_MNT, 'v1') +
                                      "my_fruits, {SELECT * FROM vegetables WHERE name LIKE '%o'} AS o_vegetables, "
                                      "vegetables AS vegetables, fruits AS all_fruits",
                                      "SQL CREATE TABLE test_table AS SELECT * FROM all_fruits"]


def test_splitfile_recreate_custom_from(local_engine_empty, remote_engine):
    add_multitag_dataset_to_engine(remote_engine)
    execute_commands(load_splitfile('from_remote.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    recreated_commands = image_hash_to_splitfile(OUTPUT, get_current_head(OUTPUT), err_on_end=False)

    # Parser strips newlines in sql but not the whitespace, so we have to reproduce the query here verbatim.
    with switch_engine(REMOTE_ENGINE):
        assert recreated_commands == ["FROM test/pg_mount:%s" % get_tagged_id(PG_MNT, 'v1'),
                                      "SQL CREATE TABLE join_table AS SELECT fruit_id AS id, fruits.name AS fruit, "
                                      "vegetables.name AS vegetable                                 FROM fruits "
                                      "JOIN vegetables                                ON fruit_id = vegetable_id"]


def test_splitfile_incomplete_provenance(local_engine_empty, remote_engine):
    # This splitfile has a MOUNT as its first command. We're expecting image_hash_to_splitfile to base the result
    # on the image created by MOUNT and not regenerate the MOUNT command.
    add_multitag_dataset_to_engine(remote_engine)
    execute_commands(load_splitfile('import_from_mounted_db_with_sql.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    head = get_current_head(OUTPUT)
    image_with_mount = get_log(OUTPUT, head)[-2]
    recreated_commands = image_hash_to_splitfile(OUTPUT, head, err_on_end=False)

    assert recreated_commands == ["FROM output:%s" % image_with_mount,
                                  "SQL CREATE TABLE new_table AS SELECT * FROM all_fruits"]


def test_rerun_with_new_version(local_engine_empty, remote_engine):
    add_multitag_dataset_to_engine(remote_engine)
    # Test running commands that base new datasets on a remote repository.
    execute_commands(load_splitfile('from_remote.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)

    output_v1 = get_current_head(OUTPUT)
    # Do a logical rebase of the newly created image on the V2 of the remote repository

    rerun_image_with_replacement(OUTPUT, output_v1, {PG_MNT: 'v2'})
    output_v2 = get_current_head(OUTPUT)
    assert local_engine_empty.run_sql("SELECT * FROM output.join_table") == [(2, 'orange', 'carrot')]

    # Do some checks on the structure of the final output repo. In particular, make sure that the two derived versions
    # still exist and depend only on the respective tags of the source.
    with switch_engine(REMOTE_ENGINE):
        v1 = get_tagged_id(PG_MNT, 'v1')
        v2 = get_tagged_id(PG_MNT, 'v2')
    assert provenance(OUTPUT, output_v1) == [(PG_MNT, v1)]
    assert provenance(OUTPUT, output_v2) == [(PG_MNT, v2)]

    ov1_log = get_log(OUTPUT, output_v1)
    ov2_log = get_log(OUTPUT, output_v2)

    # ov1_log: CREATE TABLE commit, then FROM v1
    # ov2_log: CREATE TABLE commit, then FROM v2 which is based on FROM v1 (since we cloned both from test/pg_mount),
    # then as previously.
    assert ov1_log[1:] == ov2_log[2:]


def test_rerun_with_from_import(local_engine_empty, remote_engine):
    add_multitag_dataset_to_engine(remote_engine)
    execute_commands(load_splitfile('import_remote_multiple.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)

    output_v1 = get_current_head(OUTPUT)
    # Do a logical rebase of the newly created image on the V2 of the remote repository

    rerun_image_with_replacement(OUTPUT, output_v1, {PG_MNT: 'v2'})
    output_v2 = get_current_head(OUTPUT)

    # Do some checks on the structure of the final output repo. In particular, make sure that the two derived versions
    # still exist and depend only on the respective tags of the source.
    with switch_engine(REMOTE_ENGINE):
        v1 = get_tagged_id(PG_MNT, 'v1')
        v2 = get_tagged_id(PG_MNT, 'v2')
    assert provenance(OUTPUT, output_v1) == \
           [(PG_MNT, v1)]
    assert provenance(OUTPUT, output_v2) == \
           [(PG_MNT, v2)]

    ov1_log = get_log(OUTPUT, output_v1)
    ov2_log = get_log(OUTPUT, output_v2)

    # ov1_log: CREATE TABLE commit, then IMPORT from v1, then the 00000 commit
    # ov2_log: CREATE TABLE commit, then FROM v1, then the 00000.. commit
    assert ov1_log[2:] == ov2_log[2:]
    assert len(ov1_log) == 3
