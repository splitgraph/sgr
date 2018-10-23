from splitgraph.commands import get_log
from splitgraph.commands.provenance import provenance, image_hash_to_sgfile
from splitgraph.meta_handler.tags import get_current_head, get_tagged_id
from splitgraph.sgfile import execute_commands
from splitgraph.sgfile.execution import rerun_image_with_replacement
from test.splitgraph.conftest import OUTPUT, PG_MNT
from test.splitgraph.test_sgfile import _load_sgfile, _add_multitag_dataset_to_remote_driver


def test_provenance(empty_pg_conn, remote_driver_conn):
    new_head = _add_multitag_dataset_to_remote_driver(remote_driver_conn)
    execute_commands(empty_pg_conn, _load_sgfile('import_remote_multiple.sgfile'), params={'TAG': 'v1'},
                     output=OUTPUT)
    dependencies = provenance(empty_pg_conn, OUTPUT, get_current_head(empty_pg_conn, OUTPUT))

    assert dependencies == [(PG_MNT, get_tagged_id(remote_driver_conn, PG_MNT, 'v1'))]


def test_provenance_with_from(empty_pg_conn, remote_driver_conn):
    new_head = _add_multitag_dataset_to_remote_driver(remote_driver_conn)
    execute_commands(empty_pg_conn, _load_sgfile('from_remote.sgfile'), params={'TAG': 'v1'}, output=OUTPUT)
    dependencies = provenance(empty_pg_conn, OUTPUT, get_current_head(empty_pg_conn, OUTPUT))

    assert dependencies == [(PG_MNT, get_tagged_id(remote_driver_conn, PG_MNT, 'v1'))]


def test_sgfile_recreate(empty_pg_conn, remote_driver_conn):
    new_head = _add_multitag_dataset_to_remote_driver(remote_driver_conn)
    execute_commands(empty_pg_conn, _load_sgfile('import_with_custom_query_and_sql.sgfile'), params={'TAG': 'v1'},
                     output=OUTPUT)
    recreated_commands = image_hash_to_sgfile(empty_pg_conn, OUTPUT, get_current_head(empty_pg_conn, OUTPUT),
                                              err_on_end=False)

    assert recreated_commands == ["FROM test_pg_mount:%s IMPORT {SELECT * FROM fruits WHERE name = 'orange'} AS " \
                                  % get_tagged_id(remote_driver_conn, PG_MNT, 'v1') +
                                  "my_fruits, {SELECT * FROM vegetables WHERE name LIKE '%o'} AS o_vegetables, "
                                  "vegetables AS vegetables, fruits AS all_fruits",
                                  "SQL CREATE TABLE test_table AS SELECT * FROM all_fruits"]


def test_sgfile_recreate_custom_from(empty_pg_conn, remote_driver_conn):
    _add_multitag_dataset_to_remote_driver(remote_driver_conn)
    execute_commands(empty_pg_conn, _load_sgfile('from_remote.sgfile'), params={'TAG': 'v1'}, output=OUTPUT)
    recreated_commands = image_hash_to_sgfile(empty_pg_conn, OUTPUT, get_current_head(empty_pg_conn, OUTPUT),
                                              err_on_end=False)

    # Parser strips newlines in sql but not the whitespace, so we have to reproduce the query here verbatim.
    assert recreated_commands == ["FROM test_pg_mount:%s" % get_tagged_id(remote_driver_conn, PG_MNT, 'v1'),
                                  "SQL CREATE TABLE join_table AS SELECT fruit_id AS id, fruits.name AS fruit, "
                                  "vegetables.name AS vegetable                                 FROM fruits "
                                  "JOIN vegetables                                ON fruit_id = vegetable_id"]


def test_sgfile_incomplete_provenance(empty_pg_conn, remote_driver_conn):
    # This sgfile has a MOUNT as its first command. We're expecting image_hash_to_sgfile to base the result
    # on the image created by MOUNT and not regenerate the MOUNT command.
    _add_multitag_dataset_to_remote_driver(remote_driver_conn)
    execute_commands(empty_pg_conn, _load_sgfile('import_from_mounted_db_with_sql.sgfile'),
                     params={'TAG': 'v1'}, output=OUTPUT)
    head = get_current_head(empty_pg_conn, OUTPUT)
    image_with_mount = get_log(empty_pg_conn, OUTPUT, head)[-2]
    recreated_commands = image_hash_to_sgfile(empty_pg_conn, OUTPUT, head, err_on_end=False)

    assert recreated_commands == ["FROM output:%s" % image_with_mount,
                                  "SQL CREATE TABLE new_table AS SELECT * FROM all_fruits"]


def test_rerun_with_new_version(empty_pg_conn, remote_driver_conn):
    _add_multitag_dataset_to_remote_driver(remote_driver_conn)
    # Test running commands that base new datasets on a remote repository.
    execute_commands(empty_pg_conn, _load_sgfile('from_remote.sgfile'), output=OUTPUT, params={'TAG': 'v1'})

    output_v1 = get_current_head(empty_pg_conn, OUTPUT)
    # Do a logical rebase of the newly created image on the V2 of the remote repository

    rerun_image_with_replacement(empty_pg_conn, OUTPUT, output_v1, {PG_MNT: 'v2'})
    output_v2 = get_current_head(empty_pg_conn, OUTPUT)
    with empty_pg_conn.cursor() as cur:
        cur.execute("SELECT * FROM output.join_table")
        assert cur.fetchall() == [(2, 'orange', 'carrot')]

    # Do some checks on the structure of the final output repo. In particular, make sure that the two derived versions
    # still exist and depend only on the respective tags of the source.
    assert provenance(empty_pg_conn, OUTPUT, output_v1) == \
           [(PG_MNT, get_tagged_id(remote_driver_conn, PG_MNT, 'v1'))]
    assert provenance(empty_pg_conn, OUTPUT, output_v2) == \
           [(PG_MNT, get_tagged_id(remote_driver_conn, PG_MNT, 'v2'))]

    ov1_log = get_log(empty_pg_conn, OUTPUT, output_v1)
    ov2_log = get_log(empty_pg_conn, OUTPUT, output_v2)

    # ov1_log: CREATE TABLE commit, then FROM v1
    # ov2_log: CREATE TABLE commit, then FROM v2 which is based on FROM v1 (since we cloned both from test_pg_mount),
    # then as previously.
    assert ov1_log[1:] == ov2_log[2:]


def test_rerun_with_from_import(empty_pg_conn, remote_driver_conn):
    _add_multitag_dataset_to_remote_driver(remote_driver_conn)
    execute_commands(empty_pg_conn, _load_sgfile('import_remote_multiple.sgfile'), output=OUTPUT, params={'TAG': 'v1'})

    output_v1 = get_current_head(empty_pg_conn, OUTPUT)
    # Do a logical rebase of the newly created image on the V2 of the remote repository

    rerun_image_with_replacement(empty_pg_conn, OUTPUT, output_v1, {PG_MNT: 'v2'})
    output_v2 = get_current_head(empty_pg_conn, OUTPUT)

    # Do some checks on the structure of the final output repo. In particular, make sure that the two derived versions
    # still exist and depend only on the respective tags of the source.
    assert provenance(empty_pg_conn, OUTPUT, output_v1) == \
           [(PG_MNT, get_tagged_id(remote_driver_conn, PG_MNT, 'v1'))]
    assert provenance(empty_pg_conn, OUTPUT, output_v2) == \
           [(PG_MNT, get_tagged_id(remote_driver_conn, PG_MNT, 'v2'))]

    ov1_log = get_log(empty_pg_conn, OUTPUT, output_v1)
    ov2_log = get_log(empty_pg_conn, OUTPUT, output_v2)

    # ov1_log: CREATE TABLE commit, then IMPORT from v1, then the 00000 commit
    # ov2_log: CREATE TABLE commit, then FROM v1, then the 00000.. commit
    assert ov1_log[2:] == ov2_log[2:]
    assert len(ov1_log) == 3
