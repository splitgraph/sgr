from splitgraph.commands import get_log
from splitgraph.commands.provenance import provenance, image_hash_to_sgfile
from splitgraph.meta_handler import get_current_head, get_tagged_id
from splitgraph.sgfile import execute_commands
from test.splitgraph.test_sgfile import _load_sgfile, _add_multitag_dataset_to_snapper


def test_provenance(empty_pg_conn, snapper_conn):
    new_head = _add_multitag_dataset_to_snapper(snapper_conn)
    execute_commands(empty_pg_conn, _load_sgfile('import_remote_multiple.sgfile'), params={'TAG': 'v1'},
                     output='output')
    dependencies = provenance(empty_pg_conn, 'output', get_current_head(empty_pg_conn, 'output'))

    assert dependencies == [('test_pg_mount', get_tagged_id(snapper_conn, 'test_pg_mount', 'v1'))]


def test_provenance_with_from(empty_pg_conn, snapper_conn):
    new_head = _add_multitag_dataset_to_snapper(snapper_conn)
    execute_commands(empty_pg_conn, _load_sgfile('from_remote.sgfile'), params={'TAG': 'v1'}, output='output')
    dependencies = provenance(empty_pg_conn, 'output', get_current_head(empty_pg_conn, 'output'))

    assert dependencies == [('test_pg_mount', get_tagged_id(snapper_conn, 'test_pg_mount', 'v1'))]


def test_sgfile_recreate(empty_pg_conn, snapper_conn):
    new_head = _add_multitag_dataset_to_snapper(snapper_conn)
    execute_commands(empty_pg_conn, _load_sgfile('import_with_custom_query_and_sql.sgfile'), params={'TAG': 'v1'},
                     output='output')
    recreated_commands = image_hash_to_sgfile(empty_pg_conn, 'output', get_current_head(empty_pg_conn, 'output'),
                                              err_on_end=False)

    assert recreated_commands == ["FROM test_pg_mount:%s IMPORT {SELECT * FROM fruits WHERE name = 'orange'} AS " \
                                  % get_tagged_id(snapper_conn, 'test_pg_mount', 'v1') +
                                  "my_fruits, {SELECT * FROM vegetables WHERE name LIKE '%o'} AS o_vegetables, "
                                  "vegetables AS vegetables, fruits AS all_fruits",
                                  "SQL CREATE TABLE test_table AS SELECT * FROM all_fruits"]


def test_sgfile_recreate_custom_from(empty_pg_conn, snapper_conn):
    _add_multitag_dataset_to_snapper(snapper_conn)
    execute_commands(empty_pg_conn, _load_sgfile('from_remote.sgfile'), params={'TAG': 'v1'}, output='output')
    recreated_commands = image_hash_to_sgfile(empty_pg_conn, 'output',
                                              get_current_head(empty_pg_conn, 'output'), err_on_end=False)

    # Parser strips newlines in sql but not the whitespace, so we have to reproduce the query here verbatim.
    assert recreated_commands == ["FROM test_pg_mount:%s" % get_tagged_id(snapper_conn, 'test_pg_mount', 'v1'),
                                  "SQL CREATE TABLE join_table AS SELECT fruit_id AS id, fruits.name AS fruit, "
                                  "vegetables.name AS vegetable                                 FROM fruits "
                                  "JOIN vegetables                                ON fruit_id = vegetable_id"]


def test_sgfile_incomplete_provenance(empty_pg_conn, snapper_conn):
    # This sgfile has a MOUNT as its first command. We're expecting image_hash_to_sgfile to base the result
    # on the image created by MOUNT and not regenerate the MOUNT command.
    _add_multitag_dataset_to_snapper(snapper_conn)
    execute_commands(empty_pg_conn, _load_sgfile('import_from_mounted_db_with_sql.sgfile'),
                     params={'TAG': 'v1'}, output='output')
    head = get_current_head(empty_pg_conn, 'output')
    image_with_mount = get_log(empty_pg_conn, 'output', head)[-2]
    recreated_commands = image_hash_to_sgfile(empty_pg_conn, 'output', head, err_on_end=False)

    assert recreated_commands == ["FROM output:%s" % image_with_mount,
                                  "SQL CREATE TABLE new_table AS SELECT * FROM all_fruits"]
