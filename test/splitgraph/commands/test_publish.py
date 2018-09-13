from splitgraph.commands import push
from splitgraph.commands.publish import publish
from splitgraph.meta_handler import set_tag, get_current_head, get_tagged_id
from splitgraph.registry_meta_handler import get_published_info
from splitgraph.sgfile import execute_commands
from test.splitgraph.conftest import SNAPPER_CONN_STRING
from test.splitgraph.test_sgfile import _add_multitag_dataset_to_snapper, _load_sgfile


def test_publish(empty_pg_conn, snapper_conn):
    # Run some sgfile commands to create a dataset and push it
    new_head = _add_multitag_dataset_to_snapper(snapper_conn)
    execute_commands(empty_pg_conn, _load_sgfile('import_remote_multiple.sgfile'), params={'TAG': 'v1'},
                     output='output')
    set_tag(empty_pg_conn, 'output', get_current_head(empty_pg_conn, 'output'), 'v1')
    push(empty_pg_conn, 'output', remote_conn_string=SNAPPER_CONN_STRING)
    publish(empty_pg_conn, 'output', 'v1', readme="A test repo.")

    # Base the derivation on v2 of test_pg_mount and publish that too.
    execute_commands(empty_pg_conn, _load_sgfile('import_remote_multiple.sgfile'), params={'TAG': 'v2'},
                     output='output')
    set_tag(empty_pg_conn, 'output', get_current_head(empty_pg_conn, 'output'), 'v2')
    push(empty_pg_conn, 'output', remote_conn_string=SNAPPER_CONN_STRING)
    publish(empty_pg_conn, 'output', 'v2', readme="Based on v2.")

    image_hash, published_dt, provenance, readme = get_published_info(snapper_conn, 'output', 'v1')
    assert image_hash == get_tagged_id(empty_pg_conn, 'output', 'v1')
    assert provenance == [["test_pg_mount", get_tagged_id(snapper_conn, 'test_pg_mount', 'v1')]]
    assert readme == "A test repo."
    image_hash, published_dt, provenance, readme = get_published_info(snapper_conn, 'output', 'v2')
    assert image_hash == get_tagged_id(empty_pg_conn, 'output', 'v2')
    assert provenance == [["test_pg_mount", get_tagged_id(snapper_conn, 'test_pg_mount', 'v2')]]
    assert readme == "Based on v2."
