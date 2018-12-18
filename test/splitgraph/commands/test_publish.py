import pytest

from splitgraph._data.registry import get_published_info
from splitgraph.commands import push
from splitgraph.commands.publish import publish
from splitgraph.commands.tagging import get_current_head, get_tagged_id, set_tag
from splitgraph.engine import switch_engine
from splitgraph.splitfile import execute_commands
from test.splitgraph.conftest import OUTPUT, PG_MNT, add_multitag_dataset_to_engine, \
    load_splitfile, REMOTE_ENGINE


@pytest.mark.parametrize('extra_info', [True, False])
def test_publish(local_engine_empty, remote_engine, extra_info):
    # Run some splitfile commands to create a dataset and push it
    add_multitag_dataset_to_engine(remote_engine)
    execute_commands(load_splitfile('import_remote_multiple.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    set_tag(OUTPUT, get_current_head(OUTPUT), 'v1')
    push(OUTPUT, remote_driver='remote_driver')
    publish(OUTPUT, 'v1', readme="A test repo.", include_provenance=extra_info, include_table_previews=extra_info)

    # Base the derivation on v2 of test/pg_mount and publish that too.
    execute_commands(load_splitfile('import_remote_multiple.splitfile'), params={'TAG': 'v2'}, output=OUTPUT)
    set_tag(OUTPUT, get_current_head(OUTPUT), 'v2')
    push(OUTPUT, remote_driver='remote_driver')
    publish(OUTPUT, 'v2', readme="Based on v2.", include_provenance=extra_info, include_table_previews=extra_info)

    with switch_engine(REMOTE_ENGINE):
        image_hash, published_dt, provenance, readme, schemata, previews = get_published_info(OUTPUT, 'v1')
    assert image_hash == get_tagged_id(OUTPUT, 'v1')
    assert readme == "A test repo."
    expected_schemata = {'join_table': [['id', 'integer', False],
                                        ['fruit', 'character varying', False],
                                        ['vegetable', 'character varying', False]],
                         'my_fruits': [['fruit_id', 'integer', False],
                                       ['name', 'character varying', False]],
                         'vegetables': [['vegetable_id', 'integer', False],
                                        ['name', 'character varying', False]]}

    assert schemata == expected_schemata
    if extra_info:
        with switch_engine(REMOTE_ENGINE):
            assert provenance == [[['test', 'pg_mount'], get_tagged_id(PG_MNT, 'v1')]]
        assert previews == {'join_table': [[1, 'apple', 'potato'], [2, 'orange', 'carrot']],
                            'my_fruits': [[1, 'apple'], [2, 'orange']],
                            'vegetables': [[1, 'potato'], [2, 'carrot']]}

    else:
        assert provenance is None
        assert previews is None

    with switch_engine(REMOTE_ENGINE):
        image_hash, published_dt, provenance, readme, schemata, previews = get_published_info(OUTPUT, 'v2')
    assert image_hash == get_tagged_id(OUTPUT, 'v2')
    assert readme == "Based on v2."
    assert schemata == expected_schemata
    if extra_info:
        with switch_engine(REMOTE_ENGINE):
            assert provenance == [[['test', 'pg_mount'], get_tagged_id(PG_MNT, 'v2')]]
        assert previews == {'join_table': [[2, 'orange', 'carrot']],
                            'my_fruits': [[2, 'orange']],
                            'vegetables': [[1, 'potato'], [2, 'carrot']]}
    else:
        assert provenance is None
        assert previews is None
