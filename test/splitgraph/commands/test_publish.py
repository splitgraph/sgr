import pytest

from splitgraph.core.registry import get_published_info
from splitgraph.core.repository import Repository
from splitgraph.splitfile import execute_commands
from test.splitgraph.conftest import OUTPUT, load_splitfile


@pytest.mark.parametrize('extra_info', [True, False])
def test_publish(local_engine_empty, remote_engine, pg_repo_remote_multitag, extra_info):
    # Run some splitfile commands to create a dataset and push it
    execute_commands(load_splitfile('import_remote_multiple.splitfile'), params={'TAG': 'v1'}, output=OUTPUT)
    OUTPUT.head.tag('v1')

    remote_output = Repository.from_template(OUTPUT, engine=remote_engine)

    OUTPUT.push(remote_output)
    OUTPUT.publish('v1', readme="A test repo.", include_provenance=extra_info, include_table_previews=extra_info)

    # Base the derivation on v2 of test/pg_mount and publish that too.
    execute_commands(load_splitfile('import_remote_multiple.splitfile'), params={'TAG': 'v2'}, output=OUTPUT)
    OUTPUT.head.tag('v2')
    OUTPUT.push(remote_output)
    OUTPUT.publish('v2', readme="Based on v2.", include_provenance=extra_info, include_table_previews=extra_info)

    image_hash, published_dt, provenance, readme, schemata, previews = get_published_info(remote_output, 'v1')
    assert image_hash == OUTPUT.images['v1'].image_hash
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
        assert provenance == [[['test', 'pg_mount'], pg_repo_remote_multitag.images['v1'].image_hash]]
        assert previews == {'join_table': [[1, 'apple', 'potato'], [2, 'orange', 'carrot']],
                            'my_fruits': [[1, 'apple'], [2, 'orange']],
                            'vegetables': [[1, 'potato'], [2, 'carrot']]}

    else:
        assert provenance is None
        assert previews is None

    image_hash, published_dt, provenance, readme, schemata, previews = get_published_info(remote_output, 'v2')
    assert image_hash == OUTPUT.images['v2'].image_hash
    assert readme == "Based on v2."
    assert schemata == expected_schemata
    if extra_info:
        assert provenance == [[['test', 'pg_mount'], pg_repo_remote_multitag.images['v2'].image_hash]]
        assert previews == {'join_table': [[2, 'orange', 'carrot']],
                            'my_fruits': [[2, 'orange']],
                            'vegetables': [[1, 'potato'], [2, 'carrot']]}
    else:
        assert provenance is None
        assert previews is None
