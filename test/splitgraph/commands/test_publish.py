import pytest

from splitgraph.core.registry import get_published_info
from splitgraph.core.repository import Repository
from splitgraph.core.types import TableColumn
from splitgraph.splitfile import execute_commands
from test.splitgraph.conftest import OUTPUT, load_splitfile


@pytest.mark.parametrize("extra_info", [True, False])
def test_publish(local_engine_empty, remote_engine, pg_repo_remote_multitag, extra_info):
    # Run some splitfile commands to create a dataset and push it
    execute_commands(
        load_splitfile("import_remote_multiple.splitfile"), params={"TAG": "v1"}, output=OUTPUT
    )
    OUTPUT.head.tag("v1")

    remote_output = Repository.from_template(OUTPUT, engine=remote_engine)

    OUTPUT.push(remote_output)
    OUTPUT.publish(
        "v1",
        readme="A test repo.",
        include_provenance=extra_info,
        include_table_previews=extra_info,
    )

    # Base the derivation on v2 of test/pg_mount and publish that too.
    execute_commands(
        load_splitfile("import_remote_multiple.splitfile"), params={"TAG": "v2"}, output=OUTPUT
    )
    OUTPUT.head.tag("v2")
    OUTPUT.push(remote_output)
    OUTPUT.publish(
        "v2",
        readme="Based on v2.",
        include_provenance=extra_info,
        include_table_previews=extra_info,
    )

    info = get_published_info(remote_output, "v1")
    assert info.image_hash == OUTPUT.images["v1"].image_hash
    assert info.readme == "A test repo."
    expected_schemata = {
        "join_table": [
            TableColumn(1, "id", "integer", False, None),
            TableColumn(2, "fruit", "character varying", False, None),
            TableColumn(3, "vegetable", "character varying", False, None),
        ],
        "my_fruits": [
            TableColumn(1, "fruit_id", "integer", False, None),
            TableColumn(2, "name", "character varying", False, None),
        ],
        "vegetables": [
            TableColumn(1, "vegetable_id", "integer", False, None),
            TableColumn(2, "name", "character varying", False, None),
        ],
    }

    assert info.schemata == expected_schemata
    if extra_info:
        assert info.provenance == [
            [["test", "pg_mount"], pg_repo_remote_multitag.images["v1"].image_hash]
        ]
        assert info.previews == {
            "join_table": [[1, "apple", "potato"], [2, "orange", "carrot"]],
            "my_fruits": [[1, "apple"], [2, "orange"]],
            "vegetables": [[1, "potato"], [2, "carrot"]],
        }

    else:
        assert info.provenance is None
        assert info.previews is None

    info = get_published_info(remote_output, "v2")
    assert info.image_hash == OUTPUT.images["v2"].image_hash
    assert info.readme == "Based on v2."
    assert info.schemata == expected_schemata
    if extra_info:
        assert info.provenance == [
            [["test", "pg_mount"], pg_repo_remote_multitag.images["v2"].image_hash]
        ]
        assert info.previews == {
            "join_table": [[2, "orange", "carrot"]],
            "my_fruits": [[2, "orange"]],
            "vegetables": [[1, "potato"], [2, "carrot"]],
        }
    else:
        assert info.provenance is None
        assert info.previews is None
