from click.testing import CliRunner
from test.splitgraph.conftest import SPLITFILE_ROOT

from splitgraph.commandline import clone_c, pull_c, push_c, publish_c, reindex_c
from splitgraph.core.engine import repository_exists
from splitgraph.core.registry import get_published_info
from splitgraph.core.repository import Repository
from splitgraph.core.types import TableColumn


def test_pull_push(local_engine_empty, pg_repo_remote):
    runner = CliRunner()
    pg_repo_local = Repository.from_template(pg_repo_remote, engine=local_engine_empty)

    result = runner.invoke(clone_c, [str(pg_repo_local)])
    assert result.exit_code == 0
    assert repository_exists(pg_repo_local)

    pg_repo_remote.run_sql("INSERT INTO fruits VALUES (3, 'mayonnaise')")
    remote_engine_head = pg_repo_remote.commit()

    result = runner.invoke(pull_c, [str(pg_repo_local)])
    assert result.exit_code == 0
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 0

    result = runner.invoke(pull_c, [str(pg_repo_local), "--download-all"])
    assert result.exit_code == 0
    assert len(pg_repo_local.objects.get_downloaded_objects()) == 3

    pg_repo_local.images.by_hash(remote_engine_head.image_hash).checkout()

    pg_repo_local.run_sql("INSERT INTO fruits VALUES (4, 'mustard')")
    local_head = pg_repo_local.commit()

    assert local_head.image_hash not in list(pg_repo_remote.images)
    result = runner.invoke(push_c, [str(pg_repo_local), "-h", "DB"])
    assert result.exit_code == 0
    assert pg_repo_local.head.get_table("fruits")

    pg_repo_local.head.tag("v1")
    pg_repo_local.commit_engines()
    result = runner.invoke(
        publish_c, [str(pg_repo_local), "v1", "-r", SPLITFILE_ROOT + "README.md"]
    )
    assert result.exit_code == 0
    info = get_published_info(pg_repo_remote, "v1")
    assert info.image_hash == local_head.image_hash
    assert info.provenance == []
    assert info.readme == "Test readme for a test dataset."
    assert info.schemata == {
        "fruits": [
            TableColumn(1, "fruit_id", "integer", False, None),
            TableColumn(2, "name", "character varying", False, None),
        ],
        "vegetables": [
            TableColumn(1, "vegetable_id", "integer", False, None),
            TableColumn(2, "name", "character varying", False, None),
        ],
    }
    assert info.previews == {
        "fruits": [[1, "apple"], [2, "orange"], [3, "mayonnaise"], [4, "mustard"]],
        "vegetables": [[1, "potato"], [2, "carrot"]],
    }


def test_reindex_and_force_push(pg_repo_local, pg_repo_remote):
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(clone_c, [str(pg_repo_local)])
    assert result.exit_code == 0
    assert repository_exists(pg_repo_local)

    result = runner.invoke(
        reindex_c,
        [str(pg_repo_local) + ":latest", "fruits", '-i {"bloom": {"name": {"probability": 0.01}}}'],
    )
    assert result.exit_code == 0
    assert "Reindexed 1 object(s)" in result.output

    result = runner.invoke(push_c, [str(pg_repo_local), "-f"], mix_stderr=False)
    assert result.exit_code == 0

    obj = pg_repo_remote.images["latest"].get_table("fruits").objects[0]
    assert "bloom" in pg_repo_remote.objects.get_object_meta([obj])[obj].object_index
