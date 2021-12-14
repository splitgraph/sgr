import os
from io import StringIO
from test.splitgraph.conftest import RESOURCES

from click.testing import CliRunner
from splitgraph.cloud.project.models import RepositoriesYAML
from splitgraph.cloud.project.utils import merge_project_files
from splitgraph.commandline.cloud import validate_c
from splitgraph.utils.yaml import safe_dump, safe_load


def test_project_merging(snapshot):
    with open(os.path.join(RESOURCES, "repositories_yml", "repositories.yml")) as f:
        left = RepositoriesYAML.parse_obj(safe_load(f))
    with open(os.path.join(RESOURCES, "repositories_yml", "repositories.override.yml")) as f:
        right = RepositoriesYAML.parse_obj(safe_load(f))

    merged = merge_project_files(left, right)

    result = StringIO()
    safe_dump(merged.dict(by_alias=True, exclude_unset=True), result)
    result.seek(0)
    snapshot.assert_match(result.read(), "repositories.merged.yml")


def test_project_validate(snapshot):
    # Use the same file as the previous test
    snapshot.snapshot_dir = os.path.join(
        os.path.dirname(__file__), "snapshots/test_cloud_project_merging/test_project_merging"
    )
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        validate_c,
        [
            "-f",
            os.path.join(RESOURCES, "repositories_yml", "repositories.yml"),
            "-f",
            os.path.join(RESOURCES, "repositories_yml", "repositories.override.yml"),
        ],
    )
    assert result.exit_code == 0

    snapshot.assert_match(result.stdout, "repositories.merged.yml")
