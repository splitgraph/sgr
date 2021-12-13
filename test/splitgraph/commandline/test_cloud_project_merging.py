import os
from io import StringIO
from test.splitgraph.conftest import RESOURCES

from splitgraph.cloud.models import RepositoriesYAML
from splitgraph.cloud.project.utils import merge_project_files
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
