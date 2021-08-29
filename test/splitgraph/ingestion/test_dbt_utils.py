import os
import subprocess
import tempfile

import yaml
from splitgraph.ingestion.dbt.utils import patch_dbt_project_sources, prepare_git_repo

_REPO_PATH = "https://github.com/splitgraph/jaffle_shop_archive"


def test_dbt_repo_clone_patch():
    # Test a couple dbt helper utils by cloning an actual Git repository with the sample
    # project that we rescued from dbt's GitHub and running the patcher there.

    with tempfile.TemporaryDirectory() as tmp_dir:
        # demo/master actually has a declared source
        prepare_git_repo(_REPO_PATH, target_path=tmp_dir, ref="demo/master")

        assert "models" in os.listdir(tmp_dir)
        assert "dbt_project.yml" in os.listdir(tmp_dir)

        patch_dbt_project_sources(tmp_dir, "new_patched_schema")

        assert (
            subprocess.check_output(["git", "status", "--short"], cwd=tmp_dir).decode().strip()
            == "M models/staging/jaffle_shop/jaffle_shop.yml"
        )

        with open(os.path.join(tmp_dir, "models/staging/jaffle_shop/jaffle_shop.yml"), "r") as f:
            assert yaml.safe_load(f)["sources"][0]["schema"] == "new_patched_schema"
