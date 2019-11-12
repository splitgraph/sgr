import os
import subprocess

import pytest


@pytest.mark.example
def test_example(example_path):
    """
    Run a test for a single example directory

    :param example_path: Path to the example directory
    """

    # Remove SG-specific envvars that were set in tests (to simulate run_example.py being
    # actually run in a clean environment, e.g. without SG_CONFIG_FILE set.)
    env = {k: v for k, v in os.environ.items() if not k.startswith("SG_")}

    result = subprocess.run(
        args=["../run_example.py", "example.yaml", "--no-pause"],
        cwd=example_path,
        stderr=subprocess.STDOUT,
        env=env,
    )

    if result.returncode != 0:
        raise AssertionError("Example exited with code %d!" % result.returncode)
