import subprocess

import pytest


@pytest.mark.example
def test_example(example_path):
    """
    Run a test for a single example directory

    :param example_path: Path to the example directory
    """
    result = subprocess.run(
        args=["../run_example.py", "example.yaml", "--no-pause"],
        cwd=example_path,
        stderr=subprocess.STDOUT,
    )

    if result.returncode != 0:
        raise AssertionError("Example exited with code %d!" % result.returncode)
