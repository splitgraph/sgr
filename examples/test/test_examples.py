import subprocess


def test_example(example_path):
    """
    Run a test for a single example directory

    :param example_path: Path to the example directory
    """
    subprocess.run(
        args=["../run_example.py", "example.yaml", "--no-pause"],
        cwd=example_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
