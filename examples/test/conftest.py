import os.path


def pytest_generate_tests(metafunc):
    """Generate test cases (one for each subdirectory in examples/ apart from "test" itself"""
    if "example_path" in metafunc.fixturenames:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        subfolders = [
            f.path for f in os.scandir(basedir) if f.is_dir() if f.name not in ["htmlcov", "test"]
        ]

        metafunc.parametrize("example_path", subfolders)
