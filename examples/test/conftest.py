import os.path


def pytest_generate_tests(metafunc):
    """Generate test cases (one for each subdirectory in examples/ apart from "test" itself"""
    if "example_path" in metafunc.fixturenames:
        basedir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

        subfolders = []
        for f in os.scandir(basedir):
            if (
                not f.is_dir()
                or f.name in ["htmlcov", "test"]
                or not os.path.exists(os.path.join(f.path, "example.yaml"))
            ):
                continue
            subfolders.append(f.path)

        metafunc.parametrize("example_path", subfolders)
