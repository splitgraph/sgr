import json
import os
import sys
from contextlib import contextmanager
from unittest.mock import patch

import pytest

from splitgraph.config import CONFIG, create_config_dict
from splitgraph.config import keys
from splitgraph.config.argument_config import get_arg_tuples, get_argument_config_value
from splitgraph.config.config_file_config import hoist_section
from splitgraph.config.environment_config import get_environment_config_value
from splitgraph.config.system_config import (
    get_explicit_config_file_location,
    get_explicit_config_file_dirs,
    get_config_file,
    VALID_CONFIG_FILE_NAMES,
)
from splitgraph.core.engine import _parse_paths_overrides


@contextmanager
def patch_os_environ(update):
    # Context manager to inject some extra values into the environment, e.g.
    # when SG_CONFIG_FILE is already set and is used by other tests and we
    # want to test it being overridden.
    mock_environ = os.environ.copy()
    mock_environ.update(update)
    with patch.object(os, "environ", mock_environ):
        yield


def test_every_key_has_matching_arg_key():
    KEYS = keys.KEYS
    ARGUMENT_KEY_MAP = keys.ARGUMENT_KEY_MAP
    ARG_KEYS = keys.ARG_KEYS

    for arg_key in ARG_KEYS:
        assert ARGUMENT_KEY_MAP[arg_key] in KEYS


def test_get_arg_tuples():
    mock_argv = [
        "sg",
        "--namespace",
        "namespace-from-arg",
        "--non-existing-key",
        "foo",
        "--meta-schema",
        "bar",
    ]

    with patch.object(sys, "argv", mock_argv):
        arg_tuples = get_arg_tuples()

        assert len(arg_tuples) == 2
        assert arg_tuples[0] == ("--namespace", "namespace-from-arg")
        assert arg_tuples[1] == ("--meta-schema", "bar")


def test_get_argument_config_value():
    mock_argv = ["sg", "--namespace", "namespace-from-arg"]

    with patch.object(sys, "argv", mock_argv):
        namespace_val = get_argument_config_value("SG_NAMESPACE")
        assert namespace_val == "namespace-from-arg"

        non_val = get_argument_config_value("DOESNOTEXISTSKJKJ", None)
        assert non_val is None


def test_get_argument_config_value_duplicate():
    mock_argv = ["sg", "--namespace", "namespace-1-from-arg", "--namespace", "namespace-2-from-arg"]

    with patch.object(sys, "argv", mock_argv):
        namespace_val = get_argument_config_value("SG_NAMESPACE")
        assert namespace_val == "namespace-1-from-arg"

        non_val = get_argument_config_value("DOESNOTEXISTSKJKJ", None)
        assert non_val is None


def test_get_environment_config_value():
    mock_environ = {"SOME_ARBITRARY_KEY": "foo bar"}

    with patch_os_environ(mock_environ):
        val = get_environment_config_value("SOME_ARBITRARY_KEY")

        assert val == "foo bar"

        lowercase_non_val = get_environment_config_value("some_arbitrary_key")
        assert lowercase_non_val is None

        default_none_non_val = get_environment_config_value("doesnotexist")
        assert default_none_non_val is None

        non_val = get_environment_config_value("doesnotexist", "xxx")
        assert non_val == "xxx"


def test_config_exports_dict():
    """ The CONFIG object exported from config should be the same
        as the result of calling create_config_dict()
    """
    assert json.dumps(CONFIG) == json.dumps(create_config_dict())


def test_sanity_pyfakefs_is_working(fs):
    """ Sanity check that fs is provided magically by pyfakefs
        http://jmcgeheeiv.github.io/pyfakefs/release/usage.html#test-scenarios
    """
    fs.create_file("/var/bogus/directory/bogus_file.txt")
    assert os.path.exists("/var/bogus/directory/bogus_file.txt")


def test_get_explicit_config_file_location_from_env_existing_file(fs):
    """ get_explicit_config_file_location()
        from env var when file exists should return the location
    """
    existing_file = "/var/mock/foo_bar.cfg"

    fs.create_file(existing_file)

    mock_environ = {"SG_CONFIG_FILE": existing_file}

    assert os.path.isfile(existing_file)

    with patch_os_environ(mock_environ):
        loc = get_explicit_config_file_location()

        assert loc == "/var/mock/foo_bar.cfg"


def test_get_explicit_config_file_location_from_env_nonexisting_file(fs):
    """ get_explicit_config_file_location()
        from env var when file does NOT exist should return None
    """
    dne_file = "/var/doesnotexist/foo_bar.cfg"

    mock_environ = {"SG_CONFIG_FILE": dne_file}

    assert not os.path.isfile(dne_file)

    with patch_os_environ(mock_environ):
        loc = get_explicit_config_file_location()

        assert loc is None


def test_get_explicit_config_file_location_from_arg_existing_file(fs):
    """ get_explicit_config_file_location()
        from arg flag when file exists should return the location
    """
    existing_file = "/var/mock/foo_bar.cfg"

    fs.create_file(existing_file)

    mock_sysargv = ["sg", "--config-file", existing_file]

    assert os.path.isfile(existing_file)

    with patch.object(sys, "argv", mock_sysargv):
        loc = get_explicit_config_file_location()

        assert loc == "/var/mock/foo_bar.cfg"


def test_get_explicit_config_file_location_from_arg_nonexisting_file(fs):
    """ get_explicit_config_file_location()
        from arg flag when file does NOT exist should return None
    """
    dne_file = "/var/doesnotexist/foo_bar.cfg"

    mock_sysargv = ["sg", "--config-file", dne_file]

    assert not os.path.isfile(dne_file)

    with patch.object(sys, "argv", mock_sysargv):
        loc = get_explicit_config_file_location()

        assert loc is None


def test_get_explicit_config_file_existing_dirs_via_env(fs):
    exists_dir_1 = "/var/some-dir-to-look-for-configs"
    exists_dir_2 = "/var/another-dir-to-look-for-configs"

    fs.create_dir(exists_dir_1)
    fs.create_dir(exists_dir_2)

    assert os.path.isdir(exists_dir_1)
    assert os.path.isdir(exists_dir_2)

    # Try with duplicates which should be filtered
    mock_environ = {"SG_CONFIG_DIRS": "%s:%s:%s" % (exists_dir_1, exists_dir_1, exists_dir_2)}

    with patch_os_environ(mock_environ):
        dirs = get_explicit_config_file_dirs()

        assert len(dirs) == 2
        assert exists_dir_1 in dirs
        assert exists_dir_2 in dirs


def test_get_explicit_config_file_existing_single_dir_via_env(fs):
    exists_dir = "/var/some-dir-to-look-for-configs"

    fs.create_dir(exists_dir)

    assert os.path.isdir(exists_dir)

    # singular
    mock_environ = {"SG_CONFIG_DIR": exists_dir}

    with patch_os_environ(mock_environ):
        dirs = get_explicit_config_file_dirs()

        assert len(dirs) == 1
        assert exists_dir in dirs


def test_get_explicit_config_file_non_existing_single_dir_via_env(fs):
    dne_dir = "/var/some-nonexisting-dir-to-look-for-configs"

    assert not os.path.isdir(dne_dir)

    # singular
    mock_environ = {"SG_CONFIG_DIR": dne_dir}

    with patch_os_environ(mock_environ):
        dirs = get_explicit_config_file_dirs()

        assert len(dirs) == 0


def test_get_explicit_config_file_existing_single_and_plural_dir_via_env(fs):
    exists_dir_1 = "/var/some-dir-to-look-for-configs"
    exists_dir_2 = "/var/another-dir-to-look-for-configs"
    exists_dir_3 = "/var/yet-anothe-dir-to-look-for-configs"

    fs.create_dir(exists_dir_1)
    fs.create_dir(exists_dir_2)
    fs.create_dir(exists_dir_3)

    assert os.path.isdir(exists_dir_1)
    assert os.path.isdir(exists_dir_2)
    assert os.path.isdir(exists_dir_3)

    # combine singular and plural and filter dupes
    mock_environ = {
        "SG_CONFIG_DIR": exists_dir_1,
        "SG_CONFIG_DIRS": "%s:%s:%s" % (exists_dir_1, exists_dir_2, exists_dir_3),
    }

    with patch_os_environ(mock_environ):
        dirs = get_explicit_config_file_dirs()

        assert len(dirs) == 3
        assert exists_dir_1 in dirs
        assert exists_dir_2 in dirs
        assert exists_dir_3 in dirs


def test_get_explicit_config_file_non_existing_dirs_via_env(fs):
    non_exist_dir_1 = "/var/some-non-existing-dir-to-look-for-configs"
    non_exist_dir_2 = "/var/another-non-existing-dir-to-look-for-configs"

    assert not os.path.isdir(non_exist_dir_1)
    assert not os.path.isdir(non_exist_dir_2)

    mock_environ = {
        "SG_CONFIG_DIR": non_exist_dir_1,
        "SG_CONFIG_DIRS": "%s:%s:%s" % (non_exist_dir_1, non_exist_dir_1, non_exist_dir_2),
    }

    with patch_os_environ(mock_environ):
        dirs = get_explicit_config_file_dirs()

        assert len(dirs) == 0


##


def test_get_explicit_config_file_existing_dirs_via_arg(fs):
    exists_dir_1 = "/var/some-dir-to-look-for-configs"
    exists_dir_2 = "/var/another-dir-to-look-for-configs"

    fs.create_dir(exists_dir_1)
    fs.create_dir(exists_dir_2)

    assert os.path.isdir(exists_dir_1)
    assert os.path.isdir(exists_dir_2)

    # Try with duplicates which should be filtered
    mock_sysargv = ["sg", "--config-dirs", "%s:%s:%s" % (exists_dir_1, exists_dir_1, exists_dir_2)]

    with patch.object(sys, "argv", mock_sysargv):
        dirs = get_explicit_config_file_dirs()

        assert len(dirs) == 2
        assert exists_dir_1 in dirs
        assert exists_dir_2 in dirs


def test_get_explicit_config_file_existing_single_dir_via_arg(fs):
    exists_dir = "/var/some-dir-to-look-for-configs"

    fs.create_dir(exists_dir)

    assert os.path.isdir(exists_dir)

    # singular
    mock_sysargv = ["sg", "--config-dir", exists_dir]

    with patch.object(sys, "argv", mock_sysargv):
        dirs = get_explicit_config_file_dirs()

        assert len(dirs) == 1
        assert exists_dir in dirs


def test_get_explicit_config_file_non_existing_single_dir_via_arg(fs):
    dne_dir = "/var/some-nonexisting-dir-to-look-for-configs"

    assert not os.path.isdir(dne_dir)

    # singular
    mock_sysargv = ["sg", "--config-dir", dne_dir]

    with patch.object(sys, "argv", mock_sysargv):
        dirs = get_explicit_config_file_dirs()

        assert len(dirs) == 0


def test_get_explicit_config_file_existing_single_and_plural_dir_via_arg(fs):
    exists_dir_1 = "/var/some-dir-to-look-for-configs"
    exists_dir_2 = "/var/another-dir-to-look-for-configs"
    exists_dir_3 = "/var/yet-anothe-dir-to-look-for-configs"

    fs.create_dir(exists_dir_1)
    fs.create_dir(exists_dir_2)
    fs.create_dir(exists_dir_3)

    assert os.path.isdir(exists_dir_1)
    assert os.path.isdir(exists_dir_2)
    assert os.path.isdir(exists_dir_3)

    # combine singular and plural and filter dupes
    mock_sysargv = [
        "sg",
        "--config-dir",
        exists_dir_1,
        "--config-dirs",
        "%s:%s:%s" % (exists_dir_1, exists_dir_2, exists_dir_3),
    ]

    with patch.object(sys, "argv", mock_sysargv):
        dirs = get_explicit_config_file_dirs()

        assert len(dirs) == 3
        assert exists_dir_1 in dirs
        assert exists_dir_2 in dirs
        assert exists_dir_3 in dirs


def test_get_explicit_config_file_non_existing_dirs_via_arg(fs):
    non_exist_dir_1 = "/var/some-non-existing-dir-to-look-for-configs"
    non_exist_dir_2 = "/var/another-non-existing-dir-to-look-for-configs"

    assert not os.path.isdir(non_exist_dir_1)
    assert not os.path.isdir(non_exist_dir_2)

    mock_sysargv = [
        "sg",
        "--config-dirs",
        "%s:%s:%s" % (non_exist_dir_1, non_exist_dir_1, non_exist_dir_2),
    ]

    with patch.object(sys, "argv", mock_sysargv):
        dirs = get_explicit_config_file_dirs()

        assert len(dirs) == 0


def test_SG_CONFIG_FILE_none_exists(fs):
    assert get_config_file() == None


@pytest.mark.parametrize("config_file_name", VALID_CONFIG_FILE_NAMES)
def test_SG_CONFIG_FILE_valid_names_home_sub_dir(fs, config_file_name):
    mock_environ = {"HOME": "/fake/home/dir"}

    home_sub_dir = os.path.join(mock_environ["HOME"], ".splitgraph")

    fs.create_dir(home_sub_dir)

    config_file = os.path.join(home_sub_dir, config_file_name)

    fs.create_file(config_file)

    with patch_os_environ(mock_environ):
        assert os.path.isdir(home_sub_dir)
        assert os.path.isfile(config_file)

        assert get_config_file() == config_file


@pytest.mark.parametrize("config_file_name", VALID_CONFIG_FILE_NAMES)
def test_SG_CONFIG_FILE_valid_names_cwd(fs, config_file_name):
    fake_cwd = os.getcwd()
    fake_config_file = os.path.join(fake_cwd, config_file_name)

    fs.create_file(fake_config_file)

    assert get_config_file() == fake_config_file


@pytest.mark.parametrize("config_file_name", VALID_CONFIG_FILE_NAMES)
def test_SG_CONFIG_FILE_explicit_dir_supercedes_valid_names_cwd(fs, config_file_name):
    fake_cwd = os.getcwd()
    fake_config_file = os.path.join(fake_cwd, config_file_name)

    fs.create_file(fake_config_file)

    assert get_config_file() == fake_config_file

    exists_dir = "/var/some-dir-to-look-for-configs"

    fs.create_dir(exists_dir)

    assert os.path.isdir(exists_dir)

    # singular
    mock_sysargv = ["sg", "--config-dir", exists_dir]

    explicit_config_file = os.path.join(exists_dir, config_file_name)

    fs.create_file(explicit_config_file)

    with patch.object(sys, "argv", mock_sysargv):
        assert get_config_file() == explicit_config_file


def _write_config_file(fs, lines):
    fake_cwd = os.getcwd()
    fake_config_file = os.path.join(fake_cwd, ".sgconfig")

    with open(fake_config_file, "w") as f:
        for line in lines:
            f.write("%s\n" % line)

    return fake_config_file


def test_key_set_in_config_file(fs):
    fake_config_file = _write_config_file(fs, ["[defaults]", "SG_NAMESPACE=pass-the-test"])

    with patch_os_environ({"SG_CONFIG_FILE": fake_config_file}):
        config = create_config_dict()

    assert config["SG_NAMESPACE"] == "pass-the-test"


def test_hoist_section():
    orig_config_dict = {"hoistableSection": {"OVERRIDE_THIS_ARBITRARY_KEY": "foo"}}

    new_config_dict = hoist_section(orig_config_dict, section="hoistableSection")

    assert new_config_dict["OVERRIDE_THIS_ARBITRARY_KEY"] == "foo"
    assert "hoistableSection" not in new_config_dict.keys()


def test_config_file_accumulation(fs):
    fake_config_file = _write_config_file(
        fs,
        [
            "[remote: blah]",
            "SG_ENGINE_HOST=pass-the-test",
            "[remote: foo]",
            "SG_ENGINE_HOST=foo-pass",
        ],
    )

    with patch_os_environ({"SG_CONFIG_FILE": fake_config_file}):
        config = create_config_dict()

    assert config["remotes"]["blah"]["SG_ENGINE_HOST"] == "pass-the-test"
    assert config["remotes"]["foo"]["SG_ENGINE_HOST"] == "foo-pass"


# Hardcoded default key to check that it's passed through
def test_default_key(fs):
    assert CONFIG["SG_ENGINE_DB_NAME"] == "splitgraph"


def test_key_set_in_arg_flag():
    # --namespace mapped to SG_NAMESPACE in config/keys.py
    mock_argv = ["sg", "--namespace", "pass-namespace-test"]

    with patch.object(sys, "argv", mock_argv):
        config = create_config_dict()
        assert config["SG_NAMESPACE"] == "pass-namespace-test"


def test_key_set_in_env_var():
    mock_environ = {"SG_NAMESPACE": "pass-env-namespace-test"}

    with patch_os_environ(mock_environ):
        config = create_config_dict()
        assert config["SG_NAMESPACE"] == "pass-env-namespace-test"


def test_arg_flag_supercedes_config_file(fs):
    mock_argv = ["sg", "--namespace", "namespace-from-arg"]

    fake_config_file = _write_config_file(
        fs, ["[defaults]", "SG_NAMESPACE=namespace-from-config-file"]
    )

    with patch_os_environ({"SG_CONFIG_FILE": fake_config_file}):
        config = create_config_dict()
        assert config["SG_NAMESPACE"] == "namespace-from-config-file"

        with patch.object(sys, "argv", mock_argv):
            config = create_config_dict()
            assert config["SG_NAMESPACE"] == "namespace-from-arg"


def test_arg_flag_supercedes_env_var(fs):
    mock_environ = {"SG_NAMESPACE": "namespace-from-env-var"}

    mock_argv = ["sg", "--namespace", "namespace-from-arg"]

    with patch.object(sys, "argv", mock_argv):
        with patch_os_environ(mock_environ):
            assert os.environ.get("SG_NAMESPACE", None) == "namespace-from-env-var"
            assert sys.argv[2] == "namespace-from-arg"

            config = create_config_dict()

            assert config["SG_NAMESPACE"] == "namespace-from-arg"


def test_env_var_supercedes_config_file(fs):
    fake_config_file = _write_config_file(
        fs, ["[defaults]", "SG_NAMESPACE=namespace-from-config-file"]
    )

    with patch_os_environ({"SG_CONFIG_FILE": fake_config_file}):
        config = create_config_dict()
        assert config["SG_NAMESPACE"] == "namespace-from-config-file"

    with patch_os_environ(
        {"SG_NAMESPACE": "pass-env-namespace-test", "SG_CONFIG_FILE": fake_config_file}
    ):
        config = create_config_dict()
        assert config["SG_NAMESPACE"] == "pass-env-namespace-test"


def test_lookup_override_parser():
    assert _parse_paths_overrides(
        lookup_path="remote_engine", override_path="override_repo_1:local"
    ) == (["remote_engine"], {"override_repo_1": "local"})
