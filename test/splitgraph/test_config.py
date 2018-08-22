import pytest

from splitgraph.config import CONFIG, create_config_dict

import os
import sys

import json

try:
    # python 3.4+ should use builtin unittest.mock not mock package
    from unittest.mock import patch
except ImportError:
    from mock import patch

def test_config_exports_dict():
    assert json.dumps(CONFIG) == json.dumps(create_config_dict())

# Sanity check that fs is provided magically by pyfakefs
# http://jmcgeheeiv.github.io/pyfakefs/release/usage.html#test-scenarios
def test_sanity_pyfakefs_is_working(fs):
    fs.create_file('/var/bogus/directory/bogus_file.txt')
    assert os.path.exists('/var/bogus/directory/bogus_file.txt')

def _write_config_file(fs, lines):
    fake_cwd = os.getcwd()
    fake_config_file = os.path.join(fake_cwd, '.sgconfig')

    # If running tests in docker, SG_CONFIG_FILE is already set and would
    # override our fake_config_file. In that case explicitly set SG_CONFIG_FILE
    if os.environ.get('SG_CONFIG_FILE', None) is not None:
        os.environ['SG_CONFIG_FILE'] = fake_config_file

    with open(fake_config_file, 'w') as f:
        for line in lines:
            f.write('%s\n' % line)

def test_key_set_in_config_file(fs):

    _write_config_file(fs, [
        '[defaults]',
        'SG_NAMESPACE=pass-the-test'
    ])

    config = create_config_dict()

    assert config['SG_NAMESPACE'] == 'pass-the-test'

def test_config_file_accumulation(fs):
    _write_config_file(fs, [
        '[remote: blah]',
        'SG_DRIVER_HOST=pass-the-test',
        '[remote: foo]',
        'SG_DRIVER_HOST=foo-pass'
    ])

    config = create_config_dict()

    assert config['remotes']['blah']['SG_DRIVER_HOST'] == 'pass-the-test'
    assert config['remotes']['foo']['SG_DRIVER_HOST'] == 'foo-pass'

# Hardcoded default key to check that it's passed hrough
def test_default_key(fs):
    assert CONFIG['SG_DRIVER_DB_NAME'] == 'cachedb'

def test_key_set_in_arg_flag():
    # --namespace mapped to SG_NAMESPACE in config/keys.py
    mock_argv = ['sg', '--namespace', 'pass-namespace-test']

    with patch.object(sys, 'argv', mock_argv):
        config = create_config_dict()
        assert config['SG_NAMESPACE'] == 'pass-namespace-test'

def test_key_set_in_env_var():
    mock_environ = {
        'SG_NAMESPACE': 'pass-env-namespace-test'
    }

    with patch.object(os, 'environ', mock_environ):
        config = create_config_dict()
        assert config['SG_NAMESPACE'] == 'pass-env-namespace-test'

def test_arg_flag_supercedes_config_file(fs):
    mock_argv = ['sg', '--namespace', 'namespace-from-arg']

    _write_config_file(fs, [
        '[defaults]',
        'SG_NAMESPACE=namespace-from-config-file'
    ])

    config = create_config_dict()
    assert config['SG_NAMESPACE'] == 'namespace-from-config-file'

    with patch.object(sys, 'argv', mock_argv):
        config = create_config_dict()
        assert config['SG_NAMESPACE'] == 'namespace-from-arg'

def test_arg_flag_supercedes_env_var(fs):
    mock_environ = {
        'SG_NAMESPACE': 'namespace-from-env-var'
    }

    mock_argv = ['sg', '--namespace', 'namespace-from-arg']

    with patch.object(sys, 'argv', mock_argv):
        with patch.object(os, 'environ', mock_environ):
            assert os.environ.get('SG_NAMESPACE', None) == 'namespace-from-env-var'
            assert sys.argv[2] == 'namespace-from-arg'

            config = create_config_dict()

            assert config['SG_NAMESPACE'] == 'namespace-from-arg'

def test_env_var_supercedes_config_file(fs):
    _write_config_file(fs, [
        '[defaults]',
        'SG_NAMESPACE=namespace-from-config-file'
    ])

    config = create_config_dict()
    assert config['SG_NAMESPACE'] == 'namespace-from-config-file'

    mock_environ = {
        'SG_NAMESPACE': 'pass-env-namespace-test'
    }

    with patch.object(os, 'environ', mock_environ):
        config = create_config_dict()
        assert config['SG_NAMESPACE'] == 'pass-env-namespace-test'
