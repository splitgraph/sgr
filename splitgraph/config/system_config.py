import os

from .environment_config import get_environment_config_value


def is_file(filename):
    return os.path.isfile(filename)


''' Export an object of SystemConfigGetters, for getting config values that
    require operations with the system, like checking if files exist.

    Don't forget to add each method definiton to the object after defining it.
'''
SystemConfigGetters = {}


def SG_CONFIG_FILE(default_return=None):
    '''
        Get the location of an existing SG_CONFIG_FILE on the system with
        a valid name. Do not attempt to parse the config file, just return
        its location.

        Otherwise, return default_return
    '''
    key = 'SG_CONFIG_FILE'

    valid_dirs = [
        os.getcwd()
    ]

    valid_names = [
        ".sgconfig",
        ".sgrc",
        ".splitgraph.config",
    ]

    env_val = get_environment_config_value(key, None)

    def file_exists(_dir, fn): return os.path.isfile(os.path.join(_dir, fn))

    cwd = os.getcwd()

    matching_files = []
    for _dir in valid_dirs:
        matching_files = matching_files + [os.path.join(_dir, name) for name in valid_names if file_exists(_dir, name)]

    num_matching_files = len(matching_files)

    if env_val and is_file(env_val):
        return env_val
    elif env_val and not is_file(env_val):
        sys.stderr.write('Warning: %s = %s is not a file' % (key, env_val))
        return default_return
    elif num_matching_files > 0:
        if num_matching_files > 1:
            sys.stderr.write(
                'Warning: %d splitgraph config files found. \n Using %s \n'
                % (num_matching_files, matching_files[0])
            )

        return matching_files[0]

    if env_val:
        return env_val


# Don't forget to do this for each method you want in the object
SystemConfigGetters["SG_CONFIG_FILE"] = SG_CONFIG_FILE


def get_system_config_value(key, default_return=None):
    return SystemConfigGetters.get(key, lambda: default_return)()
