# driver params

from .keys import KEYS
from .argument_config import get_argument_config_value
from .environment_config import get_environment_config_value
from .system_config import get_system_config_value
from .default_config import get_default_config_value
from .config_file_config import get_config_dict_from_config_file

def lazy_get_config_value(key, default_return=None):
    '''
        Get the config value for a key in the following precedence
        Otherwise return default_return
    '''

    return (
        get_argument_config_value(key, None)
        or get_environment_config_value(key, None)
        or get_system_config_value(key, None)
        or get_default_config_value(key, None)
        or default_return
    )

def update_config_dict_from_arguments(config_dict):
    '''
        Given an existing config_dict, update after reading sys.argv
        and overwriting any keys.

        Return updated copy of config_dict.
    '''
    new_config_dict = config_dict.copy()
    argument_config_dict = {k: get_argument_config_value(k, None) for k in KEYS if get_argument_config_value(k) is not None}
    new_config_dict.update(argument_config_dict)
    return new_config_dict

def update_config_dict_from_env_vars(config_dict):
    '''
        Given an existing config_dict, update after reading os.environ
        and overwriting any keys.

        Return updated copy of config_dict.
    '''

    new_config_dict = config_dict.copy()
    argument_config_dict = {k: get_environment_config_value(k, None) for k in KEYS if get_environment_config_value(k) is not None}
    new_config_dict.update(argument_config_dict)

    return new_config_dict

def update_config_dict_from_file(config_dict, sg_config_file):
    '''
        Given an existing config_dict, update after reading sg_config_file
        and overwriting any keys according to the rules in config_file_config

        Return updated copy of config_dict.
    '''

    new_config_dict = config_dict.copy()
    config_file_dict = get_config_dict_from_config_file(sg_config_file)
    new_config_dict.update(config_file_dict)

    return new_config_dict

def create_config_dict():
    '''
        Create and return a dict of all known config values
    '''

    config_dict = {k: lazy_get_config_value(k) for k in KEYS}

    sg_config_file = config_dict.get('SG_CONFIG_FILE', None)

    # if not sg_config_file:
    #     return config_dict

    if sg_config_file:
        config_dict = update_config_dict_from_file(config_dict, sg_config_file)

    config_dict = update_config_dict_from_env_vars(config_dict)
    config_dict = update_config_dict_from_arguments(config_dict)

    return config_dict
