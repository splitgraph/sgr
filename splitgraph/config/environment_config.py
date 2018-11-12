import os


def get_environment_config_value(key, default_return=None):
    """ Get the environment variable value of the environment variable matching key.
        Otherwise return default_return.
    """

    return os.environ.get(key, default_return)
