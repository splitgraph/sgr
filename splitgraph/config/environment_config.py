import os


from typing import Optional


def get_environment_config_value(key: str, default_return: Optional[str] = None) -> Optional[str]:
    """ Get the environment variable value of the environment variable matching key.
        Otherwise return default_return.
    """

    return os.environ.get(key, default_return)
