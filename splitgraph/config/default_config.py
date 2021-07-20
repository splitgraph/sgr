from typing import Any, Optional

from .keys import DEFAULTS


def get_default_config_value(key: str, default_return: Optional[str] = None) -> Any:
    """Get the hard-coded default value of a config key.
    Otherwise return default_return.
    """

    return DEFAULTS.get(key, default_return)
