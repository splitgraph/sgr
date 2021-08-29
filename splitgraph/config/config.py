# engine params
from typing import Any, Callable, Dict, Optional, Sequence, Union, cast

from .argument_config import get_argument_config_value
from .config_file_config import get_config_dict_from_config_file
from .default_config import get_default_config_value
from .environment_config import get_environment_config_value
from .keys import ALL_KEYS, KEYS, ConfigDict
from .system_config import get_system_config_value


def chain_getters(
    getters: Sequence[Callable[[str], Optional[str]]],
    key: str,
    default_return: Optional[str] = None,
) -> Optional[str]:
    for getter in getters:
        result = getter(key)
        if result is not None:
            return result
    return default_return


def lazy_get_config_value(
    key: str, default_return: Optional[str] = None
) -> Optional[Union[str, Dict[str, Dict[str, str]]]]:
    """
    Get the config value for a key in the following precedence
    Otherwise return default_return
    """

    if key not in ALL_KEYS:
        # For sections which can't be overridden via envvars/arguments,
        # we only use default values
        return chain_getters([get_default_config_value], key, default_return)

    return chain_getters(
        [
            get_argument_config_value,
            get_environment_config_value,
            get_system_config_value,
            get_default_config_value,
        ],
        key,
        default_return,
    )


def update_config_dict_from_arguments(config_dict: ConfigDict) -> ConfigDict:
    """
    Given an existing config_dict, update after reading sys.argv
    and overwriting any keys.

    Return updated copy of config_dict.
    """
    argument_config_dict = {
        k: get_argument_config_value(k, None)
        for k in KEYS
        if get_argument_config_value(k) is not None
    }
    new_config_dict = patch_config(config_dict, cast(ConfigDict, argument_config_dict))
    return new_config_dict


def update_config_dict_from_env_vars(config_dict: ConfigDict) -> ConfigDict:
    """
    Given an existing config_dict, update after reading os.environ
    and overwriting any keys.

    Return updated copy of config_dict.
    """

    argument_config_dict = {
        k: get_environment_config_value(k, None)
        for k in KEYS
        if get_environment_config_value(k) is not None
    }
    new_config_dict = patch_config(config_dict, cast(ConfigDict, argument_config_dict))

    return new_config_dict


def update_config_dict_from_file(config_dict: ConfigDict, sg_config_file: str) -> ConfigDict:
    """
    Given an existing config_dict, update after reading sg_config_file
    and overwriting any keys according to the rules in config_file_config

    Return updated copy of config_dict.
    """

    config_file_dict = get_config_dict_from_config_file(sg_config_file)
    new_config_dict = patch_config(config_dict, config_file_dict)

    return new_config_dict


def create_config_dict() -> ConfigDict:
    """
    Create and return a dict of all known config values
    """
    initial_dict = {k: lazy_get_config_value(k) for k in ALL_KEYS}
    config_dict = cast(ConfigDict, {k: v for k, v in initial_dict.items() if v is not None})
    try:
        sg_config_file = get_singleton(config_dict, "SG_CONFIG_FILE")
        config_dict = update_config_dict_from_file(config_dict, sg_config_file)
    except KeyError:
        pass
    config_dict = update_config_dict_from_env_vars(config_dict)
    config_dict = update_config_dict_from_arguments(config_dict)

    return config_dict


def patch_config(config: ConfigDict, patch: ConfigDict) -> ConfigDict:
    """
    Recursively updates a nested configuration dictionary:

    patch_config(
        {"key_1": "value_1",
         "dict_1": {"key_1": "value_1"}},
        {"key_1": "value_2",
         "dict_1": {"key_2": "value_2"}}) == \
        {"key_1": "value_2",
         "dict_1": {"key_1": "value_1", "key_2": "value_2"}}

    :param config: Config dictionary
    :param patch: Dictionary with the path
    :return: New patched dictionary
    """

    def _patch_internal(left: Dict[str, Any], right: Dict[str, Any]) -> Dict[str, Any]:
        result = left.copy()
        for key, value in right.items():
            if key in left and isinstance(left[key], dict) and isinstance(value, dict):
                result[key] = _patch_internal(left[key], value)
            else:
                result[key] = value
        return result

    return _patch_internal(config, patch)


def get_singleton(config: ConfigDict, item: str) -> str:
    """Return a singleton (not a section) variable from the config."""
    return str(config[item])


def get_all_in_section(config: ConfigDict, section: str) -> Dict[str, Union[str, Dict[str, str]]]:
    """
    Get all subsections from a config (e.g. config["data_sources"])
    """
    result: Dict[str, Union[str, Dict[str, str]]] = cast(
        Dict[str, Union[str, Dict[str, str]]], config.get(section, {})
    )
    assert isinstance(result, dict)
    return result


def get_all_in_subsection(config: ConfigDict, section: str, subsection: str) -> Dict[str, str]:
    section_dict = get_all_in_section(config, section)
    subsection_dict: Dict[str, str] = cast(Dict[str, str], section_dict.get(subsection, {}))
    assert isinstance(subsection_dict, dict)
    return subsection_dict


def get_from_subsection(config: ConfigDict, section: str, subsection: str, item: str) -> str:
    """Return a singleton variable from a subsection of the config,
    e.g. config["remotes"]["data.splitgraph.com"]["SG_ENGINE_HOST"]"""
    subsection_dict = get_all_in_subsection(config, section, subsection)
    return subsection_dict[item]


def get_from_section(config: ConfigDict, section: str, item: str) -> str:
    section_dict = get_all_in_section(config, section)
    assert isinstance(section_dict, dict)
    return cast(str, section_dict[item])


def set_in_subsection(
    config: ConfigDict, section: str, subsection: str, item: str, value: str
) -> None:
    """Set a singleton variable in a subsection of the config,
    e.g. config["remotes"]["data.splitgraph.com"]["SG_ENGINE_HOST"]"""
    subsection_dict = get_all_in_subsection(config, section, subsection)
    subsection_dict[item] = value
