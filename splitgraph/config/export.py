"""Routines for exporting the config back into text."""
from typing import Dict, cast

from splitgraph.config import get_singleton
from splitgraph.config.config import get_all_in_section
from splitgraph.config.keys import DEFAULTS, KEYS, SENSITIVE_KEY_SUFFIXES, ConfigDict


def _kv_to_str(key: str, value: str, no_shielding: bool) -> str:
    value_str = str(value) or ""
    if any(key.endswith(s) for s in SENSITIVE_KEY_SUFFIXES) and not no_shielding:
        value_str = value_str[:1] + "*******"
    return "%s=%s" % (key, value_str)


def serialize_engine_config(
    engine_name: str, conn_params: Dict[str, str], no_shielding: bool
) -> str:
    """
    Output the config section with connection parameters for a single engine.

    :param engine_name: Name of the engine
    :param conn_params: Dictionary of connection parameters
    :param no_shielding: Don't replace passwords with asterisks
    """

    result = "[remote: %s]\n" % engine_name
    result += "\n".join(_kv_to_str(key, value, no_shielding) for key, value in conn_params.items())
    return result


# Parameters that aren't really supposed to be in a config file,
# so we skip them when emitting in the config format.
_SITUATIONAL_PARAMS = ["SG_ENGINE", "SG_CONFIG_FILE"]


def serialize_config(
    config: ConfigDict, config_format: bool, no_shielding: bool, include_defaults: bool = True
) -> str:
    """
    Pretty-print the configuration or print it in the Splitgraph config file format.

    :param config: Configuration dictionary.
    :param config_format: Output configuration in the Splitgraph config file format.
    :param no_shielding: Don't replace sensitive values (like passwords) with asterisks
    :param include_defaults: Emit the config variable even if it's the same as the default.
    :return: Textual representation of the config.
    """

    result = "[defaults]\n" if config_format else ""

    # Emit normal config parameters
    for key in KEYS:
        if config_format and key in _SITUATIONAL_PARAMS:
            continue
        value = get_singleton(config, key)
        if include_defaults or key not in DEFAULTS or value != DEFAULTS[key]:
            result += _kv_to_str(key, value, no_shielding) + "\n"

    # Emit hoisted remotes
    result += "\nCurrent registered remote engines:\n" if not config_format else ""
    for remote, remote_config in get_all_in_section(config, "remotes").items():
        assert isinstance(remote_config, dict)
        if config_format:
            result += "\n" + serialize_engine_config(remote, remote_config, no_shielding) + "\n"
        else:
            result += "\n%s:\n" % remote
            for key, value in remote_config.items():
                result += _kv_to_str(key, value, no_shielding) + "\n"

    # Print Splitfile commands
    if "commands" in config:
        result += "\nSplitfile command plugins:\n" if not config_format else "[commands]\n"
        for command_name, command_class in get_all_in_section(config, "commands").items():
            result += _kv_to_str(command_name, cast(str, command_class), no_shielding) + "\n"

    # Print data sources
    if "data_sources" in config:
        result += "\nData sources:\n" if not config_format else "[data_sources]\n"
        for handler_name, handler_func in get_all_in_section(config, "data_sources").items():
            result += _kv_to_str(handler_name, cast(str, handler_func), no_shielding) + "\n"

    # Print external object handlers
    if "external_handlers" in config:
        result += "\nExternal object handlers:\n" if not config_format else "[external_handlers]\n"
        for handler_name, handler_func in get_all_in_section(config, "external_handlers").items():
            result += _kv_to_str(handler_name, cast(str, handler_func), no_shielding) + "\n"

    return result


def overwrite_config(
    new_config: ConfigDict, config_path: str, include_defaults: bool = False
) -> None:
    """
    Serialize the new config dictionary and overwrite the current config file.
    Note: this will delete all comments in the config!

    :param new_config: Config dictionary.
    :param config_path: Path to the config file.
    :param include_defaults: Whether to include values that are the same
        as their defaults.
    """
    new_config_data = serialize_config(
        new_config, config_format=True, no_shielding=True, include_defaults=include_defaults
    )
    with open(config_path, "w") as f:
        f.write(new_config_data)
