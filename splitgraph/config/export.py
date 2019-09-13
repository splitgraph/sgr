"""Routines for exporting the config back into text."""
from splitgraph.config.keys import SENSITIVE_KEYS, KEYS, DEFAULTS


def _kv_to_str(key, value, no_shielding):
    if not value:
        value_str = ""
    elif key in SENSITIVE_KEYS and not no_shielding:
        value_str = value[0] + "*******"
    else:
        value_str = value
    return "%s=%s" % (key, value_str)


def serialize_engine_config(engine_name, conn_params, no_shielding):
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
_situational_params = ["SG_ENGINE", "SG_CONFIG_FILE"]


def serialize_config(config, config_format, no_shielding, include_defaults=True):
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
        if config_format and key in _situational_params:
            continue
        if include_defaults or key not in DEFAULTS or config[key] != DEFAULTS[key]:
            result += _kv_to_str(key, config[key], no_shielding) + "\n"

    # Emit hoisted remotes
    result += "\nCurrent registered remote engines:\n" if not config_format else ""
    for remote in config.get("remotes", []):
        if config_format:
            result += (
                "\n"
                + serialize_engine_config(remote, config["remotes"][remote], no_shielding)
                + "\n"
            )
        else:
            result += "\n%s:\n" % remote
            for key, value in config["remotes"][remote].items():
                result += _kv_to_str(key, value, no_shielding) + "\n"

    # Print Splitfile commands
    if "commands" in config:
        result += "\nSplitfile command plugins:\n" if not config_format else "[commands]\n"
        for command_name, command_class in config["commands"].items():
            result += _kv_to_str(command_name, command_class, no_shielding) + "\n"

    # Print mount handlers
    if "mount_handlers" in config:
        result += "\nFDW Mount handlers:\n" if not config_format else "[mount_handlers]\n"
        for handler_name, handler_func in config["mount_handlers"].items():
            result += _kv_to_str(handler_name, handler_func.lower(), no_shielding) + "\n"

    # Print external object handlers
    if "external_handlers" in config:
        result += "\nExternal object handlers:\n" if not config_format else "[external_handlers]\n"
        for handler_name, handler_func in config["external_handlers"].items():
            result += _kv_to_str(handler_name, handler_func, no_shielding) + "\n"

    return result


def patch_config(config, patch):
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
    result = config.copy()
    for key, value in patch.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = patch_config(result[key], value)
        else:
            result[key] = value
    return result
