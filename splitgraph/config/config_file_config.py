from configparser import ConfigParser, ExtendedInterpolation
from typing import Dict, Union, cast

from splitgraph.config.keys import ConfigDict


def hoist_section(
    config_dict: Dict[str, Dict[str, str]], section: str = "defaults"
) -> Dict[str, Union[Dict[str, str], str]]:
    """
        If a section exists called <hoist_section>, hoist it to the top level
        This is useful for overriding default configs from within a config file

        Transform `config_dict` to "hoist" any config values from a section
        into the top level (thus, overriding environment variables),
        when the name of the section matches `hoist_section`.

        Return a new, updated copy of `config_dict`.
    """

    if section not in config_dict.keys():
        # see https://github.com/python/mypy/issues/2300
        return cast(Dict[str, Union[Dict[str, str], str]], config_dict)

    hoisted_config_dict = cast(Dict[str, Union[Dict[str, str], str]], config_dict.copy())
    section_dict = config_dict[section].copy()
    del hoisted_config_dict[section]

    for k, v in section_dict.items():
        hoisted_config_dict[k.upper()] = v

    return hoisted_config_dict


def accumulate_lists(config_dict: Dict[str, Union[Dict[str, str], str]]) -> ConfigDict:
    """
        Transform a `config_dict` to "accumulate" objects "nested" via key name

        Because  ConfigParser does not support nesting, we implement our own
        syntax via the key names of sections. The ':' character can be used in
        section names to specify the left and right key. Example::

            .ini config             new config_dict

                              ----->
                                |
            [remote: remote1]   |   {
            SG_ENV_VAR=foo      |       **rest_of_config_dict,
                                |       "remotes": {
            [origin: origin1]   |           "remote1": {
            SG_ENV_VAR=bar      |               "SG_ENV_VAR": "foo"
                                |           }
            [origin: origin2]   |       },
            SG_ENV_VAR=bax      |       "origins": {
                                |           "origin1": {
                                |               "SG_ENV_VAR": "bar"
                                |           },
                                |           "origin2": {
                                |               "SG_ENV_VAR": "bax"
                                |           }
                                |       }
                                |   }
                                |
                              ----->

        :return a new, updated copy of `config_dict`
    """

    new_dict = cast(ConfigDict, config_dict.copy())

    accumulatable = {"remote": "remotes", "origin": "origins"}

    accumulatable_keys = accumulatable.keys()

    accumulated: Dict[str, Dict[str, Dict[str, str]]] = {}

    def key_matches(k):
        return k.split(":")[0] in accumulatable_keys

    matching_keys = [k for k in new_dict.keys() if key_matches(k)]

    keys_to_delete = []

    # e.g. key = "remote: remote1"
    for key in matching_keys:
        keys_to_delete.append(key)

        # e.g. "remote"
        left_key = key.split(":")[0]

        # e.g. "remote1" (to be used as a key in the new dict)
        right_key = "".join(key.split(":")[1:]).strip()

        list_key = accumulatable[left_key]
        new_item = cast(Dict[str, str], new_dict[key]).copy()
        if list_key not in accumulated.keys():
            accumulated[list_key] = {right_key: new_item}
        else:
            accumulated[list_key][right_key] = new_item

    for delete_key in keys_to_delete:
        del new_dict[delete_key]

    new_dict.update(accumulated)

    return new_dict


def transform_config_dict(config_dict: Dict[str, Dict[str, str]], **kwargs) -> ConfigDict:
    """
        Apply transformations to the raw ConfigParser.config object

            1) hoist_section
            2) accumulate_lists

        Return the a new, updated copy of `config_dict`.
    """

    config_dict = hoist_section(config_dict, **kwargs)
    config_dict = accumulate_lists(config_dict)
    return config_dict


# TODO: Get this cleaned up for parsing environment variables as default
#
# class EnvInterpolation(ExtendedInterpolation):
#     def before_get(self, parser, section, option, value, defaults):
#         return os.path.expandvars(value)
#         # if expanded == value:
#         #     return super(ExtendedInterpolation, self).before_get(parser, section, option, value, defaults)
#         # else:
#         #     return expanded


def get_config_dict_from_file(sg_file: str, **kwargs) -> Dict[str, Dict[str, str]]:
    # TODO
    # config = ConfigParser(interpolation=EnvInterpolation())
    config = ConfigParser(interpolation=ExtendedInterpolation())

    # Convert all keys to uppercase
    setattr(config, "optionxform", lambda option: option.upper())
    config.read(sg_file)

    config_dict = {s: dict(config.items(s, False)) for s in config.sections()}

    # Fix certain sections to have lower case by convention
    if "data_sources" in config_dict:
        handlers = config_dict["data_sources"]
        assert isinstance(handlers, dict)
        config_dict["data_sources"] = cast(
            Dict[str, str], {k.lower(): v for k, v in handlers.items()}
        )

    return config_dict


def get_config_dict_from_config_file(sg_file: str, **kwargs) -> ConfigDict:
    """
        Create a dict from ConfigParser, apply transformations to it.

        Return parsed and transformed `config_dict`.
    """

    config_dict = get_config_dict_from_file(sg_file, **kwargs)
    config_dict = transform_config_dict(config_dict, **kwargs)

    return config_dict
