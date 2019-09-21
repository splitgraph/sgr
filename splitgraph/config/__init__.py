""" The CONFIG object is created and exported once __at import time__
    Calling CONFIG["KEY"] directly should be sufficient in most cases,
    except when a config value has changed since importing CONFIG.
    In that case, create_config_dict() can provide an updated config dict


    Priority (highest to lowest):

        1. Command line argument values
        2. Environment variable values
        3. Config file values in [defaults] section
        4. DEFAULTS (see keys.py)

"""
from typing import Dict, Union, cast

from splitgraph.config.keys import ConfigDict
from .config import create_config_dict

CONFIG = create_config_dict()


# Type-safe functions for getting values from the config


def get_singleton(config: ConfigDict, item: str) -> str:
    """Return a singleton (not a section) variable from the config."""
    result = config[item]
    assert isinstance(result, str)
    return result


def get_all_in_section(config: ConfigDict, section: str) -> Dict[str, Union[str, Dict[str, str]]]:
    """
    Get all subsections from a config (e.g. config["mount_handlers"])
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


PG_HOST = get_singleton(CONFIG, "SG_ENGINE_HOST")
PG_PORT = get_singleton(CONFIG, "SG_ENGINE_PORT")
PG_DB = get_singleton(CONFIG, "SG_ENGINE_DB_NAME")
PG_USER = get_singleton(CONFIG, "SG_ENGINE_USER")
PG_PWD = get_singleton(CONFIG, "SG_ENGINE_PWD")

SPLITGRAPH_META_SCHEMA = get_singleton(CONFIG, "SG_META_SCHEMA")
REGISTRY_META_SCHEMA = "registry_meta"
SPLITGRAPH_API_SCHEMA = "splitgraph_api"

FDW_CLASS = get_singleton(CONFIG, "SG_FDW_CLASS")

# This is a global variable that gets flipped to True by the Multicorn FDW class
# at startup. When we're running from within an engine as an FDW, we might need to use
# different connection parameters to connect to other engines. It's not trivial to detect
# whether we're running inside of an embedded Python otherwise and this variable needs to
# ultimately make it into all get_engine() constructors, so this is simpler than threading
# it through all calls that FDW makes.
IN_FDW = False
