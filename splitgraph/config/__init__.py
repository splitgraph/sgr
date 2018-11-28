from .config import create_config_dict

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
CONFIG = create_config_dict()

PG_HOST = CONFIG["SG_DRIVER_HOST"]
PG_PORT = CONFIG["SG_DRIVER_PORT"]
PG_DB = CONFIG["SG_DRIVER_DB_NAME"]
PG_USER = CONFIG["SG_DRIVER_USER"]
PG_PWD = CONFIG["SG_DRIVER_PWD"]
POSTGRES_CONNECTION = CONFIG["SG_DRIVER_CONNECTION_STRING"]
SPLITGRAPH_META_SCHEMA = CONFIG["SG_META_SCHEMA"]
REGISTRY_META_SCHEMA = "registry_meta"
