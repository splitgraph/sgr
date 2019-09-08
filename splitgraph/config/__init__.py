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
import logging

from .config import create_config_dict

CONFIG = create_config_dict()

PG_HOST = CONFIG["SG_ENGINE_HOST"]
PG_PORT = CONFIG["SG_ENGINE_PORT"]
PG_DB = CONFIG["SG_ENGINE_DB_NAME"]
PG_USER = CONFIG["SG_ENGINE_USER"]
PG_PWD = CONFIG["SG_ENGINE_PWD"]
SPLITGRAPH_META_SCHEMA = CONFIG["SG_META_SCHEMA"]
REGISTRY_META_SCHEMA = "registry_meta"
SPLITGRAPH_API_SCHEMA = "splitgraph_api"

# Log timestamp and PID. By default we only log WARNINGs in the command line interface.
logging.basicConfig(
    format="%(asctime)s [%(process)d] %(levelname)s %(message)s", level=CONFIG["SG_LOGLEVEL"]
)
FDW_CLASS = CONFIG["SG_FDW_CLASS"]
