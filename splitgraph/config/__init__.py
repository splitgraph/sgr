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

from .config import create_config_dict

CONFIG = create_config_dict()

PG_HOST = CONFIG["SG_ENGINE_HOST"]
PG_PORT = CONFIG["SG_ENGINE_PORT"]
PG_DB = CONFIG["SG_ENGINE_DB_NAME"]
PG_USER = CONFIG["SG_ENGINE_USER"]
PG_PWD = CONFIG["SG_ENGINE_PWD"]
SPLITGRAPH_META_SCHEMA: str = CONFIG["SG_META_SCHEMA"]
REGISTRY_META_SCHEMA = "registry_meta"
SPLITGRAPH_API_SCHEMA = "splitgraph_api"

FDW_CLASS = CONFIG["SG_FDW_CLASS"]

# This is a global variable that gets flipped to True by the Multicorn FDW class
# at startup. When we're running from within an engine as an FDW, we might need to use
# different connection parameters to connect to other engines. It's not trivial to detect
# whether we're running inside of an embedded Python otherwise and this variable needs to
# ultimately make it into all get_engine() constructors, so this is simpler than threading
# it through all calls that FDW makes.
IN_FDW = False
