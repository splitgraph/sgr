from .config import create_config_dict

import os

''' The CONFIG object is created and exported once __at import time__
    Calling CONFIG["KEY"] directly should be sufficient in most cases,
    except when a config value has changed since importing CONFIG.
    In that case, create_config_dict() can provide an updated config dict


    Priority (highest to lowest):

        1. Command line argument values
        2. Config file values in [defaults] section
        3. Environment variable values
        4. DEFAULTS (see keys.py)

'''
CONFIG = create_config_dict()
