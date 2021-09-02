import logging
import os
from pathlib import Path

from splitgraph.config.config import patch_config
from splitgraph.config.export import overwrite_config
from splitgraph.config.system_config import HOME_SUB_DIR


def patch_and_save_config(config, patch):

    config_path = config["SG_CONFIG_FILE"]
    if not config_path:
        # Default to creating a config in the user's homedir rather than local.
        config_dir = Path(os.environ["HOME"]) / Path(HOME_SUB_DIR)
        config_path = config_dir / Path(".sgconfig")
        logging.debug("No config file detected, creating one at %s" % config_path)
        config_dir.mkdir(exist_ok=True, parents=True)
    else:
        logging.debug("Updating the existing config file at %s" % config_path)
    new_config = patch_config(config, patch)
    overwrite_config(new_config, config_path)
    return str(config_path)
