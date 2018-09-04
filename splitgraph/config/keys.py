DEFAULTS = {
    "SG_NAMESPACE": "sg-default-ns",
    "SG_DRIVER_CONNECTION_STRING": None,
    "SG_DRIVER_HOST": None,
    "SG_DRIVER_PORT": 5432,
    "SG_DRIVER_DB_NAME": "sg-default-ns",
    "SG_DRIVER_USER": None,
    "SG_DRIVER_PWD": None,
    "SG_DRIVER_ADMIN_USER": None,
    "SG_DRIVER_ADMIN_PWD": None,
    "SG_DRIVER_POSTGRES_DB_NAME": "postgres",
    "SG_CONFIG_FILE": None,
    "SG_META_SCHEMA": "splitgraph_meta",
    "SG_CONFIG_DIRS": None,
    "SG_CONFIG_DIR": None,
    "SG_REPO_LOOKUP": "",
    "SG_REPO_LOOKUP_OVERRIDE": "",
}

KEYS = list(DEFAULTS.keys())

''' Warning: Every key in DEFAULTS must have a key in ARGUMENT_KEY_MAP
    If you add/remove keys from DEFAULTS, make sure to do so here too.
'''
ARGUMENT_KEY_MAP = {
    "--namespace": "SG_NAMESPACE",
    "--driver-connection-string": "SG_DRIVER_CONNECTION_STRING",
    "--driver-host": "SG_DRIVER_HOST",
    "--driver-port": "SG_DRIVER_PORT",
    "--driver-db-name": "SG_DRIVER_DB_NAME",
    "--driver-user": "SG_DRIVER_USER",
    "--driver-pwd": "SG_DRIVER_PWD",
    "--driver-admin-user": "SG_DRIVER_ADMIN_USER",
    "--driver-admin-pwd": "SG_DRIVER_ADMIN_PWD",
    "--driver-postgres-db-name": "SG_DRIVER_POSTGRES_DB_NAME",
    "--config-file": "SG_CONFIG_FILE",
    "--meta-schema": "SG_META_SCHEMA",
    "--config-dirs": "SG_CONFIG_DIRS",
    "--config-dir": "SG_CONFIG_DIR",
    "--repo-lookup-path": "SG_REPO_LOOKUP",
    "--repo-lookup-override": "SG_REPO_LOOKUP_OVERRIDE"
}

ARG_KEYS = list(ARGUMENT_KEY_MAP.keys())

# Reverse of ARGUMENT_KEY_MAP
KEY_ARGUMENT_MAP = {v: k for k, v in ARGUMENT_KEY_MAP.items()}

# ini keys that override environment keys must be same (including SG_)
