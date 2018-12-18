DEFAULTS = {
    "SG_NAMESPACE": "sg-default-ns",
    "SG_ENGINE_HOST": "localhost",
    "SG_ENGINE_PORT": 5432,
    "SG_ENGINE_DB_NAME": "splitgraph",
    "SG_ENGINE_USER": "sgr",
    "SG_ENGINE_PWD": "supersecure",
    "SG_ENGINE_ADMIN_USER": "sgr",
    "SG_ENGINE_ADMIN_PWD": "supersecure",
    "SG_ENGINE_POSTGRES_DB_NAME": "postgres",
    "SG_CONFIG_FILE": None,
    "SG_META_SCHEMA": "splitgraph_meta",
    "SG_CONFIG_DIRS": None,
    "SG_CONFIG_DIR": None,
    "SG_REPO_LOOKUP": "",
    "SG_REPO_LOOKUP_OVERRIDE": "",
    "SG_S3_HOST": "localhost",
    "SG_S3_PORT": "9000",
    # Anonymous access by default
    "SG_S3_KEY": None,
    "SG_S3_PWD": None,
}

KEYS = list(DEFAULTS.keys())
# Keys whose contents we don't print fully
SENSITIVE_KEYS = [k for k in KEYS if '_PWD' in k]

""" Warning: Every key in DEFAULTS must have a key in ARGUMENT_KEY_MAP
    If you add/remove keys from DEFAULTS, make sure to do so here too.
"""
ARGUMENT_KEY_MAP = {
    "--namespace": "SG_NAMESPACE",
    "--engine-host": "SG_ENGINE_HOST",
    "--engine-port": "SG_ENGINE_PORT",
    "--engine-db-name": "SG_ENGINE_DB_NAME",
    "--engine-user": "SG_ENGINE_USER",
    "--engine-pwd": "SG_ENGINE_PWD",
    "--engine-admin-user": "SG_ENGINE_ADMIN_USER",
    "--engine-admin-pwd": "SG_ENGINE_ADMIN_PWD",
    "--engine-postgres-db-name": "SG_ENGINE_POSTGRES_DB_NAME",
    "--config-file": "SG_CONFIG_FILE",
    "--meta-schema": "SG_META_SCHEMA",
    "--config-dirs": "SG_CONFIG_DIRS",
    "--config-dir": "SG_CONFIG_DIR",
    "--repo-lookup-path": "SG_REPO_LOOKUP",
    "--repo-lookup-override": "SG_REPO_LOOKUP_OVERRIDE",
    "--s3-host": "SG_S3_HOST",
    "--s3-port": "SG_S3_PORT",
    "--s3-access-key": "SG_S3_KEY",
    "--s3-secret-key": "SG_S3_PWD"
}

ARG_KEYS = list(ARGUMENT_KEY_MAP.keys())

# Reverse of ARGUMENT_KEY_MAP
KEY_ARGUMENT_MAP = {v: k for k, v in ARGUMENT_KEY_MAP.items()}

# ini keys that override environment keys must be same (including SG_)
