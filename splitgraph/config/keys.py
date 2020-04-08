from typing import Dict, Union

# Define a ConfigDict: it's a nested dictionary of strings but so far
# we only go down max 2 levels (config["remotes"]["data.splitgraph.com"][param])
# so we enumerate them all explicitly.
ConfigDict = Dict[str, Union[str, Dict[str, str], Dict[str, Dict[str, str]]]]

DEFAULTS: ConfigDict = {
    "SG_ENGINE": "",
    # Logging threshold (log messages not emitted below this). Accepted values are
    # CRITICAL, ERROR, WARNING, INFO and DEBUG.
    "SG_LOGLEVEL": "WARNING",
    # Prefix for Docker containers that are treated as Splitgraph engines.
    "SG_ENGINE_PREFIX": "splitgraph_engine_",
    "SG_NAMESPACE": "sg-default-ns",
    # Whether this engine is a registry (access only via the API) or
    # a personal Splitgraph engine
    "SG_IS_REGISTRY": "",
    # Whether to check the version of Splitgraph core installed on this engine
    # when we first connect to it.
    "SG_CHECK_VERSION": "true",
    "SG_ENGINE_FDW_HOST": "localhost",
    "SG_ENGINE_FDW_PORT": "5432",
    "SG_ENGINE_HOST": "localhost",
    "SG_ENGINE_PORT": "5432",
    "SG_ENGINE_DB_NAME": "splitgraph",
    "SG_ENGINE_USER": "sgr",
    "SG_ENGINE_PWD": "supersecure",
    "SG_ENGINE_ADMIN_USER": "sgr",
    "SG_ENGINE_ADMIN_PWD": "supersecure",
    "SG_ENGINE_POSTGRES_DB_NAME": "postgres",
    "SG_ENGINE_OBJECT_PATH": "/var/lib/splitgraph/objects",
    # Postgres query planner configuration for Splitfile execution/table imports that uses layered
    # querying a lot. Multicorn/LQFDW currently doesn't give good estimates for GroupBy
    # aggregations (returns 1 distinct group) which makes Postgres use a Sort + GroupAgg
    # aggregation method, so we force it to use hash aggregations for that. This speeds up the
    # US election dataset build by about 30% (82s -> 56s) for the version that uses FROM IMPORT and
    # by about 50% (101s -> 53s) for the version that runs a single big join against multiple images.
    "SG_LQ_TUNING": "SET enable_sort=off; SET enable_hashagg=on;",
    # Size of the connection pool used to download/upload objects + talk to the engine
    "SG_ENGINE_POOL": "16",
    "SG_CONFIG_FILE": "",
    "SG_META_SCHEMA": "splitgraph_meta",
    "SG_CONFIG_DIRS": "",
    "SG_CONFIG_DIR": "",
    "SG_REPO_LOOKUP": "",
    "SG_REPO_LOOKUP_OVERRIDE": "",
    "SG_S3_HOST": "//localhost",
    "SG_S3_PORT": "9000",
    "SG_S3_SECURE": "false",
    # Anonymous S3 access by default
    "SG_S3_BUCKET": "splitgraph",
    "SG_S3_KEY": "",
    "SG_S3_PWD": "",
    # Object cache (objects downloaded from an external location) tuning
    # Will try to target this size (in MB).
    "SG_OBJECT_CACHE_SIZE": "10240",
    # Significance of recent usage time and object size in cache eviction.
    # See splitgraph.core.object_manager for an explanation.
    "SG_EVICTION_DECAY": "0.002",
    "SG_EVICTION_FLOOR": "1",
    "SG_EVICTION_MIN_FRACTION": "0.05",
    "SG_FDW_CLASS": "splitgraph.core.fdw_checkout.QueryingForeignDataWrapper",
    # sgr commandline config
    # Set to true to disable Unicode progressbars (done in asciinema recording).
    "SG_CMD_ASCII": "false",
    # Some default sections: these can't be overridden via envvars.
    # Default remote engine (data.splitgraph.com).
    # No credentials here: they are fetched from data.splitgraph.com/auth
    # at registration time.
    "remotes": {
        "data.splitgraph.com": {
            "SG_IS_REGISTRY": "true",
            "SG_ENGINE_HOST": "data.splitgraph.com",
            "SG_ENGINE_PORT": "5432",
            "SG_ENGINE_DB_NAME": "sgregistry",
            "SG_AUTH_API": "https://api.splitgraph.com/auth",
            "SG_QUERY_API": "https://data.splitgraph.com",
        }
    },
    "external_handlers": {"S3": "splitgraph.hooks.s3.S3ExternalObjectHandler"},
}

ALL_KEYS = list(DEFAULTS.keys())
KEYS = [k for k in ALL_KEYS if k not in ["remotes", "external_handlers"]]
# Keys whose contents we don't print fully
SENSITIVE_KEY_SUFFIXES = ["_PWD", "_TOKEN"]

""" Warning: Every key in DEFAULTS must have a key in ARGUMENT_KEY_MAP
    If you add/remove keys from DEFAULTS, make sure to do so here too.
"""
ARGUMENT_KEY_MAP = {
    "--engine": "SG_ENGINE",
    "--loglevel": "SG_LOGLEVEL",
    "--namespace": "SG_NAMESPACE",
    "--engine-fdw-host": "SG_ENGINE_FDW_HOST",
    "--engine-fdw-port": "SG_ENGINE_FDW_PORT",
    "--engine-host": "SG_ENGINE_HOST",
    "--engine-port": "SG_ENGINE_PORT",
    "--engine-db-name": "SG_ENGINE_DB_NAME",
    "--engine-user": "SG_ENGINE_USER",
    "--engine-pwd": "SG_ENGINE_PWD",
    "--engine-admin-user": "SG_ENGINE_ADMIN_USER",
    "--engine-admin-pwd": "SG_ENGINE_ADMIN_PWD",
    "--engine-postgres-db-name": "SG_ENGINE_POSTGRES_DB_NAME",
    "--engine-object-path": "SG_ENGINE_OBJECT_PATH",
    "--engine-pool": "SG_ENGINE_POOL",
    "--config-file": "SG_CONFIG_FILE",
    "--meta-schema": "SG_META_SCHEMA",
    "--config-dirs": "SG_CONFIG_DIRS",
    "--config-dir": "SG_CONFIG_DIR",
    "--repo-lookup-path": "SG_REPO_LOOKUP",
    "--repo-lookup-override": "SG_REPO_LOOKUP_OVERRIDE",
    "--s3-host": "SG_S3_HOST",
    "--s3-port": "SG_S3_PORT",
    "--s3-access-key": "SG_S3_KEY",
    "--s3-secret-key": "SG_S3_PWD",
    "--s3-bucket": "SG_S3_BUCKET",
    "--object-cache-size": "SG_OBJECT_CACHE_SIZE",
    "--eviction-decay": "SG_EVICTION_DECAY",
    "--eviction-floor": "SG_EVICTION_FLOOR",
    "--eviction-fraction": "SG_EVICTION_MIN_FRACTION",
    "--fdw-class": "SG_FDW_CLASS",
}

ARG_KEYS = list(ARGUMENT_KEY_MAP.keys())

# Reverse of ARGUMENT_KEY_MAP
KEY_ARGUMENT_MAP = {v: k for k, v in ARGUMENT_KEY_MAP.items()}

# ini keys that override environment keys must be same (including SG_)
