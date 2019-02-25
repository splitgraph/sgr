DEFAULTS = {
    # Logging threshold (log messages not emitted below this). Accepted values are
    # CRITICAL, ERROR, WARNING, INFO and DEBUG.
    "SG_LOGLEVEL": "WARNING",
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
    # Object cache (objects downloaded from an external location) tuning
    # Will try to target this size (in MB).
    "SG_OBJECT_CACHE_SIZE": 1024,
    # Significance of recent usage time and object size in cache eviction.
    # See splitgraph.core.object_manager for an explanation.
    "SG_EVICTION_DECAY": 0.002,
    "SG_EVICTION_FLOOR": 1,
    # Times the object manager returns a DIFF chain in a given window of time before it materializes
    # it instead (to speed up layered querying).
    "SG_SNAP_CACHE_MISSES": 5,
    "SG_SNAP_CACHE_LOOKBACK": 300
}

KEYS = list(DEFAULTS.keys())
# Keys whose contents we don't print fully
SENSITIVE_KEYS = [k for k in KEYS if '_PWD' in k]

""" Warning: Every key in DEFAULTS must have a key in ARGUMENT_KEY_MAP
    If you add/remove keys from DEFAULTS, make sure to do so here too.
"""
ARGUMENT_KEY_MAP = {
    "--loglevel": "SG_LOGLEVEL",
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
    "--s3-secret-key": "SG_S3_PWD",
    "--object-cache-size": "SG_OBJECT_CACHE_SIZE",
    "--eviction-decay": "SG_EVICTION_DECAY",
    "--eviction-floor": "SG_EVICTION_FLOOR",
    "--cache-misses-for-snap": "SG_SNAP_CACHE_MISSES",
    "--cache-misses-lookback": "SG_SNAP_CACHE_LOOKBACK",
}

ARG_KEYS = list(ARGUMENT_KEY_MAP.keys())

# Reverse of ARGUMENT_KEY_MAP
KEY_ARGUMENT_MAP = {v: k for k, v in ARGUMENT_KEY_MAP.items()}

# ini keys that override environment keys must be same (including SG_)
