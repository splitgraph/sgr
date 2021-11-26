from typing import Dict, Union

# Define a ConfigDict: it's a nested dictionary of strings but so far
# we only go down max 2 levels (config["remotes"]["data.splitgraph.com"][param])
# so we enumerate them all explicitly.
ConfigDict = Dict[str, Union[str, Dict[str, str], Dict[str, Dict[str, str]]]]

DEFAULTS: ConfigDict = {
    "SG_ENGINE": "",
    "SG_LOGLEVEL": "WARNING",
    "SG_ENGINE_PREFIX": "splitgraph_engine_",
    "SG_NAMESPACE": "",
    "SG_IS_REGISTRY": "",
    "SG_CHECK_VERSION": "true",
    "SG_ENGINE_FDW_HOST": "localhost",
    "SG_ENGINE_FDW_PORT": "5432",
    "SG_ENGINE_HOST": "localhost",
    "SG_ENGINE_PORT": "5432",
    "SG_ENGINE_DB_NAME": "splitgraph",
    "SG_ENGINE_USER": "sgr",
    "SG_ENGINE_PWD": "supersecure",
    "SG_ENGINE_ADMIN_USER": "",
    "SG_ENGINE_ADMIN_PWD": "",
    "SG_ENGINE_POSTGRES_DB_NAME": "postgres",
    "SG_ENGINE_OBJECT_PATH": "/var/lib/splitgraph/objects",
    # Multicorn/LQFDW currently doesn't give good estimates for GroupBy
    # aggregations (returns 1 distinct group) which makes Postgres use a Sort + GroupAgg
    # aggregation method, so we force it to use hash aggregations for that. This speeds up the
    # US election dataset build by about 30% (82s -> 56s) for the version that uses FROM IMPORT and
    # by about 50% (101s -> 53s) for the version that runs a single big join against multiple images.
    "SG_LQ_TUNING": "SET enable_sort=off; SET enable_hashagg=on;",
    "SG_COMMIT_CHUNK_SIZE": "10000",
    "SG_ENGINE_POOL": "16",
    "SG_CONFIG_FILE": "",
    "SG_META_SCHEMA": "splitgraph_meta",
    "SG_CONFIG_DIRS": "",
    "SG_CONFIG_DIR": "",
    "SG_REPO_LOOKUP": "",
    "SG_REPO_LOOKUP_OVERRIDE": "",
    "SG_S3_HOST": "localhost",
    "SG_S3_PORT": "9000",
    "SG_S3_SECURE": "false",
    "SG_S3_BUCKET": "splitgraph",
    "SG_S3_KEY": "",
    "SG_S3_PWD": "",
    "SG_OBJECT_CACHE_SIZE": "10240",
    "SG_EVICTION_DECAY": "0.002",
    "SG_EVICTION_FLOOR": "1",
    "SG_EVICTION_MIN_FRACTION": "0.05",
    "SG_FDW_CLASS": "splitgraph.core.fdw_checkout.QueryingForeignDataWrapper",
    "SG_CMD_ASCII": "false",
    # Update checks and metrics
    "SG_UPDATE_REMOTE": "data.splitgraph.com",
    "SG_UPDATE_FREQUENCY": "86400",
    "SG_UPDATE_LAST": "0",
    "SG_UPDATE_ANONYMOUS": "false",
    "SG_PLUGIN_DIR": "",
    # Some default sections: these can't be overridden via envvars.
    "external_handlers": {"S3": "splitgraph.hooks.s3.S3ExternalObjectHandler"},
    "data_sources": {
        "postgres_fdw": "splitgraph.hooks.data_source.PostgreSQLDataSource",
        "mongo_fdw": "splitgraph.hooks.data_source.MongoDataSource",
        "mysql_fdw": "splitgraph.hooks.data_source.MySQLDataSource",
        "socrata": "splitgraph.ingestion.socrata.mount.SocrataDataSource",
        "elasticsearch": "splitgraph.hooks.data_source.ElasticSearchDataSource",
        "csv": "splitgraph.ingestion.csv.CSVDataSource",
        "snowflake": "splitgraph.ingestion.snowflake.SnowflakeDataSource",
        "dbt": "splitgraph.ingestion.dbt.data_source.DBTDataSource",
    },
}

ALL_KEYS = list(DEFAULTS.keys())
KEYS = [k for k in ALL_KEYS if k not in ["remotes", "external_handlers", "data_sources"]]
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

# Dictionary of keys to a Markdown snippet with documentation. This is compiled
# into a Markdown page referencing the configuration at release time.
# A unit test enforces that each key in KEYS has a documentation in KEY_DOCS.
KEY_DOCS: Dict[str, str] = {
    "SG_ENGINE": """Current engine name in use by Splitgraph. By default, this is the local engine.

This can be overridden to make `sgr` use a different engine in cases where the `--remote` flag is not supported.""",
    "SG_LOGLEVEL": """Logging threshold (log messages not emitted below this).  
Accepted values are CRITICAL, ERROR, WARNING, INFO and DEBUG.  
This can also be changed by passing `--verbosity` to `sgr`, e.g. `sgr --verbosity DEBUG init`.""",
    "SG_ENGINE_PREFIX": "Prefix for Docker containers that are treated as Splitgraph engines by `sgr engine`.",
    "SG_NAMESPACE": "Namespace used by default when pushing to this engine, if not explicitly specified. Normally this is set to the user's username on the registry.",
    "SG_IS_REGISTRY": "Whether this engine is a registry (access only via the SQL API) or an actual Splitgraph engine that supports checkouts.",
    "SG_CHECK_VERSION": "Whether to check the version of the Splitgraph library installed on this engine when a connection to it is first made.",
    "SG_ENGINE_FDW_HOST": "Hostname to use for this engine when it's connecting to itself (during layered querying).",
    "SG_ENGINE_FDW_PORT": "Port to use for this engine when it's connecting to itself (during layered querying).",
    "SG_ENGINE_HOST": "Hostname to use for sgr to connect to the engine.",
    "SG_ENGINE_PORT": "Port to use for sgr to connect to the engine.",
    "SG_ENGINE_DB_NAME": "Database used by Splitgraph.",
    "SG_ENGINE_USER": "Username used by sgr.",
    "SG_ENGINE_PWD": "Password used by sgr.",
    "SG_ENGINE_ADMIN_USER": "Superuser username for the engine, used to first initialize it and create the required Splitgraph schemata and extensions. The one user created on the standard engine is also a superuser, so you only need to override this if you created more than one user on your engine.",
    "SG_ENGINE_ADMIN_PWD": "Superuser password for the engine, used to first initialize it and create the required Splitgraph schemata and extensions. Defaults to the same as SG_ENGINE_PWD.",
    "SG_ENGINE_POSTGRES_DB_NAME": "Name of the default database that the superuser connects to to initialize Splitgraph.",
    "SG_ENGINE_OBJECT_PATH": "Path on the engine's filesystem where Splitgraph physical object files are stored.",
    "SG_LQ_TUNING": "Postgres query planner configuration for Splitfile execution and table imports. This is run before a layered query is executed and allows to tune query planning in case of LQ performance issues. For possible values, see the [PostgreSQL documentation](https://www.postgresql.org/docs/12/runtime-config-query.html).",
    "SG_COMMIT_CHUNK_SIZE": "Default chunk size when `sgr commit` is run. Can be overriden in the command line client by passing `--chunk-size`",
    "SG_ENGINE_POOL": "Size of the connection pool used to download/upload objects. Note that in the case of layered querying with joins on multiple tables, each table will use this many parallel threads to download objects, which can overwhelm the engine. Decrease this value in that case.",
    "SG_CONFIG_FILE": "Location of the Splitgraph configuration file. By default, Splitgraph looks for the configuration in `~/.splitgraph/.sgconfig` and then the current directory.",
    "SG_META_SCHEMA": "Name of the metadata schema. Note that whilst this can be changed, it hasn't been tested and won't be taken into account by engines connecting to this one.",
    "SG_CONFIG_DIRS": "List of directories used to look up the configuration file.",
    "SG_CONFIG_DIR": "Directory the current configuration file is located in.",
    "SG_REPO_LOOKUP": "List of remote names, comma-separated, used for repository lookups during Splitfile execution and `sgr clone` (if a remote name is not specified explicitly).",
    "SG_REPO_LOOKUP_OVERRIDE": "List of overrides for remote engines for some repositories. For example, `override_repo_1:local,override_repo_2:data.splitgraph.com`.",
    "SG_S3_HOST": "Hostname used by the remote engine for object storage. Note that the S3 settings are only used by the remote engine when constructing the URL to give to the client wishing to download/upload objects to S3, not by `sgr` itself.",
    "SG_S3_PORT": "Port used by the remote engine for object storage.",
    "SG_S3_SECURE": "Whether to use HTTPS for object storage.",
    "SG_S3_BUCKET": "S3 bucket used by the engine for object storage.",
    "SG_S3_KEY": "S3 access key.",
    "SG_S3_PWD": "S3 secure key.",
    "SG_OBJECT_CACHE_SIZE": "Object cache size, in megabytes. This only concerns objects downloaded from an external location or a remote engine. When there is no space in the object cache, an eviction is run and objects that haven't been used recently or that are small enough to be easily redownloaded are deleted to free up space.",
    "SG_EVICTION_DECAY": "Significance of recent usage time and object size in cache eviction. See documentation for splitgraph.core.object_manager for an explanation.",
    "SG_EVICTION_FLOOR": "Significance of recent usage time and object size in cache eviction. See documentation for splitgraph.core.object_manager for an explanation.",
    "SG_EVICTION_MIN_FRACTION": "Minimum fraction of the total cache size that has to get freed when an eviction is run. This is to avoid frequent evictions.",
    "SG_FDW_CLASS": "Name of the class used by the layered querying foreign data wrapper on the engine. Internal.",
    "SG_CMD_ASCII": "Set to `true` to disable Unicode output in sgr. Note that `sgr sql` will still output Unicode data.",
    "SG_UPDATE_REMOTE": "Name of the Splitgraph registry to check for sgr updates.",
    "SG_UPDATE_FREQUENCY": "How often to check for updates when sgr is run, in seconds. Set to 0 to disable.",
    "SG_UPDATE_LAST": "Last timestamp an update check was performed. Internal.",
    "SG_UPDATE_ANONYMOUS": "Set to `true` to disable sending the user's ID to the update checker.",
    "SG_PLUGIN_DIR": "Extra directory to look for plugins in. Each subdirectory must have a plugin.py file with a top-level __plugin__ variable pointing at the plugin class",
}
