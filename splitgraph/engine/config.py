from contextlib import contextmanager
from typing import TYPE_CHECKING, Dict, Iterator, Optional, Union, cast

from splitgraph.config import CONFIG
from splitgraph.config.config import get_all_in_section, get_singleton
from splitgraph.config.keys import ConfigDict

if TYPE_CHECKING:
    from .postgres.engine import PostgresEngine


# List of config flags that are extracted from the global configuration and passed to a given engine
_ENGINE_SPECIFIC_CONFIG = [
    "SG_ENGINE_HOST",
    "SG_ENGINE_PORT",
    "SG_ENGINE_USER",
    "SG_ENGINE_PWD",
    "SG_ENGINE_DB_NAME",
    "SG_ENGINE_POSTGRES_DB_NAME",
    "SG_ENGINE_ADMIN_USER",
    "SG_ENGINE_ADMIN_PWD",
    "SG_ENGINE_FDW_HOST",
    "SG_ENGINE_FDW_PORT",
    "SG_ENGINE_OBJECT_PATH",
    "SG_NAMESPACE",
    "SG_IS_REGISTRY",
    "SG_CHECK_VERSION",
]

# Some engine config keys default to values of other keys if unspecified.
_ENGINE_CONFIG_DEFAULTS = {
    "SG_ENGINE_FDW_HOST": "SG_ENGINE_HOST",
    "SG_ENGINE_FDW_PORT": "SG_ENGINE_PORT",
}


def _prepare_engine_config(config_dict: ConfigDict, name: str = "LOCAL") -> Dict[str, str]:
    result = {}

    # strictly speaking the "config_dict" itself doesn't have the type Dict[str, str]
    # (since it has nested things) but whatever
    subsection: Dict[str, str] = cast(
        Dict[str, str],
        config_dict if name == "LOCAL" else get_all_in_section(config_dict, "remotes")[name],
    )

    for key in _ENGINE_SPECIFIC_CONFIG:
        try:
            result[key] = subsection[key]
        except KeyError:
            try:
                result[key] = subsection[_ENGINE_CONFIG_DEFAULTS[key]]
            except KeyError:
                result[key] = ""
    return result


# Name of the current global engine, 'LOCAL' for the local.
# Can be overridden via normal configuration routes, e.g.
# $ SG_ENGINE=remote_engine sgr init
# will initialize the remote engine instead.
_ENGINE: Union[str, "PostgresEngine"] = get_singleton(CONFIG, "SG_ENGINE") or "LOCAL"

# Map of engine names -> Engine instances
_ENGINES: Dict[str, "PostgresEngine"] = {}


def get_engine(
    name: Optional[str] = None,
    use_socket: bool = False,
    use_fdw_params: bool = False,
    autocommit: bool = False,
) -> "PostgresEngine":
    """
    Get the current global engine or a named remote engine

    :param name: Name of the remote engine as specified in the config. If None, the current global engine
        is returned.
    :param use_socket: Use a local UNIX socket instead of PG_HOST, PG_PORT for LOCAL engine connections.
    :param use_fdw_params: Use the _FDW connection parameters (SG_ENGINE_FDW_HOST/PORT). By default,
        will infer from the global splitgraph.config.IN_FDW flag.
    :param autocommit: If True, the engine will not open SQL transactions implicitly.
    """
    from .postgres.engine import PostgresEngine

    if not name:
        if isinstance(_ENGINE, PostgresEngine):
            return _ENGINE
        name = _ENGINE
    if name not in _ENGINES:
        # Here we'd get the engine type/backend (Postgres/MySQL etc)
        # and instantiate the actual Engine class.
        # As we only have PostgresEngine, we instantiate that.

        conn_params = cast(Dict[str, Optional[str]], _prepare_engine_config(CONFIG, name))

        try:
            is_registry = bool(conn_params.pop("SG_IS_REGISTRY"))
        except KeyError:
            is_registry = False
        try:
            check_version = bool(conn_params.pop("SG_CHECK_VERSION"))
        except KeyError:
            check_version = False

        if name == "LOCAL" and use_socket:
            conn_params["SG_ENGINE_HOST"] = None
            conn_params["SG_ENGINE_PORT"] = None
        if use_fdw_params:
            conn_params["SG_ENGINE_HOST"] = conn_params["SG_ENGINE_FDW_HOST"]
            conn_params["SG_ENGINE_PORT"] = conn_params["SG_ENGINE_FDW_PORT"]

        _ENGINES[name] = PostgresEngine(
            conn_params=conn_params,
            name=name,
            autocommit=autocommit,
            registry=is_registry,
            check_version=check_version,
            in_fdw=use_fdw_params and name == "LOCAL",
        )
    return _ENGINES[name]


def set_engine(engine: "PostgresEngine") -> None:
    """
    Switch the global engine to a different one.

    :param engine: Engine
    """
    global _ENGINE
    _ENGINE = engine


@contextmanager
def switch_engine(engine: "PostgresEngine") -> Iterator[None]:
    """
    Switch the global engine to a different one. The engine will
    get switched back on exit from the context manager.

    :param engine: Engine
    """
    global _ENGINE
    _prev_engine = _ENGINE
    try:
        _ENGINE = engine
        yield
    finally:
        _ENGINE = _prev_engine
