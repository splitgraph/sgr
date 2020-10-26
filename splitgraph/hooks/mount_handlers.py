"""
Hooks for additional handlers used to mount other databases via FDW. These handlers become available
in the command line tool (via `sgr mount`) and in the Splitfile interpreter (via `FROM MOUNT`).
"""
import logging
import types
from importlib import import_module
from typing import Dict, List, TYPE_CHECKING, Any, Type

from splitgraph.config import CONFIG
from splitgraph.config.config import get_all_in_section, get_singleton
from splitgraph.config.keys import DEFAULTS
from splitgraph.exceptions import MountHandlerError
from splitgraph.hooks.data_source import ForeignDataWrapperDataSource

if TYPE_CHECKING:
    pass

_MOUNT_HANDLERS: Dict[str, Type[ForeignDataWrapperDataSource]] = {}


def get_mount_handler(mount_handler: str) -> Type[ForeignDataWrapperDataSource]:
    """Returns a mount class for a given handler."""
    try:
        return _MOUNT_HANDLERS[mount_handler]
    except KeyError:
        raise MountHandlerError("Mount handler %s not supported!" % mount_handler)


def get_mount_handlers() -> List[str]:
    """Returns the names of all registered mount handlers."""
    return list(_MOUNT_HANDLERS.keys())


def register_mount_handler(name: str, mount_class: Type[ForeignDataWrapperDataSource]) -> None:
    """Returns a data source under a given name. See `get_mount_handler` for the mount handler spec."""
    global _MOUNT_HANDLERS
    _MOUNT_HANDLERS[name] = mount_class


def mount_postgres(mountpoint, **kwargs) -> None:
    """
    Mount a Postgres database.

    Mounts a schema on a remote Postgres database as a set of foreign tables locally.
    \b

    :param mountpoint: Schema to mount the remote into.
    :param server: Database hostname.
    :param port: Port the Postgres server is running on.
    :param username: A read-only user that the database will be accessed as.
    :param password: Password for the read-only user.
    :param dbname: Remote database name.
    :param remote_schema: Remote schema name.
    :param extra_server_args: Dictionary of extra arguments to pass to the foreign server
    :param tables: Tables to mount (default all). If a list, then will use IMPORT FOREIGN SCHEMA.
    If a dictionary, must have the format {"table_name": {"col_1": "type_1", ...}}.
    """
    mount(mountpoint, mount_handler="postgres_fdw", handler_kwargs=kwargs, overwrite=True)


def mount(
    mountpoint: str, mount_handler: str, handler_kwargs: Dict[str, Any], overwrite: bool = True
) -> None:
    """
    Mounts a foreign database via an FDW (without creating new Splitgraph objects)

    :param mountpoint: Mountpoint to import the new tables into.
    :param mount_handler: The type of the mounted database.
    :param handler_kwargs: Dictionary of options to pass to the mount handler.
    :param overwrite: Delete the foreign server if it already exists. Used by mount_postgres for data pulls.
    """
    from splitgraph.engine import get_engine
    from psycopg2.sql import Identifier, SQL

    engine = get_engine()
    data_source = get_mount_handler(mount_handler)

    engine.run_sql(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(mountpoint)))
    engine.run_sql(
        SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(mountpoint + "_server"))
    )

    source = data_source.from_commandline(engine, handler_kwargs)
    source.mount(schema=mountpoint, overwrite=overwrite)
    engine.commit()


def _register_default_handlers() -> None:
    # Register the mount handlers from the config.
    for handler_name, handler_class_name in get_all_in_section(CONFIG, "mount_handlers").items():
        assert isinstance(handler_class_name, str)

        try:
            handler_class = _load_handler(handler_name, handler_class_name)

            assert issubclass(handler_class, ForeignDataWrapperDataSource)
            register_mount_handler(handler_name.lower(), handler_class)
        except (ImportError, AttributeError) as e:
            raise MountHandlerError(
                "Error loading custom mount handler {0}".format(handler_name)
            ) from e


def _load_handler(handler_name, handler_class_name):
    # Hack for old-style mount handlers that have now been moved -- don't crash and instead
    # replace them in the config on the fly
    handler_defaults = get_all_in_section(DEFAULTS, "mount_handlers")

    fallback_used = False

    try:
        ix = handler_class_name.rindex(".")
        handler_class = getattr(
            import_module(handler_class_name[:ix]), handler_class_name[ix + 1 :]
        )
    except (ImportError, AttributeError):
        if handler_name not in handler_defaults:
            raise
        handler_class_name = handler_defaults[handler_name]
        ix = handler_class_name.rindex(".")
        handler_class = getattr(
            import_module(handler_class_name[:ix]), handler_class_name[ix + 1 :]
        )
        fallback_used = True

    if isinstance(handler_class, types.FunctionType):
        if handler_name not in handler_defaults:
            raise MountHandlerError(
                "Handler %s uses the old-style function interface which is not"
                " compatible with this version of Splitgraph. "
                "Delete it from your .sgconfig's [mount_handlers] section (%s)"
                % (handler_name, get_singleton(CONFIG, "SG_CONFIG_FILE"))
            )
        handler_class_name = handler_defaults[handler_name]
        ix = handler_class_name.rindex(".")
        handler_class = getattr(
            import_module(handler_class_name[:ix]), handler_class_name[ix + 1 :]
        )
        fallback_used = True
    if fallback_used:
        logging.warning(
            "Handler %s uses the old-style function interface and was automatically replaced.",
            handler_name,
        )
        logging.warning(
            "Replace it with %s=%s in your .sgconfig's [mount_handlers] section (%s)",
            handler_name,
            handler_class_name,
            get_singleton(CONFIG, "SG_CONFIG_FILE"),
        )
    return handler_class


_register_default_handlers()
