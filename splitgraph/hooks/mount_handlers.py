"""
Extra wrapper code for mount handlers
"""
from typing import TYPE_CHECKING, Any, Dict, Optional

from splitgraph.core.common import unmount_schema
from splitgraph.exceptions import DataSourceError

if TYPE_CHECKING:
    from splitgraph.core.types import TableInfo


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
    mountpoint: str,
    mount_handler: str,
    handler_kwargs: Dict[str, Any],
    overwrite: bool = True,
    tables: Optional["TableInfo"] = None,
) -> None:
    """
    Mounts a foreign database via an FDW (without creating new Splitgraph objects)

    :param mountpoint: Mountpoint to import the new tables into.
    :param mount_handler: The type of the mounted database.
    :param handler_kwargs: Dictionary of options to pass to the mount handler.
    :param overwrite: Delete the foreign server if it already exists. Used by mount_postgres for data pulls.
    :param tables: List of tables to mount or their schemas
    """
    # Workaround for circular imports
    from splitgraph.engine import get_engine
    from splitgraph.hooks.data_source import get_data_source
    from splitgraph.hooks.data_source.fdw import ForeignDataWrapperDataSource

    engine = get_engine()
    data_source = get_data_source(mount_handler)

    if not data_source.supports_mount:
        raise DataSourceError("Data source %s does not support mounting!")

    assert issubclass(data_source, ForeignDataWrapperDataSource)

    unmount_schema(engine, mountpoint)

    source = data_source.from_commandline(engine, handler_kwargs)
    source.mount(schema=mountpoint, overwrite=overwrite, tables=tables)
    engine.commit()
