"""
Commands for mounting other databases locally via Postgres FDW.
"""

import logging

from splitgraph._data.common import ensure_metadata_schema
from splitgraph.commands.misc import unmount
from splitgraph.commands.repository import to_repository
from splitgraph.connection import get_connection
from splitgraph.hooks.mount_handlers import get_mount_handler


def mount(mountpoint, mount_handler, handler_kwargs):
    """
    Mounts a foreign database via Postgres FDW (without creating new Splitgraph objects)

    :param mountpoint: Mountpoint to import the new tables into.
    :param mount_handler: The type of the mounted database. Must be one of `postgres_fdw` or `mongo_fdw`.
    :param handler_kwargs: Dictionary of options to pass to the mount handler.
    """
    ensure_metadata_schema()
    mh_func = get_mount_handler(mount_handler)
    logging.info("Connecting to remote server...")

    unmount(to_repository(mountpoint))
    mh_func(mountpoint, **handler_kwargs)

    get_connection().commit()
