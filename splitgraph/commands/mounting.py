import logging

from splitgraph.commands.misc import unmount
from splitgraph.commands.mount_handlers import get_mount_handler
from splitgraph.constants import to_repository
from splitgraph.meta_handler.common import ensure_metadata_schema


def mount(conn, mountpoint, mount_handler, handler_kwargs):
    """
    Mounts a foreign database via Postgres FDW (without creating new Splitgraph objects)

    :param conn: psycopg connection object
    :param mountpoint: Mountpoint to import the new tables into.
    :param mount_handler: The type of the mounted database. Must be one of `postgres_fdw` or `mongo_fdw`.
    :param handler_kwargs: Dictionary of options to pass to the mount handler.
    """
    ensure_metadata_schema(conn)
    mh_func = get_mount_handler(mount_handler)
    logging.info("Connecting to remote server...")

    unmount(conn, to_repository(mountpoint))
    mh_func(conn, mountpoint, **handler_kwargs)

    conn.commit()
