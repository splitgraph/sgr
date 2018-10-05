import logging

from psycopg2.sql import Identifier, SQL

from splitgraph.commands.importing import import_tables
from splitgraph.commands.misc import unmount
from splitgraph.commands.mount_handlers import get_mount_handler
from splitgraph.meta_handler.common import ensure_metadata_schema
from splitgraph.meta_handler.misc import get_all_foreign_tables


def mount(conn, mountpoint, mount_handler, handler_kwargs):
    """
    Mounts a foreign database via Postgres FDW and copies all of its tables over, registering them as new SplitGraph
    objects.

    :param conn: psycopg connection object
    :param mountpoint: Mountpoint to import the new tables into.
    :param mount_handler: The type of the mounted database. Must be one of `postgres_fdw` or `mongo_fdw`.
    :param handler_kwargs: Dictionary of options to pass to the mount handler.
    :return: Image hash that the new tables were committed under.
    """
    ensure_metadata_schema(conn)
    mh_func = get_mount_handler(mount_handler)
    logging.info("Connecting to remote server...")

    staging_mountpoint = mountpoint + '_tmp_staging'
    unmount(conn, staging_mountpoint)
    try:
        mh_func(conn, staging_mountpoint, **handler_kwargs)

        # Mimic the behaviour of the old mount here: create the schema and have a random initial commit in it
        # wit the imported (snapshotted) foreign tables.
        with conn.cursor() as cur:
            cur.execute(SQL("CREATE SCHEMA {}").format(Identifier(mountpoint)))
        # Import the foreign tables (creates a new commit with a random ID and copies the tables over into
        # the final mountpoint).
        new_head = import_tables(conn, staging_mountpoint, get_all_foreign_tables(conn, staging_mountpoint), mountpoint,
                                 [],
                                 foreign_tables=True, do_checkout=True)
    finally:
        unmount(conn, staging_mountpoint)

    conn.commit()
    return new_head
