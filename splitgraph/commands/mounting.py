from psycopg2.sql import Identifier, SQL

from splitgraph.commands.importing import import_tables
from splitgraph.commands.misc import unmount
from splitgraph.commands.mount_handlers import get_mount_handler
from splitgraph.constants import log
from splitgraph.meta_handler import ensure_metadata_schema, get_all_foreign_tables


def mount(conn, server, port, username, password, mountpoint, mount_handler, extra_options):
    """
    Mounts a foreign database via Postgres FDW and copies all of its tables over, registering them as new SplitGraph
    objects.
    :param conn: psycopg connection object
    :param server: Hostname of the source database.
    :param port: Port of the source database.
    :param username: Username to use to connect to the source database.
    :param password: Password to use to connect to the source database.
    :param mountpoint: Mountpoint to import the new tables into.
    :param mount_handler: The type of the mounted database. Must be one of `postgres_fdw` or `mongo_fdw`.
    :param extra_options: Dictionary of options to pass to the mount handler specifying the structure of the new tables.
    :return: Image hash that the new tables were committed under.
    """
    ensure_metadata_schema(conn)
    mh_func = get_mount_handler(mount_handler)
    log("Connecting to remote server...")

    staging_mountpoint = mountpoint + '_tmp_staging'
    unmount(conn, staging_mountpoint)
    try:
        mh_func(conn, server, port, username, password, staging_mountpoint, extra_options)

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
