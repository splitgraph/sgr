from psycopg2.sql import Identifier, SQL

from splitgraph.commands.importing import import_tables
from splitgraph.commands.misc import mount_postgres, mount_mongo, unmount
from splitgraph.constants import _log, SplitGraphException
from splitgraph.meta_handler import ensure_metadata_schema, get_all_foreign_tables


def mount(conn, server, port, username, password, mountpoint, mount_handler, extra_options):
    ensure_metadata_schema(conn)
    if mount_handler == 'postgres_fdw':
        mh_func = mount_postgres
    elif mount_handler == 'mongo_fdw':
        mh_func = mount_mongo
    else:
        raise SplitGraphException("Mount handler %s not supported!" % mount_handler)

    _log("Connecting to remote server...")

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
