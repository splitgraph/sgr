from psycopg2.sql import SQL, Identifier
from random import getrandbits

from splitgraph.commands.checkout import checkout
from splitgraph.commands.misc import mount_postgres, mount_mongo
from splitgraph.pg_utils import copy_table
from splitgraph.constants import _log, SplitGraphException, get_random_object_id
from splitgraph.meta_handler import ensure_metadata_schema, get_all_foreign_tables, register_mountpoint


def mount(conn, server, port, username, password, mountpoint, mount_handler, extra_options):
    ensure_metadata_schema(conn)
    if mount_handler == 'postgres_fdw':
        mh_func = mount_postgres
    elif mount_handler == 'mongo_fdw':
        mh_func = mount_mongo
    else:
        raise SplitGraphException("Mount handler %s not supported!" % mount_handler)

    _log("Connecting to remote server...")
    mh_func(conn, server, port, username, password, mountpoint, extra_options)

    with conn.cursor() as cur:
        # For now we are just assigning a random ID to this schema snap
        schema_snap = "%0.2x" % getrandbits(256)

        _log("Pulling image %s from remote schema..." % schema_snap[:12])

        # Update tables to list all tables we've actually pulled.
        tables = get_all_foreign_tables(conn, mountpoint)

        # Rename all foreign tables into tablename_origin and pretend to "pull" the current remote schema HEAD.
        table_object_ids = []
        for table in tables:
            # Create a dummy object ID for each table.
            object_id = get_random_object_id()
            table_object_ids.append(object_id)
            cur.execute(SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                Identifier(mountpoint), Identifier(table),
                Identifier(table + '_origin')))
            copy_table(conn, mountpoint, table + '_origin', mountpoint, object_id)

    # Finally, register the mountpoint in our metadata store.
    register_mountpoint(conn, mountpoint, schema_snap, tables, table_object_ids)

    # Also check out the HEAD of the schema
    checkout(conn, mountpoint, schema_snap)

    conn.commit()
    return schema_snap
