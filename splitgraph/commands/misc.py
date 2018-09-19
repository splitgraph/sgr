import psycopg2
from psycopg2.sql import SQL, Identifier

from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler.common import META_TABLES, ensure_metadata_schema
from splitgraph.meta_handler.images import get_image_parent
from splitgraph.meta_handler.misc import register_mountpoint, unregister_mountpoint
from splitgraph.meta_handler.objects import get_object_meta
from splitgraph.meta_handler.tables import get_table
from splitgraph.pg_audit import manage_audit, discard_pending_changes
from splitgraph.pg_utils import pg_table_exists


def table_exists_at(conn, mountpoint, table_name, image_hash):
    """Determines whether a given table exists in a SplitGraph image without checking it out. If `image_hash` is None,
    determines whether the table exists in the current staging area."""
    return pg_table_exists(conn, mountpoint, table_name) if image_hash is None \
        else bool(get_table(conn, mountpoint, table_name, image_hash))


def make_conn(server, port, username, password, dbname):
    return psycopg2.connect(host=server, port=port, user=username, password=password, dbname=dbname)


def get_parent_children(conn, mountpoint, image_hash):
    """Gets the parent and a list of children of a given image."""
    parent = get_image_parent(conn, mountpoint, image_hash)

    with conn.cursor() as cur:
        cur.execute(SQL("""SELECT snap_id FROM {}.snap_tree WHERE mountpoint = %s AND parent_id = %s""").format(
            Identifier(SPLITGRAPH_META_SCHEMA)),
            (mountpoint, image_hash))
        children = [c[0] for c in cur.fetchall()]
    return parent, children


def get_log(conn, mountpoint, start_snap):
    """Repeatedly gets the parent of a given snapshot until it reaches the bottom."""
    result = []
    while start_snap is not None:
        result.append(start_snap)
        start_snap = get_image_parent(conn, mountpoint, start_snap)
    return result


def find_path(conn, mountpoint, hash_1, hash_2):
    """If the two images are on the same path in the commit tree, returns that path."""
    path = []
    while hash_2 is not None:
        path.append(hash_2)
        hash_2 = get_image_parent(conn, mountpoint, hash_2)
        if hash_2 == hash_1:
            return path


@manage_audit
def init(conn, mountpoint):
    """
    Initializes an empty repo with an initial commit (hash 0000...)
    :param conn: psycopg connection object.
    :param mountpoint: Mountpoint to create the repository in. Must not exist.
    """
    with conn.cursor() as cur:
        cur.execute(SQL("CREATE SCHEMA {}").format(Identifier(mountpoint)))
    snap_id = '0' * 64
    register_mountpoint(conn, mountpoint, snap_id, tables=[], table_object_ids=[])


def unmount(conn, mountpoint):
    """
    Discards all changes to a given mountpoint and all of its history, deleting the physical Postgres schema.
    Doesn't delete any cached physical objects.
    :param conn: psycopg connection object
    :param mountpoint: Mountpoint to unmount.
    :return:
    """
    # Make sure to discard changes to this mountpoint if they exist, otherwise they might
    # be applied/recorded if a new mountpoint with the same name appears.
    ensure_metadata_schema(conn)
    discard_pending_changes(conn, mountpoint)

    with conn.cursor() as cur:
        cur.execute(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(mountpoint)))
        # Drop server too if it exists (could have been a non-foreign mountpoint)
        cur.execute(SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(mountpoint + '_server')))

    # Currently we just discard all history info about the mounted schema
    unregister_mountpoint(conn, mountpoint)
    conn.commit()


def cleanup_objects(conn, include_external=False):
    """
    Deletes all local objects not required by any current mountpoint, including their dependencies, their remote
    locations and their cached local copies.
    :param conn: psycopg connection object.
    :param include_external: If True, deletes all external objects cached locally and redownloads them when they're
        needed.
    """
    # First, get a list of all objects required by a table.
    with conn.cursor() as cur:
        cur.execute(SQL("SELECT DISTINCT (object_id) FROM {}.tables").format(Identifier(SPLITGRAPH_META_SCHEMA)))
        primary_objects = {c[0] for c in cur.fetchall()}

    # Expand that since each object might have a parent it depends on.
    if primary_objects:
        while True:
            new_parents = set(parent_id for _, _, parent_id in get_object_meta(conn, list(primary_objects))
                              if parent_id not in primary_objects and parent_id is not None)
            if not new_parents:
                break
            else:
                primary_objects.update(new_parents)

    # Go through the tables that aren't mountpoint-dependent and delete entries there.
    with conn.cursor() as cur:
        for table_name in ['object_tree', 'object_locations']:
            query = SQL("DELETE FROM {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table_name))
            if primary_objects:
                query += SQL(" WHERE object_id NOT IN (" + ','.join('%s' for _ in range(len(primary_objects))) + ")")
            cur.execute(query, list(primary_objects))

    # Go through the physical objects and delete them as well
    with conn.cursor() as cur:
        cur.execute("""SELECT information_schema.tables.table_name FROM information_schema.tables
                        WHERE information_schema.tables.table_schema = %s""", (SPLITGRAPH_META_SCHEMA,))
        # This is slightly dirty, but since the info about the objects was deleted on unmount, we just say that
        # anything in splitgraph_meta that's not a system table is fair game.
        tables_in_meta = set(c[0] for c in cur.fetchall() if c[0] not in META_TABLES)

        to_delete = tables_in_meta.difference(primary_objects)

        # All objects in `object_locations` are assumed to exist externally (so we can redownload them if need be).
        # This can be improved on by, on materialization, downloading all SNAPs directly into the target schema and
        # applying the DIFFs to it (instead of downloading them into a staging area), but that requires us to change
        # the object downloader interface.
        if include_external:
            cur.execute(SQL("SELECT object_id FROM {}.object_locations").format(Identifier(SPLITGRAPH_META_SCHEMA)))
            to_delete += set(c[0] for c in cur.fetchall())

    delete_objects(conn, to_delete)
    return to_delete


def delete_objects(conn, objects):
    """
    Deletes objects from the Splitgraph cache
    :param conn: Psycopg connection object
    :param objects: A sequence of objects to be deleted
    """
    if objects:
        with conn.cursor() as cur:
            cur.execute(SQL(";").join(SQL("DROP TABLE {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                     Identifier(d)) for d in objects))
