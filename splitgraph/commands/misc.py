"""
Miscellaneous commands for Splitgraph repository management
"""
import logging
from pkgutil import get_data

import psycopg2
from psycopg2.sql import SQL, Identifier

from splitgraph._data.common import META_TABLES, ensure_metadata_schema
from splitgraph._data.objects import get_object_meta
from splitgraph.commands.info import get_image, get_table
from splitgraph.commands.repository import register_repository, unregister_repository
from splitgraph.config import SPLITGRAPH_META_SCHEMA, CONFIG, PG_HOST, PG_PORT, PG_DB
from splitgraph.connection import get_connection
from splitgraph.engine import get_engine
from splitgraph.engine.postgres._pg_audit import manage_audit, discard_pending_changes

_AUDIT_SCHEMA = 'audit'
_AUDIT_TRIGGER = 'resources/audit_trigger.sql'
_PACKAGE = 'splitgraph'


def table_exists_at(repository, table_name, image_hash):
    """Determines whether a given table exists in a Splitgraph image without checking it out. If `image_hash` is None,
    determines whether the table exists in the current staging area."""
    return get_engine().table_exists(repository.to_schema(), table_name) if image_hash is None \
        else bool(get_table(repository, table_name, image_hash))


def get_log(repository, start_image):
    """Repeatedly gets the parent of a given image until it reaches the bottom."""
    result = []
    while start_image is not None:
        result.append(start_image)
        start_image = get_image(repository, start_image).parent_id
    return result


def find_path(repository, hash_1, hash_2):
    """If the two images are on the same path in the commit tree, returns that path."""
    path = []
    while hash_2 is not None:
        path.append(hash_2)
        hash_2 = get_image(repository, hash_2).parent_id
        if hash_2 == hash_1:
            return path


def cleanup_objects(include_external=False):
    """
    Deletes all local objects not required by any current mountpoint, including their dependencies, their remote
    locations and their cached local copies.

    :param include_external: If True, deletes all external objects cached locally and redownloads them when they're
        needed.
    """
    # First, get a list of all objects required by a table.
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(SQL("SELECT DISTINCT (object_id) FROM {}.tables").format(Identifier(SPLITGRAPH_META_SCHEMA)))
        primary_objects = {c[0] for c in cur.fetchall()}

    # Expand that since each object might have a parent it depends on.
    if primary_objects:
        while True:
            new_parents = set(parent_id for _, _, parent_id, _ in get_object_meta(list(primary_objects))
                              if parent_id not in primary_objects and parent_id is not None)
            if not new_parents:
                break
            else:
                primary_objects.update(new_parents)

    # Go through the tables that aren't mountpoint-dependent and delete entries there.
    with conn.cursor() as cur:
        for table_name in ['objects', 'object_locations']:
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
            to_delete = to_delete.union(set(c[0] for c in cur.fetchall()))

    delete_objects(to_delete)
    return to_delete


def delete_objects(objects):
    """
    Deletes objects from the Splitgraph cache

    :param objects: A sequence of objects to be deleted
    """
    if objects:
        with get_connection().cursor() as cur:
            cur.execute(SQL(";").join(SQL("DROP TABLE IF EXISTS {}.{}").format(Identifier(SPLITGRAPH_META_SCHEMA),
                                                                               Identifier(d)) for d in objects))


@manage_audit
def init(repository):
    """
    Initializes an empty repo with an initial commit (hash 0000...)

    :param repository: Repository to create. Must not exist locally.
    """
    with get_connection().cursor() as cur:
        cur.execute(SQL("CREATE SCHEMA {}").format(Identifier(repository.to_schema())))
    image_hash = '0' * 64
    register_repository(repository, image_hash, tables=[], table_object_ids=[])


def rm(repository, unregister=True):
    """
    Discards all changes to a given repository and optionally all of its history,
    as well as deleting the Postgres schema that it might be checked out into.
    Doesn't delete any cached physical objects.

    :param repository: Repository/schema to delete.
    :param unregister: If False, just deletes the schema without purging any other repository metadata
    """
    # Make sure to discard changes to this repository if they exist, otherwise they might
    # be applied/recorded if a new repository with the same name appears.
    ensure_metadata_schema()
    discard_pending_changes(repository.to_schema())
    conn = get_connection()

    with conn.cursor() as cur:
        cur.execute(SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(Identifier(repository.to_schema())))
        # Drop server too if it exists (could have been a non-foreign repository)
        cur.execute(SQL("DROP SERVER IF EXISTS {} CASCADE").format(Identifier(repository.to_schema() + '_server')))

    if unregister:
        unregister_repository(repository)
    conn.commit()


# Method exercised in test_commandline.test_init_new_db but in
# an external process
def init_driver():  # pragma: no cover
    """
    Initializes the driver by:

        * creating the database specified in PG_DB
        * installing the audit trigger for version controlling checked out tables
        * creating the metadata tables

    If any of these things already exist, that step is skipped.
    """
    # Use the connection to the "postgres" database to create the actual PG_DB
    with psycopg2.connect(dbname=CONFIG['SG_DRIVER_POSTGRES_DB_NAME'],
                          user=CONFIG['SG_DRIVER_ADMIN_USER'],
                          password=CONFIG['SG_DRIVER_ADMIN_PWD'],
                          host=PG_HOST,
                          port=PG_PORT) as admin_conn:
        # CREATE DATABASE can't run inside of tx
        admin_conn.autocommit = True
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (PG_DB,))
            if cur.fetchone() is None:
                logging.info("Creating database %s", PG_DB)
                cur.execute(SQL("CREATE DATABASE {}").format(Identifier(PG_DB)))
            else:
                logging.info("Database %s already exists, skipping", PG_DB)

    # Install the audit trigger if it doesn't exist
    conn = get_connection()
    if not get_engine().schema_exists(_AUDIT_SCHEMA):
        logging.info("Installing the audit trigger...")
        audit_trigger = get_data(_PACKAGE, _AUDIT_TRIGGER)
        with conn.cursor() as cur:
            cur.execute(audit_trigger.decode('utf-8'))
    else:
        logging.info("Skipping the audit trigger as it's already installed")

    # Create splitgraph_meta
    logging.info("Ensuring metadata schema %s exists...", SPLITGRAPH_META_SCHEMA)
    ensure_metadata_schema()
