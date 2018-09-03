import logging
import re
from collections import defaultdict

from psycopg2.sql import SQL, Identifier

from splitgraph.commands.external_object_handlers import get_upload_download_handler
from splitgraph.commands.misc import make_conn, unmount
from splitgraph.commands.mount_handlers import mount_postgres
from splitgraph.constants import SPLITGRAPH_META_SCHEMA
from splitgraph.meta_handler import get_downloaded_objects, get_existing_objects
from splitgraph.pg_utils import copy_table, dump_table_creation, get_primary_keys


def download_objects(conn, remote_conn_string, objects_to_fetch, object_locations, remote_conn=None):
    """
    Fetches the required objects from the remote and stores them locally. Does nothing for objects that already exist.
    :param conn: psycopg connection object
    :param remote_conn_string: Connection string to the remote SG driver of the form
        username:password@hostname:port/database.
    :param objects_to_fetch: List of object IDs to download.
    :param object_locations: List of custom object locations, encoded as tuples (object_id, object_url, protocol).
    :param remote_conn: If not None, must be a psycopg connection object used by the client to connect to the remote
        driver. The local driver will still use the parameters specified in `remote_conn_string` to download the
        actual objects from the remote.
    :return: Set of object IDs that were fetched.
    """
    existing_objects = get_downloaded_objects(conn)
    objects_to_fetch = set(o for o in objects_to_fetch if o not in existing_objects)
    if not objects_to_fetch:
        return objects_to_fetch

    external_objects = _fetch_external_objects(conn, object_locations, objects_to_fetch)

    remaining_objects_to_fetch = [o for o in objects_to_fetch if o not in external_objects]
    if not remaining_objects_to_fetch:
        return objects_to_fetch

    _fetch_remote_objects(conn, remaining_objects_to_fetch, remote_conn_string, remote_conn)
    return objects_to_fetch


def _fetch_remote_objects(conn, objects_to_fetch, remote_conn_string, remote_conn=None):
    # Instead of connecting and pushing queries to it from the Python client, we just mount the remote mountpoint
    # into a temporary space (without any checking out) and SELECT the required data into our local tables.
    match = re.match(r'(\S+):(\S+)@(.+):(\d+)/(\S+)', remote_conn_string)
    remote_data_mountpoint = 'tmp_remote_data'
    unmount(conn, remote_data_mountpoint)  # Maybe worth making sure we're not stepping on anyone else
    mount_postgres(conn, server=match.group(3), port=int(match.group(4)),
                   username=match.group(1), password=match.group(2), mountpoint=remote_data_mountpoint,
                   dbname=match.group(5), remote_schema=SPLITGRAPH_META_SCHEMA)
    remote_conn = remote_conn or make_conn(server=match.group(3), port=int(match.group(4)), username=match.group(1),
                                           password=match.group(2), dbname=match.group(5))
    try:
        for i, obj in enumerate(objects_to_fetch):
            print("(%d/%d) %s..." % (i + 1, len(objects_to_fetch), obj))
            # Foreign tables don't have PK constraints so we'll have to apply them manually.
            copy_table(conn, remote_data_mountpoint, obj, SPLITGRAPH_META_SCHEMA, obj, with_pk_constraints=False)
            with conn.cursor() as cur:
                source_pks = get_primary_keys(remote_conn, SPLITGRAPH_META_SCHEMA, obj)
                if source_pks:
                    cur.execute(SQL("ALTER TABLE {}.{} ADD PRIMARY KEY (").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier(obj))
                                + SQL(',').join(SQL("{}").format(Identifier(c)) for c, _ in source_pks) + SQL(")"))
    finally:
        unmount(conn, remote_data_mountpoint)


def _fetch_external_objects(conn, object_locations, objects_to_fetch):
    non_remote_objects = []
    non_remote_by_method = defaultdict(list)
    for object_id, object_url, protocol in object_locations:
        if object_id in objects_to_fetch:
            non_remote_by_method[protocol].append((object_id, object_url))
            non_remote_objects.append(object_id)
    if non_remote_objects:
        logging.info("Fetching external objects...")
        for method, objects in non_remote_by_method.items():
            _, handler = get_upload_download_handler(method)
            handler(conn, objects, {})
    return non_remote_objects


def upload_objects(conn, remote_conn_string, objects_to_push, handler='DB', handler_params=None, remote_conn=None):
    """
    Uploads physical objects to the remote or some other external location.
    :param conn: psycopg connection object
    :param remote_conn_string: Connection string to the remote SG driver of the form
        username:password@hostname:port/database.
    :param objects_to_push: List of object IDs to upload.
    :param handler: Name of the handler to use to upload objects. Use `DB` to push them to the remote, `FILE`
        to store them in a directory that can be accessed from the client and `HTTP` to upload them to HTTP.
    :param handler_params: For `HTTP`, a dictionary `{"username": username, "password", password}`. For `FILE`,
        a dictionary `{"path": path}` specifying the directory where the objects shall be saved.
    :param remote_conn: If not None, must be a psycopg connection object used by the client to connect to the remote
        driver. The local driver will still use the parameters specified in `remote_conn_string` to download the
        actual objects from the remote.
    :return: A list of (object_id, url, handler) that specifies all objects were uploaded (skipping objects that
        already exist on the remote).
    """
    if handler_params is None:
        handler_params = {}
    match = re.match(r'(\S+):(\S+)@(.+):(\d+)/(\S+)', remote_conn_string)
    existing_objects = get_existing_objects(remote_conn)
    objects_to_push = list(set(o for o in objects_to_push if o not in existing_objects))
    if not objects_to_push:
        logging.info("Nothing to upload.")
        return []
    logging.info("Uploading %d object(s)...", len(objects_to_push))

    if handler == 'DB':
        # Difference from pull here: since we can't get remote to mount us, we instead use normal SQL statements
        # to create new tables remotely, then mount them and write into them from our side.
        # Is there seriously no better way to do this?
        with remote_conn.cursor() as cur:
            # This also includes applying our table's FK constraints.
            cur.execute(dump_table_creation(conn, schema=SPLITGRAPH_META_SCHEMA, tables=objects_to_push,
                                            created_schema=SPLITGRAPH_META_SCHEMA))
        # Have to commit the remote connection here since otherwise we won't see the new tables in the
        # mounted remote.
        remote_conn.commit()
        remote_data_mountpoint = 'tmp_remote_data'
        unmount(conn, remote_data_mountpoint)
        mount_postgres(conn, server=match.group(3), port=int(match.group(4)),
                       username=match.group(1), password=match.group(2), mountpoint=remote_data_mountpoint,
                       dbname=match.group(5), remote_schema=SPLITGRAPH_META_SCHEMA)
        for i, obj in enumerate(objects_to_push):
            print("(%d/%d) %s..." % (i + 1, len(objects_to_push), obj))
            copy_table(conn, SPLITGRAPH_META_SCHEMA, obj, remote_data_mountpoint, obj, with_pk_constraints=False,
                       table_exists=True)
        conn.commit()
        unmount(conn, remote_data_mountpoint)

        # We assume that if the object doesn't have an explicit location, it lives on the remote.
        return []

    upload_handler, _ = get_upload_download_handler(handler)
    uploaded = upload_handler(conn, objects_to_push, handler_params)
    return [(oid, url, handler) for oid, url in zip(objects_to_push, uploaded)]
