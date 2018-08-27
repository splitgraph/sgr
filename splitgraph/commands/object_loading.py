import re
from collections import defaultdict

import requests
from psycopg2.sql import SQL, Identifier

from splitgraph.commands.misc import mount_postgres, make_conn, unmount
from splitgraph.constants import log, SplitGraphException, SPLITGRAPH_META_SCHEMA
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
    :return:
    """
    existing_objects = get_downloaded_objects(conn)
    objects_to_fetch = set(o for o in objects_to_fetch if o not in existing_objects)
    if not objects_to_fetch:
        return

    external_objects = _fetch_external_objects(conn, object_locations, objects_to_fetch)

    objects_to_fetch = [o for o in objects_to_fetch if o not in external_objects]
    if not objects_to_fetch:
        return

    _fetch_remote_objects(conn, objects_to_fetch, remote_conn_string, remote_conn)


def _fetch_remote_objects(conn, objects_to_fetch, remote_conn_string, remote_conn=None):
    # Instead of connecting and pushing queries to it from the Python client, we just mount the remote mountpoint
    # into a temporary space (without any checking out) and SELECT the required data into our local tables.
    match = re.match(r'(\S+):(\S+)@(.+):(\d+)/(\S+)', remote_conn_string)
    remote_data_mountpoint = 'tmp_remote_data'
    unmount(conn, remote_data_mountpoint)  # Maybe worth making sure we're not stepping on anyone else
    mount_postgres(conn, server=match.group(3), port=int(match.group(4)),
                   username=match.group(1), password=match.group(2), mountpoint=remote_data_mountpoint,
                   extra_options={'dbname': match.group(5), 'remote_schema': SPLITGRAPH_META_SCHEMA})
    remote_conn = remote_conn or make_conn(server=match.group(3), port=int(match.group(4)), username=match.group(1),
                                           password=match.group(2), dbname=match.group(5))
    try:
        for i, obj in enumerate(objects_to_fetch):
            log("(%d/%d) %s..." % (i + 1, len(objects_to_fetch), obj))
            # Foreign tables don't have PK constraints so we'll have to apply them manually.
            copy_table(conn, remote_data_mountpoint, obj, SPLITGRAPH_META_SCHEMA, obj, with_pk_constraints=False)
            with conn.cursor() as cur:
                source_pks = get_primary_keys(remote_conn, SPLITGRAPH_META_SCHEMA, obj)
                if source_pks:
                    cur.execute(SQL("ALTER TABLE {}.{} ADD PRIMARY KEY (").format(
                        Identifier(SPLITGRAPH_META_SCHEMA), Identifier(obj)) \
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
        print("Fetching external objects...")
        # Future: maybe have a series of hooks that let people register their own object handlers.
        for method, objects in non_remote_by_method.items():
            if method == 'HTTP':
                _http_download_objects(conn, objects, {})
            elif method == 'FILE':
                _file_download_objects(conn, objects, {})
            else:
                raise SplitGraphException("Unable to fetch some objects: unknown protocol %s")
    return non_remote_objects


def _table_dump_generator(conn, schema, table):
    # Don't include a schema (mountpoint) qualifier since the dump might be imported into a different place.
    yield (dump_table_creation(conn, schema, [table], created_schema=None) + SQL(';\n')).as_string(conn)

    # Use a server-side cursor here so we don't fetch the whole db into memory immediately.
    with conn.cursor(name='sg_table_upload_cursor') as cur:
        cur.itersize = 10000
        cur.execute(SQL("SELECT * FROM {}.{}""").format(Identifier(schema), Identifier(table)))
        row = next(cur)
        q = '(' + ','.join('%s' for _ in row) + ')'
        yield cur.mogrify(SQL("INSERT INTO {} VALUES " + q).format(Identifier(table)), row).decode('utf-8')
        for row in cur:
            yield cur.mogrify(',' + q, row).decode('utf-8')
        yield ';\n'
    return


def _http_upload_objects(conn, objects_to_push, http_params):
    url = http_params['url']

    uploaded = []
    for object_id in objects_to_push:
        remote_filename = object_id
        # First cut: just push the dump without any compression.
        r = requests.post(url + '/' + remote_filename,
                          data=_table_dump_generator(conn, SPLITGRAPH_META_SCHEMA, object_id))
        r.raise_for_status()
        uploaded.append(url + '/' + remote_filename)
    return uploaded


def _file_upload_objects(conn, objects_to_push, params):
    # Mostly for testing purposes: dumps the objects into a file.
    path = params['path']

    uploaded = []
    for object_id in objects_to_push:
        remote_filename = object_id
        # First cut: just push the dump without any compression.
        with open(path + '/' + remote_filename, 'w') as f:
            f.writelines(_table_dump_generator(conn, SPLITGRAPH_META_SCHEMA, object_id))
        uploaded.append(path + '/' + remote_filename)
    return uploaded


def _file_download_objects(conn, objects_to_fetch, params):
    for i, obj in enumerate(objects_to_fetch):
        object_id, object_url = obj
        log("(%d/%d) %s -> %s" % (i + 1, len(objects_to_fetch), object_url, object_id))
        with open(object_url, 'r') as f:
            with conn.cursor() as cur:
                # Insert into the locally checked out schema by default since the dump doesn't have the schema
                # qualification.
                cur.execute("SET search_path TO %s", (SPLITGRAPH_META_SCHEMA,))
                for chunk in f.readlines():
                    cur.execute(chunk)
                # Set the schema to (presumably) the default one.
                cur.execute("SET search_path TO public")


def _http_download_objects(conn, objects_to_fetch, http_params):
    username = http_params.get('username')
    password = http_params.get('password')

    for i, obj in enumerate(objects_to_fetch):
        object_id, object_url = obj
        log("(%d/%d) %s -> %s" % (i + 1, len(objects_to_fetch), object_url, object_id))
        # Let's execute arbitrary code from the Internet on our machine!
        r = requests.get(object_url, stream=True)
        with conn.cursor() as cur:
            # Insert into the splitgraph_meta schema by default since the dump doesn't have the schema
            # qualification.
            # NB can this break system tables in the splitgraph_meta_schema?
            cur.execute("SET search_path TO %s", (SPLITGRAPH_META_SCHEMA,))
            buf = ""
            for chunk in r.iter_content(chunk_size=4096):
                # This is dirty. What we want is to pipe the output of the fetch directly into the DB, but we don't
                # actually know when a SQL statement has terminated so we can ship it off: in particular, the
                # semicolon might have been escaped etc. Hence this horror which might/will break on
                # convoluted data.
                buf = buf + chunk
                last_sep = buf.rfind(';\n')
                cur.execute(buf[:last_sep])
                buf = buf[last_sep + 2:]
            cur.execute(buf)
        # Set the schema to (presumably) the default one.
        cur.execute("SET search_path TO public")


def upload_objects(conn, remote_conn_string, objects_to_push, handler='DB', handler_params={}, remote_conn=None):
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
    match = re.match(r'(\S+):(\S+)@(.+):(\d+)/(\S+)', remote_conn_string)
    existing_objects = get_existing_objects(remote_conn)
    objects_to_push = list(set(o for o in objects_to_push if o not in existing_objects))
    if not objects_to_push:
        log("Nothing to upload.")
        return []
    log("Uploading objects...")

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
                       extra_options={'dbname': match.group(5), 'remote_schema': SPLITGRAPH_META_SCHEMA})
        for i, obj in enumerate(objects_to_push):
            log("(%d/%d) %s..." % (i + 1, len(objects_to_push), obj))
            copy_table(conn, SPLITGRAPH_META_SCHEMA, obj, remote_data_mountpoint, obj, with_pk_constraints=False,
                       table_exists=True)
        conn.commit()
        unmount(conn, remote_data_mountpoint)

        # We assume that if the object doesn't have an explicit location, it lives on the remote.
        result = []
    elif handler == 'HTTP':
        uploaded = _http_upload_objects(conn, objects_to_push, handler_params)
        result = [(oid, url, 'HTTP') for oid, url in zip(objects_to_push, uploaded)]
    elif handler == 'FILE':
        uploaded = _file_upload_objects(conn, objects_to_push, handler_params)
        result = [(oid, url, 'FILE') for oid, url in zip(objects_to_push, uploaded)]

    return result
