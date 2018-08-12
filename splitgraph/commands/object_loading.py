import re
from collections import defaultdict

import requests
from psycopg2.sql import SQL, Identifier

from splitgraph.commands.misc import mount_postgres, make_conn, dump_table_creation, unmount, copy_table
from splitgraph.constants import _log, SplitGraphException
from splitgraph.meta_handler import get_downloaded_objects, get_existing_objects
from splitgraph.pg_replication import _get_primary_keys


def download_objects(conn, local_mountpoint, remote_conn_string, remote_mountpoint, objects_to_fetch, object_locations):
    # Fetches the required objects from the remote and stores them locally. Does nothing for objects that already exist.
    existing_objects = get_downloaded_objects(conn, local_mountpoint)
    objects_to_fetch = set(o for o in objects_to_fetch if o not in existing_objects)
    if not objects_to_fetch:
        return

    non_remote_objects = []
    non_remote_by_method = defaultdict(list)
    for object_id, object_url, protocol in object_locations:
        if object_id in objects_to_fetch:
            non_remote_by_method[protocol].append((object_id, object_url))
            non_remote_objects.append(object_id)

    if non_remote_objects:
        print("Fetching external objects...")
        for method, objects in non_remote_by_method.items():
            if method == 'HTTP':
                _http_download_objects(conn, local_mountpoint, objects, {})
            elif method == 'FILE':
                _file_download_objects(conn, local_mountpoint, objects, {})
            else:
                raise SplitGraphException("Unable to fetch some objects: unknown protocol %s")

    objects_to_fetch = [o for o in objects_to_fetch if o not in non_remote_objects]
    if not objects_to_fetch:
        return

    # Instead of connecting and pushing queries to it from the Python client, we just mount the remote mountpoint
    # into a temporary space (without any checking out) and SELECT the required data into our local tables.
    match = re.match('(\S+):(\S+)@(.+):(\d+)/(\S+)', remote_conn_string)
    remote_data_mountpoint = 'tmp_remote_data'
    unmount(conn, remote_data_mountpoint)  # Maybe worth making sure we're not stepping on anyone else
    mount_postgres(conn, server=match.group(3), port=int(match.group(4)),
                   username=match.group(1), password=match.group(2), mountpoint=remote_data_mountpoint,
                   extra_options={'dbname': match.group(5), 'remote_schema': remote_mountpoint})

    remote_conn = make_conn(server=match.group(3), port=int(match.group(4)), username=match.group(1),
                            password=match.group(2), dbname=match.group(5))

    try:
        for i, obj in enumerate(objects_to_fetch):
            _log("(%d/%d) %s..." % (i + 1, len(objects_to_fetch), obj))
            # Foreign tables don't have PK constraints so we'll have to apply them manually.
            copy_table(conn, remote_data_mountpoint, obj, local_mountpoint, obj, with_pk_constraints=False)
            with conn.cursor() as cur:
                source_pks = _get_primary_keys(remote_conn, remote_mountpoint, obj)
                if source_pks:
                    cur.execute(SQL("ALTER TABLE {}.{} ADD PRIMARY KEY (").format(
                        Identifier(local_mountpoint), Identifier(obj))\
                            + SQL(',').join(SQL("{}").format(Identifier(c)) for c, _ in source_pks) + SQL(")"))
    finally:
        remote_conn.close()
        unmount(conn, remote_data_mountpoint)


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
            yield cur.mogrify(', ' + q + '\n', row).decode('utf-8')


def _http_upload_objects(conn, local_mountpoint, objects_to_push, http_params):
    url = http_params['url']
    username = http_params.get('username')
    password = http_params.get('password')

    uploaded = []
    for object_id in objects_to_push:
        remote_filename = object_id
        # First cut: just push the dump without any compression.
        r = requests.post(url + '/' + remote_filename, data=_table_dump_generator(conn, local_mountpoint, object_id))
        r.raise_for_status()
        uploaded.append(url + '/' + remote_filename)
    return uploaded


def _file_upload_objects(conn, local_mountpoint, objects_to_push, params):
    # Mostly for testing purposes: dumps the objects into a file.
    path = params['path']

    uploaded = []
    for object_id in objects_to_push:
        remote_filename = object_id
        # First cut: just push the dump without any compression.
        with open(path + '/' + remote_filename, 'w') as f:
            f.writelines(_table_dump_generator(conn, local_mountpoint, object_id))
        uploaded.append(path + '/' + remote_filename)
    return uploaded


def _file_download_objects(conn, local_mountpoint, objects_to_fetch, params):
    for i, obj in enumerate(objects_to_fetch):
        object_id, object_url = obj
        _log("(%d/%d) %s -> %s" % (i+1, len(objects_to_fetch), object_url, object_id))
        with open(object_url, 'r') as f:
            with conn.cursor() as cur:
                # Insert into the locally checked out schema by default since the dump doesn't have the schema
                # qualification.
                cur.execute("SET search_path TO %s", (local_mountpoint,))
                for chunk in f.readlines():
                    cur.execute(chunk)
                # Set the schema to (presumably) the default one.
                cur.execute("SET search_path TO public")


def _http_download_objects(conn, local_mountpoint, objects_to_fetch, http_params):
    username = http_params.get('username')
    password = http_params.get('password')

    for i, obj in enumerate(objects_to_fetch):
        object_id, object_url = obj
        _log("(%d/%d) %s -> %s" % (i+1, len(objects_to_fetch), object_url, object_id))
        # Let's execute arbitrary code from the Internet on our machine!
        r = requests.get(object_url, stream=True)
        with conn.cursor() as cur:
            # Insert into the locally checked out schema by default since the dump doesn't have the schema
            # qualification.
            cur.execute("SET search_path TO %s", (local_mountpoint,))
            buf = ""
            for chunk in r.iter_content(chunk_size=4096):
                # This is dirty. What we want is to pipe the output of the fetch directly into the DB, but we don't
                # actually know when a SQL statement has terminated so we can ship it off: in particular, the
                # semicolon might have been escaped etc. Hence this horror which might/will break on
                # convoluted data.
                buf = buf + chunk
                last_sep = buf.rfind(';\n')
                cur.execute(buf[:last_sep])
                buf = buf[last_sep+2:]
            cur.execute(buf)
        # Set the schema to (presumably) the default one.
        cur.execute("SET search_path TO public")


def upload_objects(conn, local_mountpoint, remote_conn_string, remote_mountpoint, objects_to_push,
                   handler='DB', handler_params={}):
    match = re.match('(\S+):(\S+)@(.+):(\d+)/(\S+)', remote_conn_string)
    remote_conn = make_conn(server=match.group(3), port=int(match.group(4)), username=match.group(1),
                            password=match.group(2), dbname=match.group(5))
    existing_objects = get_existing_objects(remote_conn, remote_mountpoint)
    objects_to_push = list(set(o for o in objects_to_push if o not in existing_objects))
    _log("Uploading objects...")

    if handler == 'DB':
        # Difference from pull here: since we can't get remote to mount us, we instead use normal SQL statements
        # to create new tables remotely, then mount them and write into them from our side.
        # Is there seriously no better way to do this?
        with remote_conn.cursor() as cur:
            # This also includes applying our table's FK constraints.
            cur.execute(dump_table_creation(conn, schema=local_mountpoint,
                                            tables=objects_to_push, created_schema=remote_mountpoint))
        # Have to commit the remote connection here since otherwise we won't see the new tables in the
        # mounted remote.
        remote_conn.commit()
        remote_data_mountpoint = 'tmp_remote_data'
        unmount(conn, remote_data_mountpoint)
        mount_postgres(conn, server=match.group(3), port=int(match.group(4)),
                       username=match.group(1), password=match.group(2), mountpoint=remote_data_mountpoint,
                       extra_options={'dbname': match.group(5), 'remote_schema': remote_mountpoint})
        for i, obj in enumerate(objects_to_push):
            _log("(%d/%d) %s..." % (i + 1, len(objects_to_push), obj))
            with conn.cursor() as cur:
                cur.execute(SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
                    Identifier(remote_data_mountpoint), Identifier(obj),
                    Identifier(local_mountpoint), Identifier(obj)))
        conn.commit()
        unmount(conn, remote_data_mountpoint)

        # We assume that if the object doesn't have an explicit location, it lives on the remote.
        result = []
    elif handler == 'HTTP':
        uploaded = _http_upload_objects(conn, local_mountpoint, objects_to_push, handler_params)
        result = [(oid, url, 'HTTP') for oid, url in zip(objects_to_push, uploaded)]
    elif handler == 'FILE':
        uploaded = _file_upload_objects(conn, local_mountpoint, objects_to_push, handler_params)
        result = [(oid, url, 'FILE') for oid, url in zip(objects_to_push, uploaded)]

    remote_conn.close()
    return result
