import requests

from splitgraph.pg_utils import table_dump_generator
from splitgraph.constants import SplitGraphException, log, SPLITGRAPH_META_SCHEMA

EXTERNAL_OBJECT_HANDLERS = {}


def get_upload_download_handler(name):
    """Returns object upload/download function for a given handler (protocol).
    The upload function has a signature `(conn, list_of_object_ids, handler_params)`. It uploads objects from
    the local cache to an exteral location and returns a list of URLs (same length as `list_of_object_ids`)
    that the objects can be found at.
    The download function is passed `(conn, objects_to_download, handler_params)` where `objects_to_download` is a list
    of tuples `(object_id, object_url)`. It fetches objects from the external location and stores them in the local
    cache.
    """
    try:
        return EXTERNAL_OBJECT_HANDLERS[name]
    except KeyError:
        raise SplitGraphException("Protocol %s is not supported!" % name)


def register_upload_download_handler(name, upload_handler, download_handler):
    """Register an external protocol handler. See the docstring for `get_upload_download_handler` for the required
    signatures of the handler functions."""
    global EXTERNAL_OBJECT_HANDLERS
    if name in EXTERNAL_OBJECT_HANDLERS:
        raise SplitGraphException("Cannot register a protocol handler %s as it already exists!" % name)
    EXTERNAL_OBJECT_HANDLERS[name] = (upload_handler, download_handler)


def _http_upload_objects(conn, objects_to_push, http_params):
    url = http_params['url']

    uploaded = []
    for object_id in objects_to_push:
        remote_filename = object_id
        # First cut: just push the dump without any compression.
        r = requests.post(url + '/' + remote_filename,
                          data=table_dump_generator(conn, SPLITGRAPH_META_SCHEMA, object_id))
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
            f.writelines(table_dump_generator(conn, SPLITGRAPH_META_SCHEMA, object_id))
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


# Register the default object handlers. Just like for the mount handlers, we'll probably expose this via a config
# or allow the user to run their own bit of Python before sg gets invoked.
register_upload_download_handler('FILE', upload_handler=_file_upload_objects, download_handler=_file_download_objects)
register_upload_download_handler('HTTP', upload_handler=_http_upload_objects, download_handler=_http_download_objects)