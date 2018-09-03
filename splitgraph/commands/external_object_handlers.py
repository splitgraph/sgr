import requests

from splitgraph.constants import SplitGraphException, SPLITGRAPH_META_SCHEMA
from splitgraph.pg_utils import table_dump_generator

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
        print("(%d/%d) %s -> %s" % (i + 1, len(objects_to_fetch), object_url, object_id))
        with open(object_url, 'r') as f:
            with conn.cursor() as cur:
                # Insert into the locally checked out schema by default since the dump doesn't have the schema
                # qualification.
                cur.execute("SET search_path TO %s", (SPLITGRAPH_META_SCHEMA,))
                for chunk in f.readlines():
                    cur.execute(chunk)
                # Set the schema to (presumably) the default one.
                cur.execute("SET search_path TO public")


# Register the default object handlers. Just like for the mount handlers, we'll probably expose this via a config
# or allow the user to run their own bit of Python before sg gets invoked.
register_upload_download_handler('FILE', upload_handler=_file_upload_objects, download_handler=_file_download_objects)
