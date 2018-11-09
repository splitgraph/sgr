from splitgraph.constants import SplitGraphException
from splitgraph.objects.s3 import _s3_upload_objects, _s3_download_objects

EXTERNAL_OBJECT_HANDLERS = {}


def get_upload_download_handler(name):
    """Returns object upload/download function for a given handler (protocol).
    The upload function has a signature `(conn, list_of_object_ids, handler_params)`. It uploads objects from
    the local cache to an external location and returns a list of URLs (same length as `list_of_object_ids`)
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


# Register the default object handlers. Just like for the mount handlers, we'll probably expose this via a config
# or allow the user to run their own bit of Python before sg gets invoked.
register_upload_download_handler('S3', upload_handler=_s3_upload_objects, download_handler=_s3_download_objects)
