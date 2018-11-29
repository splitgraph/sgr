"""
Hooks for registering handlers to upload/download objects from external locations into Splitgraph's cache.
"""

from splitgraph.exceptions import SplitGraphException
from splitgraph.hooks.s3 import s3_upload_objects, s3_download_objects

_EXTERNAL_OBJECT_HANDLERS = {}


# TODO refactor this (and the mount handlers) so that it matches the custom Splitfile command structure
# (extends a base class, extra object handlers added via a config)


def get_upload_download_handler(name):
    """Returns object upload/download function for a given handler (protocol).
    The upload function has a signature `(list_of_object_ids, handler_params)`. It uploads objects from
    the local cache to an external location and returns a list of URLs (same length as `list_of_object_ids`)
    that the objects can be found at.
    The download function is passed `(objects_to_download, handler_params)` where `objects_to_download` is a list
    of tuples `(object_id, object_url)`. It fetches objects from the external location and stores them in the local
    cache.
    """
    try:
        return _EXTERNAL_OBJECT_HANDLERS[name]
    except KeyError:
        raise SplitGraphException("Protocol %s is not supported!" % name)


def register_upload_download_handler(name, upload_handler, download_handler):
    """Register an external protocol handler. See the docstring for `get_upload_download_handler` for the required
    signatures of the handler functions."""
    global _EXTERNAL_OBJECT_HANDLERS
    if name in _EXTERNAL_OBJECT_HANDLERS:
        raise SplitGraphException("Cannot register a protocol handler %s as it already exists!" % name)
    _EXTERNAL_OBJECT_HANDLERS[name] = (upload_handler, download_handler)


# Register the default object handlers. Just like for the mount handlers, we'll probably expose this via a config
# or allow the user to run their own bit of Python before sgr gets invoked.
register_upload_download_handler('S3', upload_handler=s3_upload_objects, download_handler=s3_download_objects)
