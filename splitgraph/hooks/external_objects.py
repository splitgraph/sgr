"""
Hooks for registering handlers to upload/download objects from external locations into Splitgraph's cache.
"""
from importlib import import_module

from splitgraph.config import CONFIG
from splitgraph.exceptions import SplitGraphException

_EXTERNAL_OBJECT_HANDLERS = {}


class ExternalObjectHandler:
    """
    Framework for allowing to dump objects from the Splitgraph cache to an external location. This allows
    the objects to be stored somewhere other than the actual remote engine.

    External object handlers must extend this class and be registered in the Splitgraph config.

    For an example of how this can be used, see splitgraph.hooks.s3: it's a handler allowing objects to be
    uploaded to S3/S3-compatible host using the Minio API. It's registered in the config as follows::

        [external_handlers]
        S3=splitgraph.hooks.s3.S3ExternalObjectHandler

    The protocol and the URLs returned by this handler are stored in splitgraph_meta.external_objects
    and used to download the objects back into the Splitgraph cache when they are needed.
    """

    def __init__(self, params):
        """
        Initialize the handler with a dictionary of parameters (e.g. remote access keys etc).
        The instantiation happens at the beginning of a pull/push.

        :param params: Extra params to be passed to the handler from the commandline invocation
            of push/pull.
        """
        self.params = params

    def upload_objects(self, objects):
        """Upload objects from the Splitgraph cache to an external location

        :param objects: List of object IDs to upload
        :return: A list of URLs (same length as `objects`) that the objects can be found at.
        """

    def download_objects(self, objects):
        """Download objects from the external location into the Splitgraph cache.

        :param objects: List of tuples `(object_id, object_url)` that this handler had previosly
            uploaded the objects to.
        """


def get_external_object_handler(name, handler_params):
    """Load an external protocol handler by its name, initializing it with optional parameters."""
    try:
        handler_class = _EXTERNAL_OBJECT_HANDLERS[name]
        return handler_class(handler_params)
    except KeyError:
        raise SplitGraphException("Protocol %s is not supported!" % name)


def register_upload_download_handler(name, handler_class):
    """Register an external protocol handler. See the docstring for `get_upload_download_handler` for the required
    signatures of the handler functions."""
    global _EXTERNAL_OBJECT_HANDLERS
    if name in _EXTERNAL_OBJECT_HANDLERS:
        raise SplitGraphException("Cannot register a protocol handler %s as it already exists!" % name)
    _EXTERNAL_OBJECT_HANDLERS[name] = handler_class


def _register_default_handlers():
    for handler_name, handler_class_name in CONFIG.get('external_handlers', {}).items():
        ix = handler_class_name.rindex('.')
        try:
            handler_class = getattr(import_module(handler_class_name[:ix]), handler_class_name[ix + 1:])
            register_upload_download_handler(handler_name, handler_class)
        except AttributeError as e:
            raise SplitGraphException("Error loading external object handler {0}".format(handler_name), e)
        except ImportError as e:
            raise SplitGraphException("Error loading external object handler {0}".format(handler_name), e)


# Register the default object handlers from the config
_register_default_handlers()
