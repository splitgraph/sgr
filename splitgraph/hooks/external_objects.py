"""
Hooks for registering handlers to upload/download objects from external locations into Splitgraph's cache.
"""
from importlib import import_module
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Sequence, Tuple

from splitgraph.config import CONFIG
from splitgraph.config.config import get_from_section
from splitgraph.exceptions import ExternalHandlerError

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PsycopgEngine


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

    def __init__(self, params: Dict[Any, Any]) -> None:
        """
        Initialize the handler with a dictionary of parameters (e.g. remote access keys etc).
        The instantiation happens at the beginning of a pull/push.

        :param params: Extra params to be passed to the handler from the commandline invocation
            of push/pull.
        """
        self.params = params

    def upload_objects(
        self, objects: List[str], remote_engine: "PsycopgEngine"
    ) -> Sequence[Tuple[str, str]]:
        """Upload objects from the Splitgraph cache to an external location

        :param objects: List of object IDs to upload
        :param remote_engine: An instance of Engine class that the objects will be registered on
        :return: A list of successfully uploaded object IDs and URLs they can be found at.
        """

    def download_objects(
        self, objects: List[Tuple[str, str]], remote_engine: "PsycopgEngine"
    ) -> Sequence[str]:
        """Download objects from the external location into the Splitgraph cache.

        :param objects: List of tuples `(object_id, object_url)` that this handler had previosly
            uploaded the objects to.
        :param remote_engine: An instance of Engine class that the objects will be registered on
        :return: A list of object IDs that have been successfully downloaded.
        """


_EXTERNAL_OBJECT_HANDLERS: Dict[str, Callable[..., ExternalObjectHandler]] = {}


def get_external_object_handler(name: str, handler_params: Dict[Any, Any]) -> ExternalObjectHandler:
    """Load an external protocol handler by its name, initializing it with optional parameters."""
    try:
        handler_class = _EXTERNAL_OBJECT_HANDLERS[name]
        return handler_class(handler_params)
    except KeyError:
        try:
            handler_class_name = get_from_section(CONFIG, "external_handlers", name)
        except KeyError:
            raise ExternalHandlerError("Protocol %s is not supported!" % name)

        index = handler_class_name.rindex(".")
        try:
            handler_class = getattr(
                import_module(handler_class_name[:index]), handler_class_name[index + 1 :]
            )
            register_upload_download_handler(name, handler_class)
            return handler_class(handler_params)
        except AttributeError as e:
            raise ExternalHandlerError(
                "Error loading external object handler {0}".format(name)
            ) from e
        except ImportError as e:
            raise ExternalHandlerError(
                "Error loading external object handler {0}".format(name)
            ) from e


def register_upload_download_handler(
    name: str, handler_class: Callable[..., ExternalObjectHandler]
) -> None:
    """Register an external protocol handler. See the docstring for `get_upload_download_handler` for the required
    signatures of the handler functions."""
    global _EXTERNAL_OBJECT_HANDLERS
    if name in _EXTERNAL_OBJECT_HANDLERS:
        raise ExternalHandlerError(
            "Cannot register a protocol handler %s as it already exists!" % name
        )
    _EXTERNAL_OBJECT_HANDLERS[name] = handler_class
