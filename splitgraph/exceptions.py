"""
Exceptions that can be raised by the Splitgraph library.
"""
from typing import List, Optional


class SplitGraphError(Exception):
    """A generic Splitgraph exception."""


class CheckoutError(SplitGraphError):
    """Errors related to checking out/committing repositories"""


class UnsupportedSQLError(SplitGraphError):
    """Raised for unsupported SQL statements, for example, containing schema-qualified tables when the statement
    is supposed to be used in an SQL/IMPORT Splitfile command."""


class EngineInitializationError(SplitGraphError):
    """Raised when the engine isn't initialized (no splitgraph_meta schema or audit triggers)"""


class EngineSetupError(SplitGraphError):
    """Raised on various setup issues with the Splitgraph engine"""


class ObjectCacheError(SplitGraphError):
    """Issues with the object cache (not enough space)"""


class ObjectNotFoundError(SplitGraphError):
    """Raised when a physical object doesn't exist in the cache."""


class ObjectIndexingError(SplitGraphError):
    """Errors related to indexing objects"""


class ObjectMountingError(SplitGraphError):
    """Errors related to incompatible objects."""


class RepositoryNotFoundError(SplitGraphError):
    """A Splitgraph repository doesn't exist."""


class ImageNotFoundError(SplitGraphError):
    """A Splitgraph image doesn't exist."""


class TableNotFoundError(SplitGraphError):
    """A table doesn't exist in an image"""


class SplitfileError(SplitGraphError):
    """Generic error class for Splitfile interpretation/execution errors."""


class ExternalHandlerError(SplitGraphError):
    """Exceptions raised by external object handlers."""


class DataSourceError(SplitGraphError):
    """Exceptions raised by mount handlers."""


class AuthAPIError(SplitGraphError):
    """Exceptions raised by the Auth API"""


class APICompatibilityError(SplitGraphError):
    """Exceptions related to API incompatibilities"""


class DockerUnavailableError(SplitGraphError):
    """Could not connect to the Docker daemon."""


class IncompleteObjectUploadError(SplitGraphError):
    """Raised when an error is encountered during upload of multiple objects.
    The handler is supposed to perform any necessary
    cleanup and reraise `reason` at the earliest opportunity."""

    def __init__(
        self,
        reason: Optional[BaseException],
        successful_objects: List[str],
        successful_object_urls: List[str],
    ):
        self.reason = reason
        self.successful_objects = successful_objects
        self.successful_object_urls = successful_object_urls


class IncompleteObjectDownloadError(SplitGraphError):
    """Raised when an error is encountered during download of multiple objects.
    The handler is supposed to perform any necessary
    cleanup and reraise `reason` at the earliest opportunity."""

    def __init__(
        self,
        reason: Optional[BaseException],
        successful_objects: List[str],
    ):
        self.reason = reason
        self.successful_objects = successful_objects


class GQLAPIError(SplitGraphError):
    """GQL API errors"""


class GQLUnauthorizedError(GQLAPIError):
    """Unauthorized (e.g. repository isn't writeable by the user)"""


class GQLUnauthenticatedError(GQLAPIError):
    """Unauthenticated (user not logged in)"""


class GQLRepoDoesntExistError(GQLAPIError):
    """Repository doesn't exist"""


class JSONSchemaValidationError(SplitGraphError):
    """Error validating the remote schema"""

    def __init__(self, message: str):
        self.message = message


def get_exception_name(o):
    module = o.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return o.__class__.__name__
    return module + "." + o.__class__.__name__
