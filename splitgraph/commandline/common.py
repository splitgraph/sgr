"""
Various common functions used by the command line interface.
"""
import json
from typing import Optional, Tuple, List, TYPE_CHECKING, Union

import click
from click.core import Context, Parameter

from splitgraph.core.common import parse_repo_tag_or_hash
from splitgraph.exceptions import RepositoryNotFoundError

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository
    from splitgraph.core.image import Image


class ImageType(click.ParamType):
    """Parser that extracts the full image specification (repository and hash/tag)."""

    name = "Image"

    def __init__(
        self,
        default: Optional[str] = "latest",
        repository_exists: bool = False,
        get_image: bool = False,
    ) -> None:
        """
        :param default: Default tag/hash for image where it's not specified.
        """
        self.default = default
        self.repository_exists = repository_exists
        self.get_image = get_image

    def convert(
        self, value: str, param: Optional[Parameter], ctx: Optional[Context]
    ) -> Tuple["Repository", Optional[Union["Image", str]]]:
        """
        Image specification must have the format [NAMESPACE/]REPOSITORY[:HASH_OR_TAG].

        The parser returns a tuple of (repository object, tag or hash).
        """
        repo, tag_or_hash = parse_repo_tag_or_hash(value, default=self.default)

        if self.get_image or self.repository_exists:
            # Check image/repo exists if we're asked (or if we need to produce
            # an actual Image object)
            from splitgraph.core.engine import repository_exists

            if not repository_exists(repo):
                raise RepositoryNotFoundError("Unknown repository %s" % repo)

        if tag_or_hash is not None and self.get_image:
            return repo, repo.images[tag_or_hash]
        else:
            return repo, tag_or_hash


class RepositoryType(click.ParamType):
    name = "Repository"

    def __init__(self, exists: bool = False) -> None:
        self.exists = exists

    def convert(
        self, value: str, param: Optional[Parameter], ctx: Optional[Context]
    ) -> "Repository":
        from splitgraph.core.repository import Repository

        result = Repository.from_schema(value)
        if self.exists:
            from splitgraph.core.engine import repository_exists

            if not repository_exists(result):
                raise RepositoryNotFoundError("Unknown repository %s" % result)
        return result


class JsonType(click.ParamType):
    """Parser for Json -- a wrapper around json.loads because without specifying
    the name Click shows the type for the option/arg as LOADS."""

    name = "Json"

    def convert(self, value: str, param: Optional[Parameter], ctx: Optional[Context]):
        return json.loads(value)


class Color:
    """
    An enumeration of console colors
    """

    PURPLE = "\033[95m"
    CYAN = "\033[96m"
    DARKCYAN = "\033[36m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"


def print_table(rows: List[Tuple[str, ...]], column_width: int = 15) -> None:
    """Print a list of rows with a constant column width"""
    click.echo(
        "\n".join(["".join([("{:" + str(column_width) + "}").format(x) for x in r]) for r in rows])
    )
