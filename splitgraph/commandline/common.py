"""
Various common functions used by the command line interface.
"""
import json
from typing import Optional, Tuple, List, TYPE_CHECKING

import click
from click.core import Context, Parameter

from splitgraph.exceptions import RepositoryNotFoundError

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository


class ImageType(click.ParamType):
    """Parser that extracts the full image specification (repository and hash/tag)."""

    name = "Image"

    def __init__(
        self,
        default: Optional[str] = "latest",
        repository_exists: bool = False,
        image_exists: bool = False,
    ) -> None:
        """
        :param default: Default tag/hash for image where it's not specified.
        """
        self.default = default
        self.repository_exists = repository_exists
        self.image_exists = image_exists

    def convert(
        self, value: str, param: Optional[Parameter], ctx: Optional[Context]
    ) -> Tuple["Repository", Optional[str]]:
        """
        Image specification must have the format [NAMESPACE/]REPOSITORY[:HASH_OR_TAG].

        The parser returns a tuple of (repository object, tag or hash).
        """
        repo_image = value.split(":")

        tag_or_hash: Optional[str]
        if len(repo_image) == 2:
            tag_or_hash = repo_image[1]
        else:
            tag_or_hash = self.default
        from splitgraph.core.repository import Repository

        repo = Repository.from_schema(repo_image[0])

        if self.image_exists or self.repository_exists:
            # Check image/repo exists if we're asked
            # image_exists supersedes repository_exists
            from splitgraph.core.engine import repository_exists

            if not repository_exists(repo):
                raise RepositoryNotFoundError("Unknown repository %s" % repo)

            if self.image_exists and tag_or_hash is not None:
                _ = repo.images[tag_or_hash]

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


def truncate_line(line: str, length: int = 80) -> str:
    """Truncates a line to a given length, replacing the remainder with ..."""
    return (line if len(line) <= length else line[: length - 3] + "...").replace("\n", "")


def pluralise(word: str, number: int) -> str:
    """1 banana, 2 bananas"""
    return "%d %s%s" % (number, word, "" if number == 1 else "s")


def print_table(rows: List[Tuple[str, ...]], column_width: int = 15) -> None:
    """Print a list of rows with a constant column width"""
    click.echo(
        "\n".join(["".join([("{:" + str(column_width) + "}").format(x) for x in r]) for r in rows])
    )
