"""
Various common functions used by the command line interface.
"""
from typing import Optional, Tuple, Union, List, TYPE_CHECKING

import click
from click.core import Argument, Context, Option, Parameter

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository


class ImageType(click.ParamType):
    """Parser that extracts the full image specification (repository and hash/tag)."""

    name = "Image"

    def __init__(self, default: str = "latest") -> None:
        """
        :param default: Default tag/hash for image where it's not specified.
        """
        self.default = default

    def convert(
        self, value: str, param: Optional[Parameter], ctx: Optional[Context]
    ) -> Union[Tuple["Repository", str], Tuple["Repository", None]]:
        """
        Image specification must have the format [NAMESPACE/]REPOSITORY[:HASH_OR_TAG].

        The parser returns a tuple of (repository object, tag or hash).
        """
        repo_image = value.split(":")

        if len(repo_image) == 2:
            tag_or_hash = repo_image[1]
        else:
            tag_or_hash = self.default
        from splitgraph.core import Repository

        return Repository.from_schema(repo_image[0]), tag_or_hash


class RepositoryType(click.ParamType):
    name = "Repository"

    def convert(
        self, value: str, param: Optional[Parameter], ctx: Optional[Context]
    ) -> "Repository":
        from splitgraph.core import Repository

        return Repository.from_schema(value)


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
