"""
Various common functions used by the command line interface.
"""
from typing import Optional, Tuple, List, TYPE_CHECKING

import click
from click.core import Context, Parameter

from splitgraph.exceptions import RepositoryNotFoundError

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository


class ImageType(click.ParamType):
    """Parser that extracts the full image specification (repository and hash/tag)."""

    name = "Image"

    def __init__(self, default: Optional[str] = "latest") -> None:
        """
        :param default: Default tag/hash for image where it's not specified.
        """
        self.default = default

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

        return Repository.from_schema(repo_image[0]), tag_or_hash


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


def patch_and_save_config(config, patch):
    from splitgraph.config.config import patch_config
    from splitgraph.config.system_config import HOME_SUB_DIR
    from splitgraph.config.export import overwrite_config
    from pathlib import Path
    import os

    config_path = config["SG_CONFIG_FILE"]
    if not config_path:
        # Default to creating a config in the user's homedir rather than local.
        config_dir = Path(os.environ["HOME"]) / Path(HOME_SUB_DIR)
        config_path = config_dir / Path(".sgconfig")
        click.echo("No config file detected, creating one at %s" % config_path)
        config_dir.mkdir(exist_ok=True, parents=True)
    else:
        click.echo("Updating the existing config file at %s" % config_path)
    new_config = patch_config(config, patch)
    overwrite_config(new_config, config_path)
    return config_path
