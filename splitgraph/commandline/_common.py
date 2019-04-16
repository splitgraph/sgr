"""
Various common functions used by the command line interface.
"""
import click


class ImageType(click.ParamType):
    name = 'Image'

    def __init__(self, default='latest'):
        """
        Makes a parser that extracts the full image specification (repository and hash/tag).

        Image specification must have the format [NAMESPACE/]REPOSITORY[:HASH_OR_TAG].

        The parser returns a tuple of (repository object, tag or hash).

        :param default: Default tag/hash for image where it's not specified.
        """
        self.default = default

    def convert(self, value, param, ctx):
        repo_image = value.split(':')

        if len(repo_image) == 2:
            tag_or_hash = repo_image[1]
        else:
            tag_or_hash = self.default
        from splitgraph.core import Repository
        return Repository.from_schema(repo_image[0]), tag_or_hash


class RepositoryType(click.ParamType):
    name = "Repository"

    def convert(self, value, param, ctx):
        from splitgraph.core import Repository
        return Repository.from_schema(value)


class Color:
    """
    An enumeration of console colors
    """
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


def truncate_line(line, length=80):
    """Truncates a line to a given length, replacing the remainder with ..."""
    return (line if len(line) <= length else line[:length - 3] + '...').replace('\n', '')


def pluralise(word, number):
    """1 banana, 2 bananas"""
    return '%d %s%s' % (number, word, '' if number == 1 else 's')
