"""
Various common functions used by the command line interface.
"""

from splitgraph import to_repository


def image_spec_parser(default='latest'):
    """
    Makes a parser that extracts the full image specification (repository and hash/tag).

    Image specification must have the format [NAMESPACE/]REPOSITORY[:HASH_OR_TAG].

    The parser returns a tuple of (repository object, tag or hash).

    :param default: Default tag/hash for image where it's not specified.
    """

    def image_spec(spec):
        repo_image = spec.split(':')

        if len(repo_image) == 2:
            tag_or_hash = repo_image[1]
        else:
            tag_or_hash = default
        return to_repository(repo_image[0]), tag_or_hash

    return image_spec


class Color:
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
