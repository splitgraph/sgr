"""
Utility functions for console output
"""


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
