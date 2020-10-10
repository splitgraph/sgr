import re
import time
from datetime import datetime, date
from typing import Union, List, Any, Optional, Dict


def pretty_size(size: Union[int, float]) -> str:
    """
    Converts a size in bytes to its string representation (e.g. 1024 -> 1KiB)
    :param size: Size in bytes
    """
    size = float(size)
    power = 2 ** 10
    base = 0
    while size > power:
        size /= power
        base += 1

    return "%.2f %s" % (size, {0: "", 1: "Ki", 2: "Mi", 3: "Gi", 4: "Ti"}[base] + "B")


def pluralise(word: str, number: int) -> str:
    """1 banana, 2 bananas"""
    return "%d %s%s" % (number, word, "" if number == 1 else "s")


def truncate_line(line: str, length: int = 80) -> str:
    """Truncates a line to a given length, replacing the remainder with ..."""
    return (line if len(line) <= length else line[: length - 3] + "...").replace("\n", "")


def truncate_list(items: List[Any], max_entries: int = 10) -> str:
    """Print a list, possibly truncating it to the specified number of entries"""
    return ",".join(str(i) for i in items[:max_entries]) + (
        ", ..." if len(items) > max_entries else ""
    )


_slugify = re.compile(r"[^\sa-zA-Z0-9]")


def slugify(text: str, max_length: int = 50) -> str:
    text = _slugify.sub("", text.lower()).strip()
    parts = re.split(r"\s+", text)
    result = parts[0]
    for p in parts[1:]:
        new = result + "_" + p
        if len(new) > max_length:
            break
        result = new
    return result[:max_length]


def parse_repo_tag_or_hash(value, default="latest"):
    repo_image = value.split(":")
    tag_or_hash: Optional[str]
    if len(repo_image) == 2:
        tag_or_hash = repo_image[1]
    else:
        tag_or_hash = default
    from splitgraph.core.repository import Repository

    repo = Repository.from_schema(repo_image[0])
    return repo, tag_or_hash


def conn_string_to_dict(connection: Optional[str]) -> Dict[str, Any]:
    if connection:
        match = re.match(r"((\S+):(\S+)@)?(.+):(\d+)", connection)
        if not match:
            raise ValueError("Invalid connection string!")
        # In the future, we could turn all of these options into actual Click options,
        # but then we'd also have to parse the docstring deeper to find out the types the function
        # requires, how to serialize them etc etc. Idea for a click-contrib addon perhaps?
        return dict(
            host=match.group(4),
            port=int(match.group(5)),
            username=match.group(2),
            password=match.group(3),
        )
    else:
        return dict(server=None, port=None, username=None, password=None)


def parse_dt(string: str) -> datetime:
    _formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
    ]
    for fmt in _formats:
        try:
            return datetime.strptime(string, fmt)
        except ValueError:
            continue

    raise ValueError("Unknown datetime format for string %s!" % string)


def parse_date(string: str) -> date:
    return datetime.strptime(string, "%Y-%m-%d").date()


def parse_time(string: str) -> time.struct_time:
    _formats = [
        "%H:%M:%S",
        "%H:%M:%S.%f",
    ]
    for fmt in _formats:
        try:
            return time.strptime(string, fmt)
        except ValueError:
            continue

    raise ValueError("Unknown time format for string %s!" % string)
