"""
Routines for managing the connections to local and remote Splitgraph engines.
"""

import re


def parse_connection_string(conn_string):
    """
    :return: a tuple (server, port, username, password, dbname)
    """
    match = re.match(r'(\S+):(\S+)@(.+):(\d+)/(\S+)', conn_string)
    if not match:
        raise ValueError("Connection string doesn't match the format!")
    return match.group(3), int(match.group(4)), match.group(1), match.group(2), match.group(5)


def serialize_connection_string(server, port, username, password, dbname):
    """
    Serializes a tuple into a Splitgraph engine connection string.
    """
    return '%s:%s@%s:%s/%s' % (username, password, server, port, dbname)
