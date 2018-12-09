"""
Routines for managing the connections to local and remote Splitgraph drivers.
"""

import re
from contextlib import contextmanager

import psycopg2

from splitgraph.config import CONFIG, PG_DB, PG_USER, PG_PWD, PG_HOST, PG_PORT

_PSYCOPG_CONN = None


@contextmanager
def override_driver_connection(conn):
    """
    Override the default psycopg connection to the driver. The old value will be restored
    on exit from the manager.

    :param conn: Psycopg connection object.
    """
    global _PSYCOPG_CONN
    # Keep track of the old value (so that if we have several context manager calls in the call stack,
    # the old values get restored as we move back
    old_override = _PSYCOPG_CONN
    _PSYCOPG_CONN = conn
    try:
        yield
    finally:
        _PSYCOPG_CONN = old_override


@contextmanager
def do_in_driver(remote=None):
    """
    Switches the global connection to one pointing to a specific (or current) driver.

    :param remote: Name of the driver as specified in the config. If None, the current driver is used.
    """
    if remote:
        with make_driver_connection(remote) as conn:
            with override_driver_connection(conn):
                yield
    else:
        yield


def get_connection():
    """
    Get a connection to the current Splitgraph driver.
    :return: Psycopg connection object
    """
    global _PSYCOPG_CONN
    if not _PSYCOPG_CONN:
        _PSYCOPG_CONN = psycopg2.connect(dbname=PG_DB,
                                         user=PG_USER,
                                         password=PG_PWD,
                                         host=PG_HOST,
                                         port=PG_PORT)
    return _PSYCOPG_CONN


def commit_and_close_connection():
    """Commit/close the current global driver connection. If the connection
    doesn't exist, this does nothing."""
    global _PSYCOPG_CONN
    if _PSYCOPG_CONN:
        _PSYCOPG_CONN.commit()
        _PSYCOPG_CONN.close()
        _PSYCOPG_CONN = None


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
    Serializes a tuple into a Splitgraph driver connection string.
    """
    return '%s:%s@%s:%s/%s' % (username, password, server, port, dbname)


def make_conn(server, port, username, password, dbname):
    """
    Initializes a connection to the Splitgraph driver.
    :return: Psycopg connection object
    """
    return psycopg2.connect(host=server, port=port, user=username, password=password, dbname=dbname)


def make_driver_connection(remote_name):
    """
    Creates a connection to a remote driver.
    :param remote_name: Name of the remote driver as specified in the config file
    :return: Psycopg connection object
    """
    return make_conn(*get_remote_connection_params(remote_name))


def get_remote_connection_params(remote_name):
    """
    Gets connection parameters for a Splitgraph remote.
    :param remote_name: Name of the remote. Must be specified in the config file.
    :return: A tuple of (hostname, port, username, password, database)
    """
    pdict = CONFIG['remotes'][remote_name]
    return (pdict['SG_DRIVER_HOST'], int(pdict['SG_DRIVER_PORT']), pdict['SG_DRIVER_USER'],
            pdict['SG_DRIVER_PWD'], pdict['SG_DRIVER_DB_NAME'])
