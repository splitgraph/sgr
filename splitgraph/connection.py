from contextlib import contextmanager

import psycopg2

from splitgraph.constants import POSTGRES_CONNECTION

_PSYCOPG_CONN = None


@contextmanager
def override_driver_connection(conn):
    """
    Override the default psycopg connection to the driver. The old value will be restored
    on exit from the manager.
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


def get_connection():
    global _PSYCOPG_CONN
    if not _PSYCOPG_CONN:
        _PSYCOPG_CONN = psycopg2.connect(POSTGRES_CONNECTION)
    return _PSYCOPG_CONN
