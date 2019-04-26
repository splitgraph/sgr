"""
Exceptions that can be raised by the Splitgraph library.
"""


class SplitGraphException(Exception):
    """
    A generic Splitgraph exception
    """


class UnsupportedSQLException(SplitGraphException):
    """Raised for unsupported SQL statements, for example, containing schema-qualified tables when the statement
    is supposed to be used in an SQL/IMPORT Splitfile command."""
