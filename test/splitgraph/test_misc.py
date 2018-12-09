from psycopg2.sql import Identifier, SQL

from splitgraph import get_current_repositories, get_connection
from splitgraph._data.common import ensure_metadata_schema
from splitgraph._data.registry import setup_registry_mode, get_published_info, _ensure_registry_schema
from splitgraph.config import SPLITGRAPH_META_SCHEMA, REGISTRY_META_SCHEMA
from splitgraph.connection import override_driver_connection
from test.splitgraph.conftest import PG_MNT


def test_metadata_schema(sg_pg_conn):
    # Exercise the metadata schema creation code since it might never get reached
    # in test runs where the schema already exists
    try:
        with get_connection().cursor() as cur:
            cur.execute(SQL("DROP SCHEMA {} CASCADE").format(Identifier(SPLITGRAPH_META_SCHEMA)))
        ensure_metadata_schema()
        assert get_current_repositories() == []
    finally:
        get_connection().rollback()


def test_registry_schema(remote_driver_conn):
    # Similar idea -- exercise the registry meta schema code if for when it's already set up,
    try:
        with remote_driver_conn.cursor() as cur:
            cur.execute(SQL("DROP SCHEMA {} CASCADE").format(Identifier(REGISTRY_META_SCHEMA)))
        with override_driver_connection(remote_driver_conn):
            _ensure_registry_schema()
            setup_registry_mode()
            assert get_published_info(PG_MNT, 'latest') is None
    finally:
        remote_driver_conn.rollback()
