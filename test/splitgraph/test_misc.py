from splitgraph._data.common import ensure_metadata_schema
from splitgraph._data.registry import setup_registry_mode, get_published_info, _ensure_registry_schema
from splitgraph.config import SPLITGRAPH_META_SCHEMA, REGISTRY_META_SCHEMA
from splitgraph.core.engine import get_current_repositories
from splitgraph.engine import switch_engine
from test.splitgraph.conftest import PG_MNT, REMOTE_ENGINE


def test_metadata_schema(local_engine_with_pg):
    # Exercise the metadata schema creation code since it might never get reached
    # in test runs where the schema already exists
    try:
        local_engine_with_pg.delete_schema(SPLITGRAPH_META_SCHEMA)
        ensure_metadata_schema()
        assert get_current_repositories() == []
    finally:
        local_engine_with_pg.rollback()


def test_registry_schema(remote_engine):
    # Similar idea -- exercise the registry meta schema code if for when it's already set up,
    try:
        remote_engine.delete_schema(REGISTRY_META_SCHEMA)
        with switch_engine(REMOTE_ENGINE):
            _ensure_registry_schema()
            setup_registry_mode()
            assert get_published_info(PG_MNT, 'latest') is None
    finally:
        remote_engine.rollback()
