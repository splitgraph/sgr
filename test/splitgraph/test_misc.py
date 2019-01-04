from splitgraph.config import SPLITGRAPH_META_SCHEMA, REGISTRY_META_SCHEMA
from splitgraph.core._common import ensure_metadata_schema
from splitgraph.core.engine import get_current_repositories
from splitgraph.core.registry import setup_registry_mode, get_published_info, _ensure_registry_schema
from splitgraph.core.repository import Repository


def test_metadata_schema(pg_repo_local):
    # Exercise the metadata schema creation code since it might never get reached
    # in test runs where the schema already exists
    try:
        pg_repo_local.engine.delete_schema(SPLITGRAPH_META_SCHEMA)
        ensure_metadata_schema(pg_repo_local.engine)
        assert get_current_repositories(pg_repo_local.engine) == []
    finally:
        pg_repo_local.engine.rollback()


def test_registry_schema(remote_engine):
    # Similar idea -- exercise the registry meta schema code if for when it's already set up,
    try:
        remote_engine.delete_schema(REGISTRY_META_SCHEMA)
        _ensure_registry_schema(remote_engine)
        setup_registry_mode(remote_engine)
        assert get_published_info(Repository('test', 'pg_mount', remote_engine), 'latest') is None
    finally:
        remote_engine.rollback()
