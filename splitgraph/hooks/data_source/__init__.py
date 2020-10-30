import logging
import types
from importlib import import_module
from typing import Dict, Type, List

from .base import DataSource
from .fdw import PostgreSQLDataSource, MongoDataSource, ElasticSearchDataSource, MySQLDataSource
from ...config import CONFIG, get_singleton
from ...config.config import get_all_in_section
from ...config.keys import DEFAULTS
from ...exceptions import DataSourceError

_DATA_SOURCES: Dict[str, Type[DataSource]] = {}


def get_data_source(data_source: str) -> Type[DataSource]:
    """Returns a class for a given data source"""
    try:
        return _DATA_SOURCES[data_source]
    except KeyError:
        raise DataSourceError("Data source %s not supported!" % data_source)


def get_data_sources() -> List[str]:
    """Returns the names of all registered data sources."""
    return list(_DATA_SOURCES.keys())


def register_data_source(name: str, data_source_class: Type[DataSource]) -> None:
    """Returns a data source under a given name."""
    global _DATA_SOURCES
    _DATA_SOURCES[name] = data_source_class


def _register_default_data_sources() -> None:
    # Register the data sources from the config.
    for source_name, source_class_name in get_all_in_section(CONFIG, "data_sources").items():
        assert isinstance(source_class_name, str)

        try:
            data_source = _load_source(source_name, source_class_name)

            assert issubclass(data_source, DataSource)
            register_data_source(source_name.lower(), data_source)
        except (ImportError, AttributeError) as e:
            raise DataSourceError("Error loading custom data source {0}".format(source_name)) from e


def _load_source(source_name, source_class_name):
    # Hack for old-style data sources that have now been moved -- don't crash and instead
    # replace them in the config on the fly
    source_defaults = get_all_in_section(DEFAULTS, "data_sources")

    fallback_used = False

    try:
        ix = source_class_name.rindex(".")
        data_source = getattr(import_module(source_class_name[:ix]), source_class_name[ix + 1 :])
    except (ImportError, AttributeError):
        if source_name not in source_defaults:
            raise
        source_class_name = source_defaults[source_name]
        ix = source_class_name.rindex(".")
        data_source = getattr(import_module(source_class_name[:ix]), source_class_name[ix + 1 :])
        fallback_used = True

    if isinstance(data_source, types.FunctionType):
        if source_name not in source_defaults:
            raise DataSourceError(
                "Handler %s uses the old-style function interface which is not"
                " compatible with this version of Splitgraph. "
                "Delete it from your .sgconfig's [data_sources] section (%s)"
                % (source_name, get_singleton(CONFIG, "SG_CONFIG_FILE"))
            )
        source_class_name = source_defaults[source_name]
        ix = source_class_name.rindex(".")
        data_source = getattr(import_module(source_class_name[:ix]), source_class_name[ix + 1 :])
        fallback_used = True
    if fallback_used:
        logging.warning(
            "Data source %s uses the old-style function interface and was automatically replaced.",
            source_name,
        )
        logging.warning(
            "Replace it with %s=%s in your .sgconfig's [data_sources] section (%s)",
            source_name,
            source_class_name,
            get_singleton(CONFIG, "SG_CONFIG_FILE"),
        )
    return data_source


_register_default_data_sources()
