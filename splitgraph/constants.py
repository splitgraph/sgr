import logging
from collections import namedtuple

from splitgraph.config import CONFIG

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)

PG_HOST = CONFIG["SG_DRIVER_HOST"]
PG_PORT = CONFIG["SG_DRIVER_PORT"]
PG_DB = CONFIG["SG_DRIVER_DB_NAME"]
PG_USER = CONFIG["SG_DRIVER_USER"]
PG_PWD = CONFIG["SG_DRIVER_PWD"]
POSTGRES_CONNECTION = CONFIG["SG_DRIVER_CONNECTION_STRING"]
SPLITGRAPH_META_SCHEMA = CONFIG["SG_META_SCHEMA"]
REGISTRY_META_SCHEMA = "registry_meta"

S3_HOST = CONFIG["SG_S3_HOST"]
S3_PORT = CONFIG["SG_S3_PORT"]
S3_ACCESS_KEY = CONFIG["SG_S3_KEY"]
S3_SECRET_KEY = CONFIG["SG_S3_PWD"]


class Repository(namedtuple('Repository', ['namespace', 'repository'])):
    """
    A repository object that encapsulates the namespace and the actual repository name.
    """
    def to_schema(self):
        """Converts the object to a Postgres schema."""
        return self.repository if not self.namespace else self.namespace + '/' + self.repository

    __repr__ = to_schema
    __str__ = to_schema


def to_repository(schema):
    """Converts a Postgres schema name of the format `namespace/repository` to a Splitgraph repository object."""
    if '/' in schema:
        namespace, repository = schema.split('/')
        return Repository(namespace, repository)
    return Repository('', schema)
