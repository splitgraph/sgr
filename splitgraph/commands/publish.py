import logging
from datetime import datetime

from splitgraph.commands import provenance
from splitgraph.commands.misc import make_conn
from splitgraph.constants import SplitGraphException, parse_connection_string
from splitgraph.meta_handler import get_remote_for, get_tagged_id
from splitgraph.registry_meta_handler import publish_tag


def publish(conn, repository, tag, readme=""):
    """
    Summarizes the data on a previously-pushed repository and makes it available in the catalog.
    :param conn: Psycopg connection object.
    :param repository: Repository to be published. The repository must exist on the remote.
    :param tag: Image tag to be published.
    :param readme: Optional README for the repository.
    :return:
    """
    remote_info = get_remote_for(conn, repository, 'origin')
    if not remote_info:
        raise SplitGraphException("No remote found for repository %s. Has it been pushed?" % repository)
    remote_conn_string, remote_mountpoint = remote_info
    image_hash = get_tagged_id(conn, repository, tag)
    logging.info("Publishing %s:%s (%s)" % (repository, image_hash, tag))
    dependencies = provenance(conn, repository, image_hash)

    remote_conn = make_conn(*parse_connection_string(remote_conn_string))
    try:
        publish_tag(remote_conn, remote_mountpoint, tag, image_hash, datetime.now(), dependencies, readme)
        remote_conn.commit()
    finally:
        remote_conn.close()
