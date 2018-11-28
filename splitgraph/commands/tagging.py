from psycopg2.sql import SQL, Identifier

from splitgraph._data.common import ensure_metadata_schema, select, insert
from splitgraph._data.misc import repository_exists
from splitgraph.commands.info import get_canonical_image_id
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.connection import get_connection
from splitgraph.exceptions import SplitGraphException


def get_current_head(repository, raise_on_none=True):
    return get_tagged_id(repository, 'HEAD', raise_on_none)


def get_tagged_id(repository, tag, raise_on_none=True):
    ensure_metadata_schema()
    if not repository_exists(repository) and raise_on_none:
        raise SplitGraphException("%s is not mounted." % str(repository))

    if tag == 'latest':
        # Special case, return the latest commit from the repository.
        with get_connection().cursor() as cur:
            cur.execute(select("images", "image_hash", "namespace = %s AND repository = %s")
                        + SQL(" ORDER BY created DESC LIMIT 1"), (repository.namespace, repository.repository,))
            result = cur.fetchone()
            if result is None:
                raise SplitGraphException("No commits found in %s!")
            return result[0]

    with get_connection().cursor() as cur:
        cur.execute(select("tags", "image_hash", "namespace = %s AND repository = %s AND tag = %s"),
                    (repository.namespace, repository.repository, tag))
        result = cur.fetchone()
        if result is None or result == (None,):
            if raise_on_none:
                schema = repository.to_schema()
                if tag == 'HEAD':
                    raise SplitGraphException("No current checked out revision found for %s. Check one out with \"sgr "
                                              "checkout %s image_hash\"." % (schema, schema))
                else:
                    raise SplitGraphException("Tag %s not found in repository %s" % (tag, schema))
            else:
                return None
        return result[0]


def get_all_hashes_tags(repository):
    with get_connection().cursor() as cur:
        cur.execute(select("tags", "image_hash, tag", "namespace = %s AND repository = %s"),
                    (repository.namespace, repository.repository,))
        return cur.fetchall()


def set_tags(repository, tags, force=False):
    for tag, image_id in tags.items():
        if tag != 'HEAD':
            set_tag(repository, image_id, tag, force)


def set_tag(repository, image, tag, force=False):
    with get_connection().cursor() as cur:
        cur.execute(select("tags", "1", "namespace = %s AND repository = %s AND tag = %s"),
                    (repository.namespace, repository.repository, tag))
        if cur.fetchone() is None:
            cur.execute(insert("tags", ("image_hash", "namespace", "repository", "tag")),
                        (image, repository.namespace, repository.repository, tag))
        else:
            if force:
                cur.execute(SQL("UPDATE {}.tags SET image_hash = %s "
                                "WHERE namespace = %s AND repository = %s AND tag = %s").format(
                    Identifier(SPLITGRAPH_META_SCHEMA)),
                    (image, repository.namespace, repository.repository, tag))
            else:
                raise SplitGraphException("Tag %s already exists in %s!" % (tag, repository.to_schema()))


def tag_or_hash_to_actual_hash(repository, tag_or_hash):
    """Converts a tag or shortened hash to a full image hash that exists in the repository.
    """
    try:
        return get_canonical_image_id(repository, tag_or_hash)
    except SplitGraphException:
        try:
            return get_tagged_id(repository, tag_or_hash)
        except SplitGraphException:
            raise SplitGraphException("%s does not refer to either an image commit hash or a tag!" % tag_or_hash)
