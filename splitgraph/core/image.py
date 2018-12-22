import logging
from collections import namedtuple

from psycopg2.sql import SQL, Identifier

from splitgraph import get_engine, SplitGraphException
from splitgraph._data.common import select
from splitgraph._data.images import IMAGE_COLS, get_image_object_path
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core._common import set_tag
from splitgraph.core.table import Table
from splitgraph.engine import ResultShape


class Image(namedtuple('Image', IMAGE_COLS)):
    """
    Represents a Splitgraph image. Should't be created directly, use Image-loading methods in the
    Repository class instead.
    """

    def __new__(cls, *args, **kwargs):
        repository = kwargs.pop('repository')
        self = super(Image, cls).__new__(cls, *args, **kwargs)
        self.repository = repository
        return self

    def get_parent_children(self):
        """Gets the parent and a list of children of a given image."""
        parent = self.parent_id

        children = get_engine().run_sql(SQL("""SELECT image_hash FROM {}.images
                WHERE namespace = %s AND repository = %s AND parent_id = %s""")
                                        .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                                        (self.repository.namespace, self.repository.repository, self.image_hash),
                                        return_shape=ResultShape.MANY_ONE)
        return parent, children

    def get_tables(self):
        """
        Gets the names of all tables inside of an image.
        """
        return get_engine().run_sql(
            select('tables', 'table_name', 'namespace = %s AND repository = %s AND image_hash = %s'),
            (self.repository.namespace, self.repository.repository, self.image_hash),
            return_shape=ResultShape.MANY_ONE)

    def get_table(self, table_name):
        """
        Returns a Table object representing a version of a given table.
        Contains a list of objects that the table is linked to: a DIFF object (beginning a chain of DIFFs that
        describe a table), a SNAP object (a full table copy), or both.

        :param table_name: Name of the table
        :return: Table object or None
        """
        objects = get_engine().run_sql(SQL("""SELECT {0}.tables.object_id, format FROM {0}.tables JOIN {0}.objects
                                              ON {0}.objects.object_id = {0}.tables.object_id
                                              WHERE {0}.tables.namespace = %s AND repository = %s AND image_hash = %s
                                              AND table_name = %s""")
                                       .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                                       (self.repository.namespace, self.repository.repository,
                                        self.image_hash, table_name))
        if not objects:
            return None
        return Table(self.repository, self, table_name, objects)

    def tag(self, tag, force=False):
        """
        Tags a given image. All tags are unique inside of a repository.

        :param tag: Tag to set. 'latest' and 'HEAD' are reserved tags.
        :param force: Whether to remove the old tag if an image with this tag already exists.
        """
        set_tag(self.repository, self.image_hash, tag, force)

    def get_tags(self):
        return [t for h, t in self.repository.get_all_hashes_tags() if h == self.image_hash]

    def delete_tag(self, tag):
        """
        Deletes a tag from an image.

        :param tag: Tag to delete.
        """

        # Does checks to make sure the tag actually exists, will raise otherwise
        self.repository.get_tagged_id(tag)

        get_engine().run_sql(SQL("DELETE FROM {}.tags WHERE namespace = %s AND repository = %s AND tag = %s")
                             .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                             (self.repository.namespace, self.repository.repository, tag),
                             return_shape=None)

    def get_log(self):
        """Repeatedly gets the parent of a given image until it reaches the bottom."""
        result = [self.image_hash]
        image = self
        while image.parent_id:
            result.append(image.parent_id)
            image = self.repository.get_image(image.parent_id)
        return result

    def to_splitfile(self, err_on_end=True, source_replacement=None):
        """
        Crawls the image's parent chain to recreates an splitfile that can be used to reconstruct it.

        :param err_on_end: If False, when an image with no provenance is reached and it still has a parent, then instead of
            raising an exception, it will base the splitfile (using the FROM command) on that image.
        :param source_replacement: A dictionary of repositories and image hashes/tags specifying how to replace the
            dependencies of this splitfile (table imports and FROM commands).
        :return: A list of splitfile commands that can be fed back into the executor.
        """

        if source_replacement is None:
            source_replacement = {}
        splitfile_commands = []
        image = self
        while True:
            image_hash, parent, prov_type, prov_data = image.image_hash, image.parent_id, \
                                                       image.provenance_type, image.provenance_data
            if prov_type in ('IMPORT', 'SQL', 'FROM'):
                splitfile_commands.append(
                    _prov_command_to_splitfile(prov_type, prov_data, image_hash, source_replacement))
                if prov_type == 'FROM':
                    break
            elif prov_type in (None, 'MOUNT') and parent:
                if err_on_end:
                    raise SplitGraphException("Image %s is linked to its parent with provenance %s"
                                              " that can't be reproduced!" % (image_hash, prov_type))
                splitfile_commands.append("FROM %s:%s" % (image.repository, image_hash))
                break
            if not parent:
                break
            else:
                image = self.repository.get_image(parent)
        return list(reversed(splitfile_commands))

    # todo image deletion -- here or in the Repository?

    def get_table_schema(self, table):
        """
        Gets the schema of a given table

        :param table: Table name
        :return: The table schema. See the documentation for `get_full_table_schema` for the spec.
        """
        snap_1 = get_image_object_path(self.repository, table, self.image_hash)[0]
        return get_engine().get_full_table_schema(SPLITGRAPH_META_SCHEMA, snap_1)

    def provenance(self):
        """
        Inspects the image's parent chain to come up with a set of repositories and their hashes
        that it was created from.

        :return: List of (repository, image_hash)
        """
        from splitgraph.core.repository import Repository
        result = set()
        image = self
        while True:
            parent, prov_type, prov_data = image.parent_id, image.provenance_type, image.provenance_data
            if prov_type == 'IMPORT':
                result.add((Repository(prov_data['source_namespace'], prov_data['source']), prov_data['source_hash']))
            if prov_type == 'FROM':
                # If we reached "FROM", then that's the first statement in the image build process (as it bases the build
                # on a completely different base image). Otherwise, let's say we have several versions of the source
                # repo and base some Splitfile builds on each of them sequentially. In that case, the newest build will
                # have all of the previous FROM statements in it (since we clone the upstream commit history locally
                # and then add the FROM ... provenance data into it).
                result.add((Repository(prov_data['source_namespace'], prov_data['source']), image.image_hash))
                break
            if prov_type in (None, 'MOUNT'):
                logging.warning("Image %s has provenance type %s, which means it might not be rederiveable.",
                                image.image_hash[:12], prov_type)
            if parent is None:
                break
            image = self.repository.get_image(parent)
        return list(result)


def _prov_command_to_splitfile(prov_type, prov_data, image_hash, source_replacement):
    """
    Converts the image's provenance data stored by the Splitfile executor back to a Splitfile used to
    reconstruct it.

    :param prov_type: Provenance type (one of 'IMPORT' or 'SQL'). Any other provenances can't be reconstructed.
    :param prov_data: Provenance data as stored in the database.
    :param image_hash: Hash of the image
    :param source_replacement: Replace repository imports with different versions
    :return: String with the Splitfile command.
    """
    from splitgraph.core.repository import Repository

    if prov_type == "IMPORT":
        repo, image = Repository(prov_data['source_namespace'], prov_data['source']), prov_data['source_hash']
        result = "FROM %s:%s IMPORT " % (str(repo), source_replacement.get(repo, image))
        result += ", ".join("%s AS %s" % (tn if not q else "{" + tn.replace("}", "\\}") + "}", ta) for tn, ta, q
                            in zip(prov_data['tables'], prov_data['table_aliases'], prov_data['table_queries']))
        return result
    if prov_type == "FROM":
        repo = Repository(prov_data['source_namespace'], prov_data['source'])
        return "FROM %s:%s" % (str(repo), source_replacement.get(repo, image_hash))
    if prov_type == "SQL":
        return "SQL " + prov_data.replace("\n", "\\\n")
    raise SplitGraphException("Cannot reconstruct provenance %s!" % prov_type)
