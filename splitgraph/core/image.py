"""Image representation and provenance"""

import logging
from collections import namedtuple

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import ResultShape
from splitgraph.exceptions import SplitGraphException
from splitgraph.hooks.mount_handlers import init_fdw

from ._common import set_tag, select, manage_audit, set_head
from .table import Table

IMAGE_COLS = ["image_hash", "parent_id", "created", "comment", "provenance_type", "provenance_data"]
_PROV_QUERY = SQL("""UPDATE {}.images SET provenance_type = %s, provenance_data = %s WHERE
                            namespace = %s AND repository = %s AND image_hash = %s""") \
    .format(Identifier(SPLITGRAPH_META_SCHEMA))


class Image(namedtuple('Image', IMAGE_COLS + ['repository', 'engine'])):
    """
    Represents a Splitgraph image. Should't be created directly, use Image-loading methods in the
    :class:`splitgraph.core.repository.Repository` class instead.
    """

    def __new__(cls, *args, **kwargs):
        kwargs['engine'] = kwargs['repository'].engine
        self = super(Image, cls).__new__(cls, *args, **kwargs)
        return self

    def __eq__(self, other):
        return self.image_hash == other.image_hash and self.repository == other.repository

    def get_parent_children(self):
        """Gets the parent and a list of children of a given image."""
        parent = self.parent_id

        children = self.engine.run_sql(SQL("""SELECT image_hash FROM {}.images
                WHERE namespace = %s AND repository = %s AND parent_id = %s""")
                                       .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                                       (self.repository.namespace, self.repository.repository, self.image_hash),
                                       return_shape=ResultShape.MANY_ONE)
        return parent, children

    def get_tables(self):
        """
        Gets the names of all tables inside of an image.
        """
        return self.engine.run_sql(
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

        # TODO TF work: list of objects (fragments) here

        objects = self.engine.run_sql(SQL("""SELECT {0}.tables.object_id, format FROM {0}.tables JOIN {0}.objects
                                              ON {0}.objects.object_id = {0}.tables.object_id
                                              WHERE {0}.tables.namespace = %s AND repository = %s AND image_hash = %s
                                              AND table_name = %s""")
                                      .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                                      (self.repository.namespace, self.repository.repository,
                                       self.image_hash, table_name))
        if not objects:
            return None
        table_schema = self.engine.run_sql(SQL("SELECT table_schema FROM {}.tables WHERE namespace = %s "
                                               "AND repository = %s AND image_hash = %s AND table_name = %s")
                                           .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                                           (self.repository.namespace, self.repository.repository,
                                            self.image_hash, table_name), return_shape=ResultShape.ONE_ONE)
        return Table(self.repository, self, table_name, table_schema, objects)

    @manage_audit
    def checkout(self, force=False, layered=False):
        """
        Checks the image out, changing the current HEAD pointer. Raises an error
        if there are pending changes to its checkout.

        :param force: Discards all pending changes to the schema.
        :param layered: If True, uses layered querying to check out the image (doesn't materialize tables
            inside of it).
        """
        target_schema = self.repository.to_schema()
        if self.repository.has_pending_changes():
            if not force:
                raise SplitGraphException("{0} has pending changes! Pass force=True or do sgr checkout -f {0}:HEAD"
                                          .format(target_schema))
            logging.warning("%s has pending changes, discarding...", target_schema)
            self.engine.discard_pending_changes(target_schema)

        # Drop all current tables in staging
        self.engine.create_schema(target_schema)
        for table in self.engine.get_all_tables(target_schema):
            self.engine.delete_table(target_schema, table)

        if layered:
            self._lq_checkout()
        else:
            for table in self.get_tables():
                self.get_table(table).materialize(table)
        set_head(self.repository, self.image_hash)

    def _lq_checkout(self, target_schema=None):
        """
        Intended to be run on the sgr side. Initializes the FDW for all tables in a given image,
        allowing to query them directly without materializing the tables.
        """
        # assumes that we got to the point in the normal checkout where we're about to materialize the tables
        # (e.g. the schemata are cleared)
        # Use a per-schema "foreign server" for layered queries for now
        target_schema = target_schema or self.repository.to_schema()
        server_id = '%s_lq_checkout_server' % target_schema
        engine = self.repository.engine

        init_fdw(engine, server_id=server_id, wrapper='multicorn',
                 server_options={'wrapper': 'splitgraph.core.fdw_checkout.QueryingForeignDataWrapper',
                                 'engine': engine.name,
                                 'use_socket': 'True',
                                 'namespace': self.repository.namespace,
                                 'repository': self.repository.repository,
                                 'image_hash': self.image_hash})

        # It's easier to create the foreign tables from our side than to implement IMPORT FOREIGN SCHEMA by the FDW
        for table_name in self.get_tables():
            logging.info("Mounting %s:%s/%s into %s", self.repository.to_schema(), self.image_hash, table_name,
                         target_schema)
            self.get_table(table_name).materialize(table_name, target_schema, lq_server=server_id)

    def tag(self, tag, force=False):
        """
        Tags a given image. All tags are unique inside of a repository.

        :param tag: Tag to set. 'latest' and 'HEAD' are reserved tags.
        :param force: Whether to remove the old tag if an image with this tag already exists.
        """
        set_tag(self.repository, self.image_hash, tag, force)

    def get_tags(self):
        """Lists all tags that this image has."""
        return [t for h, t in self.repository.get_all_hashes_tags() if h == self.image_hash]

    def delete_tag(self, tag):
        """
        Deletes a tag from an image.

        :param tag: Tag to delete.
        """

        # Does checks to make sure the tag actually exists, will raise otherwise
        self.repository.images.by_tag(tag)

        self.engine.run_sql(SQL("DELETE FROM {}.tags WHERE namespace = %s AND repository = %s AND tag = %s")
                            .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                            (self.repository.namespace, self.repository.repository, tag),
                            return_shape=None)

    def get_log(self):
        """Repeatedly gets the parent of a given image until it reaches the bottom."""
        result = [self]
        while result[-1].parent_id:
            result.append(self.repository.images.by_hash(result[-1].parent_id))
        return result

    def to_splitfile(self, err_on_end=True, source_replacement=None):
        """
        Crawls the image's parent chain to recreates a Splitfile that can be used to reconstruct it.

        :param err_on_end: If False, when an image with no provenance is reached and it still has a parent, then instead
            of raising an exception, it will base the Splitfile (using the FROM command) on that image.
        :param source_replacement: A dictionary of repositories and image hashes/tags specifying how to replace the
            dependencies of this Splitfile (table imports and FROM commands).
        :return: A list of Splitfile commands that can be fed back into the executor.
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
                image = self.repository.images.by_hash(parent)
        return list(reversed(splitfile_commands))

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
                # If we reached "FROM", then that's the first statement in the image build process (as it bases the
                # build on a completely different base image). Otherwise, let's say we have several versions of the
                # source repo and base some Splitfile builds on each of them sequentially. In that case, the newest
                # build will have all of the previous FROM statements in it (since we clone the upstream commit history
                # locally and then add the FROM ... provenance data into it).
                result.add((Repository(prov_data['source_namespace'], prov_data['source']), image.image_hash))
                break
            if parent is None:
                break
            if prov_type in (None, 'MOUNT'):
                logging.warning("Image %s has provenance type %s, which means it might not be rederiveable.",
                                image.image_hash[:12], prov_type)
            image = self.repository.images.by_hash(parent)
        return list(result)

    def set_provenance(self, provenance_type, **kwargs):
        """
        Sets the image's provenance. Internal function called by the Splitfile interpreter, shouldn't
        be called directly as it changes the image after it's been created.

        :param provenance_type: One of "SQL", "MOUNT", "IMPORT" or "FROM"
        :param kwargs: Extra provenance-specific arguments
        """
        if provenance_type == "IMPORT":
            self.engine.run_sql(_PROV_QUERY,
                                ("IMPORT", Json({
                                    'source': kwargs['source_repository'].repository,
                                    'source_namespace': kwargs['source_repository'].namespace,
                                    'source_hash': kwargs['source_hash'],
                                    'tables': kwargs['tables'],
                                    'table_aliases': kwargs['table_aliases'],
                                    'table_queries': kwargs['table_queries']}),
                                 self.repository.namespace, self.repository.repository, self.image_hash))
        elif provenance_type == "SQL":
            self.engine.run_sql(_PROV_QUERY,
                                ('SQL', Json(kwargs['sql']),
                                 self.repository.namespace, self.repository.repository, self.image_hash))
        elif provenance_type == "MOUNT":
            # We don't store the details of images that come from an sgr MOUNT command
            # since those are assumed to be based on an inaccessible db.
            self.engine.run_sql(_PROV_QUERY,
                                ('MOUNT', None,
                                 self.repository.namespace, self.repository.repository, self.image_hash))
        elif provenance_type == "FROM":
            self.engine.run_sql(_PROV_QUERY,
                                ('FROM', Json({'source': kwargs['source'].repository,
                                               'source_namespace': kwargs['source'].namespace}),
                                 self.repository.namespace, self.repository.repository, self.image_hash))
        else:
            raise ValueError("Provenance type %s not supported!" % provenance_type)


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
