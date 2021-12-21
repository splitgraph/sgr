"""Image representation and provenance"""

import logging
from contextlib import contextmanager
from datetime import datetime
from random import getrandbits
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
    cast,
)

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier
from splitgraph.config import (
    CONFIG,
    FDW_CLASS,
    SPLITGRAPH_API_SCHEMA,
    SPLITGRAPH_META_SCHEMA,
    get_singleton,
)
from splitgraph.engine import ResultShape
from splitgraph.exceptions import SplitGraphError, TableNotFoundError

from .common import manage_audit, set_head, set_tag, unmount_schema
from .sql import POSTGRES_MAX_IDENTIFIER, prepare_splitfile_sql, select
from .table import Table
from .types import ProvenanceLine, TableColumn

if TYPE_CHECKING:
    from .repository import Repository

IMAGE_COLS = ["image_hash", "parent_id", "created", "comment", "provenance_data"]


class Image(NamedTuple):
    """
    Represents a Splitgraph image. Should't be created directly, use Image-loading methods in the
    :class:`splitgraph.core.repository.Repository` class instead.
    """

    image_hash: str
    parent_id: Optional[str]
    created: datetime
    comment: str
    provenance_data: List[ProvenanceLine]
    repository: "Repository"

    @property
    def engine(self):
        return self.repository.engine

    @property
    def object_engine(self):
        return self.repository.object_engine

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Image):
            return NotImplemented
        return self.image_hash == other.image_hash and self.repository == other.repository

    def get_parent_children(self) -> Tuple[Optional[str], List[str]]:
        """Gets the parent and a list of children of a given image."""
        parent = self.parent_id

        children = self.engine.run_sql(
            SQL("SELECT image_hash FROM {}.get_images(%s,%s) WHERE parent_id = %s").format(
                Identifier(SPLITGRAPH_API_SCHEMA)
            ),
            (self.repository.namespace, self.repository.repository, self.image_hash),
            return_shape=ResultShape.MANY_ONE,
        )
        return parent, children

    def get_tables(self) -> List[str]:
        """
        Gets the names of all tables inside of an image.
        """
        result = self.engine.run_sql(
            select(
                "get_tables", "table_name", table_args="(%s,%s,%s)", schema=SPLITGRAPH_API_SCHEMA
            ),
            (self.repository.namespace, self.repository.repository, self.image_hash),
            return_shape=ResultShape.MANY_ONE,
        )
        return result or []

    def get_table(self, table_name: str) -> Table:
        """
        Returns a Table object representing a version of a given table.
        Contains a list of objects that the table is linked to and the table's schema.

        :param table_name: Name of the table
        :return: Table object
        """
        result = self.engine.run_sql(
            select(
                "get_tables",
                "table_schema,object_ids",
                "table_name = %s",
                table_args="(%s,%s,%s)",
                schema=SPLITGRAPH_API_SCHEMA,
            ),
            (self.repository.namespace, self.repository.repository, self.image_hash, table_name),
            return_shape=ResultShape.ONE_MANY,
        )
        if not result:
            raise TableNotFoundError(
                "Image %s:%s does not have a table %s!"
                % (self.repository, self.image_hash, table_name)
            )
        table_schema, objects = result
        return Table(
            self.repository, self, table_name, [TableColumn(*t) for t in table_schema], objects
        )

    @manage_audit
    def checkout(self, force: bool = False, layered: bool = False) -> None:
        """
        Checks the image out, changing the current HEAD pointer. Raises an error
        if there are pending changes to its checkout.

        :param force: Discards all pending changes to the schema.
        :param layered: If True, uses layered querying to check out the image (doesn't materialize tables
            inside of it).
        """
        target_schema = self.repository.to_schema()
        if len(target_schema) > POSTGRES_MAX_IDENTIFIER:
            logging.warning(
                "The full repository name %s is longer than PostgreSQL's maximum "
                "identifier length of %d. PostgreSQL will truncate the schema name "
                "down to %d characters in all queries, which might cause clashes "
                "with other checked-out repositories.",
                target_schema,
                POSTGRES_MAX_IDENTIFIER,
                POSTGRES_MAX_IDENTIFIER,
            )

        if self.repository.has_pending_changes():
            if not force:
                raise SplitGraphError(
                    "{0} has pending changes! Pass force=True or do sgr checkout -f {0}:HEAD".format(
                        target_schema
                    )
                )
            logging.warning("%s has pending changes, discarding...", target_schema)
            self.object_engine.discard_pending_changes(target_schema)

        # Drop all current tables in staging
        self.object_engine.create_schema(target_schema)
        for table in self.object_engine.get_all_tables(target_schema):
            self.object_engine.delete_table(target_schema, table)

        if layered:
            self.lq_checkout()
        else:
            for table in self.get_tables():
                self.get_table(table).materialize(table)
        set_head(self.repository, self.image_hash)

    def lq_checkout(
        self,
        target_schema: Optional[str] = None,
        wrapper: Optional[str] = FDW_CLASS,
        only_tables: Optional[List[str]] = None,
    ) -> None:
        """
        Intended to be run on the sgr side. Initializes the FDW for all tables in a given image,
        allowing to query them directly without materializing the tables.
        """
        # assumes that we got to the point in the normal checkout where we're about to materialize the tables
        # (e.g. the schemata are cleared)
        # Use a per-schema "foreign server" for layered queries for now

        # Circular import
        from splitgraph.hooks.data_source.fdw import init_fdw

        target_schema = target_schema or self.repository.to_schema()
        server_id = "%s_lq_checkout_server" % target_schema
        engine = self.repository.engine
        object_engine = self.repository.object_engine

        init_fdw(
            object_engine,
            server_id=server_id,
            wrapper="multicorn",
            server_options={
                "wrapper": wrapper,
                "engine": engine.name,
                "object_engine": object_engine.name,
                "namespace": self.repository.namespace,
                "repository": self.repository.repository,
                "image_hash": self.image_hash,
            },
        )

        # It's easier to create the foreign tables from our side than to implement IMPORT FOREIGN SCHEMA by the FDW
        for table_name in self.get_tables():
            if only_tables and table_name not in only_tables:
                continue

            logging.debug(
                "Mounting %s:%s/%s into %s",
                self.repository.to_schema(),
                self.image_hash,
                table_name,
                target_schema,
            )
            self.get_table(table_name).materialize(table_name, target_schema, lq_server=server_id)

    @contextmanager
    def query_schema(
        self, wrapper: Optional[str] = FDW_CLASS, commit: bool = True
    ) -> Iterator[str]:
        """
        Creates a temporary schema with tables in this image mounted as foreign tables that can be accessed via
        read-only layered querying. On exit from the context manager, the schema is discarded.

        :return: The name of the schema the image is located in.
        """
        tmp_schema = str.format("o{:032x}", getrandbits(128))
        try:
            self.object_engine.create_schema(tmp_schema)
            self.lq_checkout(target_schema=tmp_schema, wrapper=wrapper)
            if commit:
                self.object_engine.commit()  # Make sure the new tables are seen by other connections

            # Inject extra query planner hints as session variables if specified.
            lq_tuning = get_singleton(CONFIG, "SG_LQ_TUNING")
            if lq_tuning:
                self.object_engine.run_sql(lq_tuning)
            yield tmp_schema
        finally:
            unmount_schema(self.object_engine, tmp_schema)

    def tag(self, tag: str) -> None:
        """
        Tags a given image. All tags are unique inside of a repository. If a tag already exists, it's removed
        from the previous image and given to the new image.

        :param tag: Tag to set. 'latest' and 'HEAD' are reserved tags.
        """
        set_tag(self.repository, self.image_hash, tag)

    def get_tags(self):
        """Lists all tags that this image has."""
        return [t for h, t in self.repository.get_all_hashes_tags() if h == self.image_hash]

    def delete_tag(self, tag: str) -> None:
        """
        Deletes a tag from an image.

        :param tag: Tag to delete.
        """

        # Does checks to make sure the tag actually exists, will raise otherwise
        self.repository.images.by_tag(tag)

        self.engine.run_sql(
            select("delete_tag", table_args="(%s,%s,%s)", schema=SPLITGRAPH_API_SCHEMA),
            (self.repository.namespace, self.repository.repository, tag),
            return_shape=None,
        )

    def get_log(self) -> List["Image"]:
        """Repeatedly gets the parent of a given image until it reaches the bottom."""
        all_images = {i.image_hash: i for i in self.repository.images()}
        result = [self]
        while result[-1].parent_id is not None:
            try:
                result.append(all_images[result[-1].parent_id])
            except KeyError:
                # If we don't have the parent's metadata, it's possible
                # that the parent hasn't been pulled -- ignore it and stop here.
                return result
        return result

    def get_size(self) -> int:
        """
        Get the physical size used by the image's objects (including those that might be
        shared with other images).

        This is calculated from the metadata, the on-disk footprint might be smaller if not all of image's
        objects have been downloaded.

        :return: Size of the image in bytes.
        """
        return cast(
            int,
            self.engine.run_sql(
                select("get_image_size", table_args="(%s,%s,%s)", schema=SPLITGRAPH_API_SCHEMA),
                (self.repository.namespace, self.repository.repository, self.image_hash),
                return_shape=ResultShape.ONE_ONE,
            )
            or 0,
        )

    def to_splitfile(
        self,
        ignore_irreproducible: bool = False,
        source_replacement: Optional[Dict["Repository", str]] = None,
    ) -> List[str]:
        """
        Recreate the Splitfile that can be used to reconstruct this image.

        :param ignore_irreproducible: If True, ignore commands from irreproducible Splitfile lines
            (like MOUNT or custom commands) and instead emit a comment (this results in an invalid Splitfile).
        :param source_replacement: A dictionary of repositories and image hashes/tags specifying how to replace the
            dependencies of this Splitfile (table imports and FROM commands).
        :return: A list of Splitfile commands that can be fed back into the executor.
        """

        return reconstruct_splitfile(
            self.provenance_data, ignore_irreproducible, source_replacement
        )

    def provenance(self, reverse=False, engine=None) -> List[Tuple["Repository", str]]:
        """
        Inspects the image's parent chain to come up with a set of repositories and their hashes
        that it was created from.

        If `reverse` is True, returns a list of images that were created _from_ this image. If
        this image is on a remote repository, `engine` can be passed in to override the engine
        used for the lookup of dependents.

        :return: List of (repository, image_hash)
        """
        from splitgraph.core.repository import Repository

        api_call = "get_image_dependents" if reverse else "get_image_dependencies"

        engine = engine or self.engine

        result = set()
        for namespace, repository, image_hash in engine.run_sql(
            select(api_call, table_args="(%s,%s,%s)", schema=SPLITGRAPH_API_SCHEMA),
            (self.repository.namespace, self.repository.repository, self.image_hash),
        ):
            result.add((Repository(namespace, repository), image_hash))
        return list(result)

    def set_provenance(self, provenance_data: List[ProvenanceLine]) -> None:
        """
        Sets the image's provenance. Internal function called by the Splitfile interpreter, shouldn't
        be called directly as it changes the image after it's been created.

        :param provenance_data: List of parsed Splitfile commands and their data.
        """
        self.engine.run_sql(
            SQL(
                """UPDATE {}.images SET provenance_data = %s WHERE
                            namespace = %s AND repository = %s AND image_hash = %s"""
            ).format(Identifier(SPLITGRAPH_META_SCHEMA)),
            (
                Json(provenance_data),
                self.repository.namespace,
                self.repository.repository,
                self.image_hash,
            ),
        )


def reconstruct_splitfile(
    provenance_data: List[ProvenanceLine],
    ignore_irreproducible: bool = False,
    source_replacement: Optional[Dict["Repository", str]] = None,
) -> List[str]:
    """
    Recreate the Splitfile that can be used to reconstruct an image.
    """

    if source_replacement is None:
        source_replacement = {}
    splitfile_commands = []
    for provenance_line in provenance_data:
        prov_type = provenance_line["type"]
        assert isinstance(prov_type, str)
        if prov_type in ("IMPORT", "SQL", "FROM"):
            splitfile_commands.append(
                _prov_command_to_splitfile(provenance_line, source_replacement)
            )
        elif prov_type in ("MOUNT", "CUSTOM"):
            if not ignore_irreproducible:
                raise SplitGraphError(
                    "Image used a Splitfile command %s" " that can't be reproduced!" % prov_type
                )
            splitfile_commands.append("# Irreproducible Splitfile command of type %s" % prov_type)
    return splitfile_commands


def _prov_command_to_splitfile(
    prov_data: ProvenanceLine,
    source_replacement: Dict["Repository", str],
) -> str:
    """
    Converts the image's provenance data stored by the Splitfile executor back to a Splitfile used to
    reconstruct it.

    :param prov_data: Provenance line for one command
    :param source_replacement: Replace repository imports with different versions
    :return: String with the Splitfile command.
    """
    from splitgraph.core.repository import Repository

    prov_type = prov_data["type"]
    assert isinstance(prov_type, str)

    if prov_type == "IMPORT":
        repo, image = (
            Repository(cast(str, prov_data["source_namespace"]), cast(str, prov_data["source"])),
            cast(str, prov_data["source_hash"]),
        )
        result = "FROM %s:%s IMPORT " % (str(repo), source_replacement.get(repo, image))
        result += ", ".join(
            "%s AS %s" % (tn if not q else "{" + tn.replace("}", "\\}") + "}", ta)
            for tn, ta, q in zip(
                cast(List[str], prov_data["tables"]),
                cast(List[str], prov_data["table_aliases"]),
                cast(List[bool], prov_data["table_queries"]),
            )
        )
        return result
    if prov_type == "FROM":
        repo = Repository(cast(str, prov_data["source_namespace"]), cast(str, prov_data["source"]))
        return "FROM %s:%s" % (str(repo), source_replacement.get(repo, prov_data["source_hash"]))
    if prov_type == "SQL":
        # Use the SQL validator/replacer to rewrite old image hashes into new hashes/tags.

        def image_mapper(repository: Repository, image_hash: str):
            new_image = (
                repository.to_schema() + ":" + source_replacement.get(repository, image_hash)
            )
            return new_image, new_image

        if source_replacement:
            _, replaced_sql = prepare_splitfile_sql(str(prov_data["sql"]), image_mapper)
        else:
            replaced_sql = str(prov_data["sql"])
        return "SQL " + "{" + replaced_sql.replace("}", "\\}") + "}"
    raise SplitGraphError("Cannot reconstruct provenance %s!" % prov_type)
