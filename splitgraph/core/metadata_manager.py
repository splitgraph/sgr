"""
Classes related to managing table/image/object metadata tables.
"""
import itertools
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    cast,
)

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier
from splitgraph.config import SPLITGRAPH_API_SCHEMA, SPLITGRAPH_META_SCHEMA
from splitgraph.core.types import TableSchema
from splitgraph.engine import ResultShape
from splitgraph.engine.postgres.engine import API_MAX_VARIADIC_ARGS, chunk

from .sql import select

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository
    from splitgraph.engine.postgres.engine import PsycopgEngine

OBJECT_COLS = [
    "object_id",
    "format",
    "namespace",
    "size",
    "created",
    "insertion_hash",
    "deletion_hash",
    "index",
    "rows_inserted",
    "rows_deleted",
]


class Object(NamedTuple):
    """Represents a Splitgraph object that tables are composed of."""

    object_id: str
    format: str
    namespace: str
    size: int
    created: datetime
    insertion_hash: str
    deletion_hash: str
    object_index: Dict[str, Any]  # Clashes with NamedTuple's "index"
    rows_inserted: int
    rows_deleted: int


class MetadataManager:
    """
    A data access layer for the metadata tables in the splitgraph_meta schema that concerns itself
    with image, table and object information.
    """

    def __init__(self, metadata_engine: "PsycopgEngine") -> None:
        self.metadata_engine = metadata_engine

    def register_objects(self, objects: List[Object], namespace: Optional[str] = None) -> None:
        """
        Registers multiple Splitgraph objects in the tree.

        :param objects: List of `Object` objects.
        :param namespace: If specified, overrides the original object namespace, required
            in the case where the remote repository has a different namespace than the local one.
        """
        object_meta = [
            tuple(
                namespace
                if namespace and a == "namespace"
                else getattr(o, a if a != "index" else "object_index")
                for a in OBJECT_COLS
            )
            for o in sorted(objects, key=lambda o: o.object_id)
        ]

        self.metadata_engine.run_sql_batch(
            SQL(
                "SELECT {}.add_object(" + ",".join(itertools.repeat("%s", len(OBJECT_COLS))) + ")"
            ).format(Identifier(SPLITGRAPH_API_SCHEMA)),
            object_meta,
        )

    def register_tables(
        self, repository: "Repository", table_meta: List[Tuple[str, str, TableSchema, List[str]]]
    ) -> None:
        """
        Links tables in an image to physical objects that they are stored as.
        Objects must already be registered in the object tree.

        :param repository: Repository that the tables belong to.
        :param table_meta: A list of (image_hash, table_name, table_schema, object_ids).
        """
        table_meta = [
            (repository.namespace, repository.repository, o[0], o[1], Json(o[2]), o[3])
            for o in table_meta
        ]

        # We're about to send len(table_meta) statements to the API and if one of them
        # is larger than the max query length, this won't work. We can't use run_chunk_sql
        # either since that only works on one statement and we might have several here
        # (still want to minimize the number of roundtrips). So we chunk the parameters
        # ourselves.

        rechunked_meta = []
        for meta in table_meta:
            rest, objects = meta[:-1], meta[-1]
            if len(objects) <= API_MAX_VARIADIC_ARGS:
                rechunked_meta.append(meta)
                continue
            rechunked_meta.extend(
                [rest + (c,) for c in chunk(objects, chunk_size=API_MAX_VARIADIC_ARGS)]
            )

        self.metadata_engine.run_sql_batch(
            SQL("SELECT {}.add_table(%s,%s,%s,%s,%s,%s)").format(Identifier(SPLITGRAPH_API_SCHEMA)),
            rechunked_meta,
        )

    def overwrite_table(
        self,
        repository: "Repository",
        image_hash: str,
        table_name: str,
        table_schema: TableSchema,
        objects: List[str],
    ):
        self.metadata_engine.run_sql(
            "UPDATE splitgraph_meta.tables "
            "SET table_schema = %s, object_ids = %s "
            "WHERE namespace = %s AND repository = %s AND image_hash = %s "
            "AND table_name = %s",
            (
                Json(table_schema),
                objects,
                repository.namespace,
                repository.repository,
                image_hash,
                table_name,
            ),
        )

    def register_object_locations(self, object_locations: List[Tuple[str, str, str]]) -> None:
        """
        Registers external locations (e.g. HTTP or S3) for Splitgraph objects.
        Objects must already be registered in the object tree.

        :param object_locations: List of (object_id, location, protocol).
        """
        self.metadata_engine.run_sql_batch(
            SQL("SELECT {}.add_object_location(%s,%s,%s)").format(
                Identifier(SPLITGRAPH_API_SCHEMA)
            ),
            sorted(object_locations, key=lambda o: str(o[0])),
        )

    def get_all_objects(self) -> List[str]:
        """
        Gets all objects currently in the Splitgraph tree.

        :return: List of object IDs.
        """
        return cast(
            List[str],
            self.metadata_engine.run_sql(
                select("objects", "object_id"), return_shape=ResultShape.MANY_ONE
            ),
        )

    def get_new_objects(self, object_ids: List[str]) -> List[str]:
        """
        Get object IDs from the passed list that don't exist in the tree.

        :param object_ids: List of objects to check
        :return: List of unknown object IDs.
        """
        return cast(
            List[str],
            self.metadata_engine.run_chunked_sql(
                SQL("SELECT {}.get_new_objects(%s)").format(Identifier(SPLITGRAPH_API_SCHEMA)),
                (object_ids,),
                return_shape=ResultShape.ONE_ONE,
                chunk_position=0,
            ),
        )

    def get_external_object_locations(self, objects: List[str]) -> List[Tuple[str, str, str]]:
        """
        Gets external locations for objects.

        :param objects: List of object IDs stored externally.
        :return: List of (object_id, location, protocol).
        """
        return cast(
            List[Tuple[str, str, str]],
            self.metadata_engine.run_chunked_sql(
                select(
                    "get_object_locations",
                    "object_id, location, protocol",
                    schema=SPLITGRAPH_API_SCHEMA,
                    table_args="(%s)",
                ),
                (objects,),
                chunk_position=0,
            ),
        )

    def get_object_meta(self, objects: List[str]) -> Dict[str, Object]:
        """
        Get metadata for multiple Splitgraph objects from the tree

        :param objects: List of objects to get metadata for.
        :return: Dictionary of object_id -> Object
        """
        if not objects:
            return {}

        metadata = self.metadata_engine.run_chunked_sql(
            select(
                "get_object_meta",
                ",".join(OBJECT_COLS),
                schema=SPLITGRAPH_API_SCHEMA,
                table_args="(%s)",
            ),
            (list(objects),),
            chunk_position=0,
        )
        result = [Object(*m) for m in metadata]
        return {o.object_id: o for o in result}

    def get_objects_for_repository(
        self, repository: "Repository", image_hash: Optional[str] = None
    ) -> List[str]:
        query = SQL(
            "SELECT object_ids FROM {}.tables WHERE namespace = %s AND repository = %s"
        ).format(Identifier(SPLITGRAPH_META_SCHEMA))
        args = [repository.namespace, repository.repository]
        if image_hash:
            query += SQL(" AND image_hash = %s")
            args.append(image_hash)

        table_objects = {
            o
            for os in self.metadata_engine.run_sql(
                query,
                args,
                return_shape=ResultShape.MANY_ONE,
            )
            for o in os
        }

        return list(table_objects)

    def delete_object_meta(self, object_ids: Sequence[str]):
        """
        Delete metadata for multiple objects (external locations, indexes, hashes).
        This doesn't delete physical objects.

        :param object_ids: Object IDs to delete
        """
        for table_name in ["object_locations", "objects"]:
            self.metadata_engine.run_chunked_sql(
                SQL("DELETE FROM {}.{} WHERE object_id = ANY(%s)").format(
                    Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table_name)
                ),
                arguments=(object_ids,),
                chunk_position=0,
            )

    def get_unused_objects(self, threshold: Optional[int] = None) -> List[Tuple[str, datetime]]:
        """
        Get a list of all objects in the metadata that aren't used by any table and can be
        safely deleted.

        :param threshold: Only return objects that were created earlier than this (in minutes)
        :return: List of objects and their creation times.
        """
        candidates = self.metadata_engine.run_sql(
            SQL(  # nosec
                "SELECT object_id, created FROM {0}.objects "
                "WHERE object_id NOT IN (SELECT unnest(object_ids) "
                "FROM {0}.tables) "
                + ("AND created < now() - INTERVAL '%s minutes' " if threshold is not None else "")
                + "ORDER BY created"
            ).format(Identifier(SPLITGRAPH_META_SCHEMA)),
            (threshold,) if threshold is not None else None,
        )
        return cast(List[Tuple[str, datetime]], candidates)

    def cleanup_metadata(self) -> List[str]:
        """
        Go through the current metadata and delete all objects that aren't required
        by any table on the engine.

        :return: List of objects that have been deleted.
        """
        candidates = self.get_unused_objects()
        to_delete = [c for c, _ in candidates]

        self.delete_object_meta(to_delete)
        return to_delete
