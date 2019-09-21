"""
Classes related to managing table/image/object metadata tables.
"""
import itertools
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING, NamedTuple, cast

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier

from splitgraph.config import SPLITGRAPH_API_SCHEMA, SPLITGRAPH_META_SCHEMA
from splitgraph.core._common import TableSchema
from ._common import select, ResultShape

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository
    from splitgraph.engine.postgres.engine import PostgresEngine


OBJECT_COLS = [
    "object_id",
    "format",
    "namespace",
    "size",
    "insertion_hash",
    "deletion_hash",
    "index",
]


class Object(NamedTuple):
    """Represents a Splitgraph object that tables are composed of."""

    object_id: str
    format: str
    namespace: str
    size: int
    insertion_hash: str
    deletion_hash: str
    object_index: Dict[str, Any]  # Clashes with NamedTuple's "index"


class MetadataManager:
    """
    A data access layer for the metadata tables in the splitgraph_meta schema that concerns itself
    with image, table and object information.
    """

    def __init__(self, metadata_engine: "PostgresEngine") -> None:
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
            for o in objects
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
        self.metadata_engine.run_sql_batch(
            SQL("SELECT {}.add_table(%s,%s,%s,%s,%s,%s)").format(Identifier(SPLITGRAPH_API_SCHEMA)),
            table_meta,
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
            object_locations,
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
            self.metadata_engine.run_sql(
                SQL("SELECT {}.get_new_objects(%s)").format(Identifier(SPLITGRAPH_API_SCHEMA)),
                (object_ids,),
                return_shape=ResultShape.ONE_ONE,
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
            self.metadata_engine.run_sql(
                select(
                    "get_object_locations",
                    "object_id, location, protocol",
                    schema=SPLITGRAPH_API_SCHEMA,
                    table_args="(%s)",
                ),
                (objects,),
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

        metadata = self.metadata_engine.run_sql(
            select(
                "get_object_meta",
                ",".join(OBJECT_COLS),
                schema=SPLITGRAPH_API_SCHEMA,
                table_args="(%s)",
            ),
            (list(objects),),
        )
        result = [Object(*m) for m in metadata]
        return {o.object_id: o for o in result}

    def cleanup_metadata(self) -> Set[str]:
        """
        Go through the current metadata and delete all objects that aren't required
        by any table on the engine.

        :return: List of objects that are still required.
        """
        # First, get a list of all objects required by a table.
        table_objects = {
            o
            for os in self.metadata_engine.run_sql(
                SQL("SELECT object_ids FROM {}.tables").format(Identifier(SPLITGRAPH_META_SCHEMA)),
                return_shape=ResultShape.MANY_ONE,
            )
            for o in os
        }
        # Go through the tables that aren't repository-dependent and delete entries there.
        for table_name in ["object_locations", "objects"]:
            query = SQL("DELETE FROM {}.{}").format(
                Identifier(SPLITGRAPH_META_SCHEMA), Identifier(table_name)
            )
            if table_objects:
                query += SQL(
                    " WHERE object_id NOT IN ("
                    + ",".join("%s" for _ in range(len(table_objects)))
                    + ")"
                )
            self.metadata_engine.run_sql(query, list(table_objects))
        return table_objects
