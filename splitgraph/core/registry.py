"""
Functions for communicating with the remote Splitgraph catalog
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union, TYPE_CHECKING, NamedTuple, cast

from psycopg2.extras import Json
from psycopg2.sql import SQL, Identifier

from splitgraph.config import REGISTRY_META_SCHEMA, SPLITGRAPH_META_SCHEMA, SPLITGRAPH_API_SCHEMA
from splitgraph.core.common import select
from splitgraph.core.types import TableSchema, TableColumn
from splitgraph.engine import ResultShape

if TYPE_CHECKING:
    from splitgraph.core.repository import Repository
    from splitgraph.engine.postgres.engine import PostgresEngine


def _create_registry_schema(engine: "PostgresEngine") -> None:
    """
    Creates the registry metadata schema that contains information on published images that will appear
    in the catalog.
    """
    engine.run_sql(SQL("CREATE SCHEMA {}").format(Identifier(REGISTRY_META_SCHEMA)))
    engine.run_sql(
        SQL(
            """CREATE TABLE {}.{} (
                        namespace  VARCHAR NOT NULL,
                        repository VARCHAR NOT NULL,
                        tag        VARCHAR NOT NULL,
                        image_hash VARCHAR NOT NULL,
                        published  TIMESTAMP,
                        provenance JSON,
                        readme     VARCHAR,
                        schemata   JSON,
                        previews   JSON,
                        PRIMARY KEY (namespace, repository, tag))"""
        ).format(Identifier(REGISTRY_META_SCHEMA), Identifier("images"))
    )


def _ensure_registry_schema(engine: "PostgresEngine") -> None:
    if (
        engine.run_sql(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
            (REGISTRY_META_SCHEMA,),
            return_shape=ResultShape.ONE_ONE,
        )
        is None
    ):
        _create_registry_schema(engine)


class PublishInfo(NamedTuple):
    image_hash: str
    published: datetime
    provenance: Optional[List[Tuple[Tuple[str, str], str]]]
    readme: str
    schemata: Dict[str, TableSchema]
    previews: Optional[Dict[str, List[Tuple]]]


def publish_tag(repository: "Repository", tag: str, info: PublishInfo) -> None:
    """
    Publishes a given tag in the remote catalog. Should't be called directly.
    Use splitgraph.commands.publish instead.

    :param repository: Remote (!) Repository object
    :param tag: Tag to publish
    :param info: A structure with information about the published image.
    """
    repository.engine.run_sql(
        SQL("SELECT {}.publish_image(%s,%s,%s,%s,%s,%s,%s,%s,%s)").format(
            Identifier(SPLITGRAPH_API_SCHEMA)
        ),
        (
            repository.namespace,
            repository.repository,
            tag,
            info.image_hash,
            info.published,
            Json(info.provenance),
            info.readme,
            Json(info.schemata),
            Json(info.previews),
        ),
    )


def get_published_info(repository: "Repository", tag: str) -> Optional[PublishInfo]:
    """
    Get information on an image that's published in a catalog.

    :param repository: Repository
    :param tag: Image tag
    :return: A PublishInfo namedtuple.
    """
    result = repository.engine.run_sql(
        select(
            "get_published_image",
            "image_hash,published,provenance,readme,schemata,previews",
            table_args="(%s,%s,%s)",
            schema=SPLITGRAPH_API_SCHEMA,
        ),
        (repository.namespace, repository.repository, tag),
        return_shape=ResultShape.ONE_MANY,
    )
    if result is None:
        return None

    return PublishInfo(
        image_hash=result[0],
        published=result[1],
        provenance=result[2],
        readme=result[3],
        schemata={t: [TableColumn(*c) for c in v] for t, v in result[4].items()},
        previews=result[5],
    )


def unpublish_repository(repository: "Repository") -> None:
    """
    Deletes the repository from the remote catalog.

    :param repository: Repository to unpublish
    """
    repository.engine.run_sql(
        SQL("SELECT {}.unpublish_repository(%s,%s)").format(Identifier(SPLITGRAPH_API_SCHEMA)),
        (repository.namespace, repository.repository),
    )


def get_info_key(engine: "PostgresEngine", key: str) -> Optional[str]:
    """
    Gets a configuration key from the remote registry, used to notify the client of the registry's capabilities.

    :param engine: Engine
    :param key: Key to get
    """
    return cast(
        Optional[str],
        engine.run_sql(
            SQL("SELECT value FROM {}.info WHERE key = %s").format(
                Identifier(SPLITGRAPH_META_SCHEMA)
            ),
            (key,),
            return_shape=ResultShape.ONE_ONE,
        ),
    )


def set_info_key(engine: "PostgresEngine", key: str, value: Union[bool, str]) -> None:
    """
    Sets a configuration value on the remote registry.

    :param engine: Engine
    :param key: Key to set
    :param value: New value for the key
    """

    engine.run_sql(
        SQL(
            "INSERT INTO {0}.info (key, value) VALUES (%s, %s)"
            " ON CONFLICT (key) DO UPDATE SET value = excluded.value WHERE info.key = excluded.key"
        ).format(Identifier(SPLITGRAPH_META_SCHEMA)),
        (key, value),
    )


def setup_registry_mode(engine: "PostgresEngine") -> None:
    """
    Drops tables in splitgraph_meta that aren't pertinent to the registry + sets up access policies/RLS:

    * Normal users aren't allowed to create tables/schemata (can't do checkouts inside of a registry or
      upload SG objects directly to it)
    * Normal users can't access the splitgraph_meta schema directly: they're only supposed to be able to
      talk to it via stored procedures in splitgraph_api. Those procedures are set up with SECURITY INVOKER
      (run with those users' credentials) and what they can access is further restricted by RLS:

      * images/tables/tags meta tables: can only create/update/delete records where the namespace = user ID
      * objects/object_location tables: same. An object (piece of data) becomes owned by the user that creates
        it and still remains so even if someone else's image starts using it. Hence, the original owner can delete
        or change it (since they control the external location they've uploaded it to anyway).

    """

    if get_info_key(engine, "registry_mode") == "true":
        return

    for schema in (SPLITGRAPH_META_SCHEMA, REGISTRY_META_SCHEMA, SPLITGRAPH_API_SCHEMA):
        engine.run_sql(SQL("REVOKE CREATE ON SCHEMA {} FROM PUBLIC").format(Identifier(schema)))
        # Allow schema usage (including splitgraph_meta) but don't actually allow writing to/reading from
        # meta tables. This is so that users can still download objects that are stored directly
        # on the registry.
        engine.run_sql(SQL("GRANT USAGE ON SCHEMA {} TO PUBLIC").format(Identifier(schema)))

    # Allow everyone to read objects that have been uploaded
    engine.run_sql(
        SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO PUBLIC").format(
            Identifier(SPLITGRAPH_META_SCHEMA)
        )
    )

    # Set up RLS policies on the registry schema -- not exercised directly since all access is via the SQL API
    # but still in place for the future.
    _setup_rls_policies(engine, "images", schema=REGISTRY_META_SCHEMA)

    set_info_key(engine, "registry_mode", "true")


def _setup_rls_policies(
    engine: "PostgresEngine",
    table: str,
    schema: str = SPLITGRAPH_META_SCHEMA,
    condition: None = None,
) -> None:
    condition = condition or "({0}.{1}.namespace = current_user)"

    engine.run_sql(
        SQL("ALTER TABLE {}.{} ENABLE ROW LEVEL SECURITY").format(
            Identifier(schema), Identifier(table)
        )
    )
    for flavour in "SIUD":
        engine.run_sql(
            SQL("DROP POLICY IF EXISTS {2} ON {0}.{1}").format(
                Identifier(schema), Identifier(table), Identifier(table + "_" + flavour)
            )
        )
    engine.run_sql(
        SQL("""CREATE POLICY {2} ON {0}.{1} FOR SELECT USING (true)""").format(
            Identifier(schema), Identifier(table), Identifier(table + "_S")
        )
    )
    engine.run_sql(
        SQL("""CREATE POLICY {2} ON {0}.{1} FOR INSERT WITH CHECK """).format(
            Identifier(schema), Identifier(table), Identifier(table + "_I")
        )
        + SQL(condition).format(Identifier(schema), Identifier(table))
    )
    engine.run_sql(
        SQL("CREATE POLICY {2} ON {0}.{1} FOR UPDATE USING ").format(
            Identifier(schema), Identifier(table), Identifier(table + "_U")
        )
        + SQL(condition).format(Identifier(schema), Identifier(table))
        + SQL(" WITH CHECK ")
        + SQL(condition).format(Identifier(schema), Identifier(table))
    )
    engine.run_sql(
        SQL("CREATE POLICY {2} ON {0}.{1} FOR DELETE USING ").format(
            Identifier(schema), Identifier(table), Identifier(table + "_D")
        )
        + SQL(condition).format(Identifier(schema), Identifier(table))
    )


def toggle_registry_rls(engine: "PostgresEngine", mode: str = "ENABLE") -> None:
    """
    Switches row-level security on the registry, restricting write access to metadata tables
    to owners of relevant repositories/objects.

    :param engine: Engine
    :param mode: ENABLE, DISABLE or FORCE (enable for superusers/table owners)
    """

    if mode not in ("ENABLE", "DISABLE", "FORCE"):
        raise ValueError()

    engine.run_sql(
        SQL("ALTER TABLE {}.{} %s ROW LEVEL SECURITY" % mode).format(
            Identifier(REGISTRY_META_SCHEMA), Identifier("images")
        )
    )
