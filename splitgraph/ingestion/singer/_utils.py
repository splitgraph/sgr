import logging
import traceback
from functools import wraps

from psycopg2.sql import SQL, Identifier

from splitgraph.core.types import TableSchema, Changeset
from splitgraph.engine import validate_type
from splitgraph.engine.postgres.engine import get_change_key, PostgresEngine


def log_exception(f):
    """Emit exceptions with full traceback instead of just the error text"""

    @wraps(f)
    def wrapped(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception:
            logging.error(traceback.format_exc())
            raise

    return wrapped


def _migrate_schema(engine, table_schema, table_name, table_schema_spec, new_schema_spec):
    """Migrate the schema of a table to match the schema_spec"""

    old_cols = {c.name: c.pg_type for c in table_schema_spec}
    new_cols = {c.name: c.pg_type for c in new_schema_spec}
    for c in old_cols:
        if c not in new_cols:
            engine.run_sql(
                SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(
                    Identifier(table_schema), Identifier(table_name), Identifier(c),
                )
            )
    for c in new_cols:
        if c not in old_cols:
            engine.run_sql(
                SQL("ALTER TABLE {}.{} ADD COLUMN {} %s" % validate_type(new_cols[c])).format(
                    Identifier(table_schema), Identifier(table_name), Identifier(c),
                )
            )
        elif new_cols[c] != old_cols[c]:
            engine.run_sql(
                SQL(
                    "ALTER TABLE {}.{} ALTER COLUMN {} TYPE %s" % validate_type(new_cols[c])
                ).format(
                    Identifier(table_schema), Identifier(table_name), Identifier(c),
                )
            )


def _make_changeset(
    engine: PostgresEngine,
    old_schema: str,
    old_table: str,
    schema: str,
    table: str,
    schema_spec: TableSchema,
    upsert_condition: str = "TRUE",
) -> Changeset:
    """Build a fake changeset from the temporary table and the existing table to pass
    to the object manager (store as a Splitgraph diff)."""

    # PK -> (upserted / deleted, old row, new row)
    # As a memory-saving hack, we only record the values of the old row (read from the
    # current table) -- this is because object creation routines read the inserted rows
    # from the staging table anyway.
    change_key = [c for c, _ in get_change_key(schema_spec)]
    # Query:
    # SELECT (new, pk, columns) AS pk,
    #        (custom upsert condition),
    #        (row_to_json(old non-pk cols)) AS old_row
    # FROM new_table n LEFT OUTER JOIN old_table o ON [o.pk = n.pk]
    # WHERE old row != new row
    query = (
        SQL("SELECT ")
        + SQL(",").join(SQL("n.") + Identifier(c) for c in change_key)
        + SQL(",")
        + SQL(upsert_condition + " AS upserted, ")
        # If PK doesn't exist in the new table, old_row is null, else output it
        + SQL("CASE WHEN ")
        + SQL(" AND ").join(SQL("o.{0} IS NULL").format(Identifier(c)) for c in change_key)
        + SQL(" THEN '{}'::json ELSE json_build_object(")
        + SQL(",").join(
            SQL("%s, o.") + Identifier(c.name) for c in schema_spec if c.name not in change_key
        )
        + SQL(") END AS old_row FROM {}.{} n LEFT OUTER JOIN {}.{} o ON ").format(
            Identifier(schema), Identifier(table), Identifier(old_schema), Identifier(old_table),
        )
        + SQL(" AND ").join(SQL("o.{0} = n.{0}").format(Identifier(c)) for c in change_key)
        + SQL("WHERE o.* IS DISTINCT FROM n.*")
    ).as_string(engine.connection)
    args = [c.name for c in schema_spec if c.name not in change_key]
    result = engine.run_sql(query, args)
    return {tuple(row[:-2]): (row[-2], row[-1], {}) for row in result}


def prepare_new_image(repository, hash_or_tag):
    from splitgraph.core.engine import repository_exists

    from random import getrandbits
    from typing import Optional
    from splitgraph.core.image import Image

    new_image_hash = "{:064x}".format(getrandbits(256))
    if repository_exists(repository):
        # Clone the base image and delta compress against it
        base_image: Optional[Image] = repository.images[hash_or_tag]
        repository.images.add(parent_id=None, image=new_image_hash, comment="Singer tap ingestion")
        repository.engine.run_sql(
            "INSERT INTO splitgraph_meta.tables "
            "(SELECT namespace, repository, %s, table_name, table_schema, object_ids "
            "FROM splitgraph_meta.tables "
            "WHERE namespace = %s AND repository = %s AND image_hash = %s)",
            (new_image_hash, repository.namespace, repository.repository, base_image.image_hash,),
        )
    else:
        base_image = None
        repository.images.add(parent_id=None, image=new_image_hash, comment="Singer tap ingestion")
    return base_image, new_image_hash
