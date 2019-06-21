"""Routines related to storing objects on the local engine as Citus CStore files"""
import json
import os

from psycopg2.sql import Identifier, SQL

from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.engine import ResultShape

CSTORE_SERVER = "cstore_server"


def get_object_schema(engine, object_name):
    return [
        tuple(t)
        for t in json.loads(
            engine.run_sql(
                "SELECT splitgraph_get_object_schema(%s)",
                (object_name,),
                return_shape=ResultShape.ONE_ONE,
            )
        )
    ]


def set_object_schema(engine, object_name, schema_spec):
    engine.run_sql(
        "SELECT splitgraph_set_object_schema(%s, %s)", (object_name, json.dumps(schema_spec))
    )


def mount_object(engine, object_name, schema=SPLITGRAPH_META_SCHEMA, table=None, schema_spec=None):
    table = table or object_name

    if not schema_spec:
        schema_spec = get_object_schema(engine, object_name)

    query = SQL("CREATE FOREIGN TABLE {}.{} (").format(Identifier(schema), Identifier(table))
    query += SQL(",".join("{} %s " % ctype for _, _, ctype, _ in schema_spec)).format(
        *(Identifier(cname) for _, cname, _, _ in schema_spec)
    )

    # foreign tables/cstore don't support PKs
    query += SQL(") SERVER {} OPTIONS (compression %s, filename %s)").format(
        Identifier(CSTORE_SERVER)
    )
    engine.run_sql(
        query, ("pglz", os.path.join(engine.conn_params["SG_ENGINE_OBJECT_PATH"], object_name))
    )


def store_object(engine, source_schema, source_table, object_name):
    """
    Converts a Postgres table with a Splitgraph object into a Citus FDW table, mounting it via FDW.
    At the end of this operation, the staging Postgres table is deleted.

    :param engine: Engine to store the object in
    :param source_schema: Schema the staging table is located.
    :param source_table: Name of the staging table
    :param object_name: Name of the object
    """
    schema_spec = engine.get_full_table_schema(source_schema, source_table)

    # Mount the object first
    mount_object(engine, object_name, schema_spec=schema_spec)

    # Insert the data into the new Citus table.
    engine.run_sql(
        SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
            Identifier(SPLITGRAPH_META_SCHEMA),
            Identifier(object_name),
            Identifier(source_schema),
            Identifier(source_table),
        )
    )

    # Also store the table schema in a file
    set_object_schema(engine, object_name, schema_spec)
    engine.delete_table(source_schema, source_table)


def delete_objects(engine, object_ids):
    unmount_query = SQL(";").join(
        SQL("DROP FOREIGN TABLE {}.{}").format(
            Identifier(SPLITGRAPH_META_SCHEMA), Identifier(object_id)
        )
        for object_id in object_ids
    )
    engine.run_sql(unmount_query)
    engine.run_sql_batch("SELECT splitgraph_delete_object_files(%s)", [(o,) for o in object_ids])
