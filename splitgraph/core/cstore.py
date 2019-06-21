"""Routines related to storing objects on the local engine as Citus CStore files"""
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from psycopg2.errors import DatabaseError
from psycopg2.sql import Identifier, SQL

from splitgraph.config import SPLITGRAPH_META_SCHEMA, CONFIG
from splitgraph.engine import get_engine, ResultShape
from splitgraph.hooks.s3 import (
    S3_ACCESS_KEY,
    S3_HOST,
    S3_PORT,
    S3_SECRET_KEY,
    S3ExternalObjectHandler,
)

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


# Downloading/uploading objects to/from S3.
# In the beginning, let's say that we just mount all objects as soon as they are downloaded -- otherwise
# we introduce another distinction between objects that are mounted in splitgraph_meta and objects that
# just exist on a hard drive somewhere.

# Currently, this is copied from the S3 object handler (and mangled).
class CStoreS3ExternalObjectHandler(S3ExternalObjectHandler):
    def upload_objects(self, objects):
        """
        Upload objects to Minio

        :param objects: List of object IDs to upload
        :return: List of URLs the objects were stored at.
        """
        access_key = self.params.get("access_key", S3_ACCESS_KEY)
        endpoint = "%s:%s" % (self.params.get("host", S3_HOST), self.params.get("port", S3_PORT))
        bucket = self.params.get("bucket", access_key)
        worker_threads = self.params.get("threads", int(CONFIG["SG_ENGINE_POOL"]) - 1)

        logging.info("Uploading %d object(s) to %s/%s", len(objects), endpoint, bucket)
        engine = get_engine()

        def _do_upload(object_id):
            return engine.run_sql(
                "SELECT splitgraph_upload_object(%s, %s, %s, %s)",
                (object_id, endpoint, access_key, self.params.get("secret_key", S3_SECRET_KEY)),
                return_shape=ResultShape.ONE_ONE,
            )

        with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
            urls = tpe.map(_do_upload, objects)

        return urls

    def download_objects(self, objects):
        """
        Download objects from Minio.

        :param objects: List of (object ID, object URL of form <endpoint>/<bucket>/<key>)
        """
        # Maybe here we have to set these to None (anonymous) if the S3 host name doesn't match our own one.
        access_key = self.params.get("access_key", S3_ACCESS_KEY)
        secret_key = self.params.get("secret_key", S3_SECRET_KEY)
        # By default, take up the whole connection pool with downloaders (less one connection for the main
        # thread that handles metadata)
        worker_threads = self.params.get("threads", int(CONFIG["SG_ENGINE_POOL"]) - 1)
        engine = get_engine()

        def _do_download(obj_id_url):
            object_id, object_url = obj_id_url
            logging.info("%s -> %s", object_url, object_id)

            try:
                engine.run_sql(
                    "SELECT splitgraph_download_object(%s, %s, %s, %s)",
                    (object_id, object_url, access_key, secret_key),
                )
            except DatabaseError:
                logging.exception("Error downloading object %s", object_id)
                return
            mount_object(engine, object_id)
            engine.commit()

        with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
            # Evaluate the results so that exceptions thrown by the downloader get raised
            list(tpe.map(_do_download, objects))
