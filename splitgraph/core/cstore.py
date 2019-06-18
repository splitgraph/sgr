"""Routines related to storing objects on the local engine as Citus CStore files"""
import logging
import os
from concurrent.futures import ThreadPoolExecutor

import urllib3
from minio import Minio
from minio.error import MinioError
from psycopg2.sql import Identifier, SQL

from splitgraph import SPLITGRAPH_META_SCHEMA, CONFIG, get_engine
from splitgraph.hooks.s3 import (
    S3_ACCESS_KEY,
    S3_HOST,
    S3_PORT,
    _ensure_bucket,
    S3_SECRET_KEY,
    S3ExternalObjectHandler,
)

CSTORE_ROOT = "/var/lib/splitgraph/objects"
CSTORE_SERVER = "cstore_server"


def mount_object(engine, object_name, schema=SPLITGRAPH_META_SCHEMA, table=None):
    table = table or object_name

    # TODO how to figure out table schema

    query = SQL(
        "CREATE FOREIGN TABLE {}.{} SERVER %s OPTIONS (compression %s, filename %s)"
    ).format(Identifier(schema), Identifier(table))
    engine.run_sql(query, (CSTORE_SERVER, "pglz", os.path.join(CSTORE_ROOT), object_name))


def store_object(engine, source_schema, source_table, object_name):
    """
    Converts a Postgres table with a Splitgraph object into a Citus FDW table, mounting it via FDW.
    At the end of this operation, the staging Postgres table is deleted.

    :param engine: Engine to store the object in
    :param source_schema: Schema the staging table is located.
    :param source_table: Name of the staging table
    :param object_name: Name of the object
    """

    # Mount the object first
    mount_object(engine, object_name)

    # Insert the data into the new Citus table.
    engine.run_sql(
        SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
            Identifier(SPLITGRAPH_META_SCHEMA),
            Identifier(object_name),
            Identifier(source_schema),
            Identifier(source_table),
        )
    )

    engine.delete_table(source_schema, source_table)


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
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=self.params.get("secret_key", S3_SECRET_KEY),
            secure=False,
        )
        _ensure_bucket(client, bucket)

        def _do_upload(object_id):
            object_path = os.path.join(CSTORE_ROOT, object_id)

            client.fput_object(bucket, object_id, object_path)
            client.fput_object(bucket, object_id + ".footer", object_path + ".footer")

            return "%s/%s/%s" % (endpoint, bucket, object_id)

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

        def _do_download(obj_id_url):
            object_id, object_url = obj_id_url
            endpoint, bucket, remote_object = object_url.split("/")
            client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
            logging.info("%s -> %s", object_url, object_id)
            object_path = os.path.join(CSTORE_ROOT, object_id)

            try:
                client.fget_object(bucket, remote_object, object_path)
                client.fget_object(bucket, remote_object + ".footer", object_path + ".footer")
            except MinioError:
                logging.exception("Error downloading object %s", object_id)
                return
            except urllib3.exceptions.RequestError:
                # Some connection errors aren't caught by MinioError
                logging.exception("URLLib error downloading object %s", object_id)
                return
            engine = get_engine()
            mount_object(engine, object_id)

        with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
            # Evaluate the results so that exceptions thrown by the downloader get raised
            list(tpe.map(_do_download, objects))
