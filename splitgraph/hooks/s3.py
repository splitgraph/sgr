"""
Plugin for uploading Splitgraph objects from the cache to an external S3-like object store
"""
import logging
from concurrent.futures import ThreadPoolExecutor

from psycopg2 import DatabaseError

from splitgraph.config import CONFIG
from splitgraph.engine import get_engine, ResultShape
from splitgraph.hooks.external_objects import ExternalObjectHandler

S3_HOST = CONFIG["SG_S3_HOST"]
S3_PORT = CONFIG["SG_S3_PORT"]
S3_ACCESS_KEY = CONFIG["SG_S3_KEY"]
S3_SECRET_KEY = CONFIG["SG_S3_PWD"]


# Downloading/uploading objects to/from S3.
# In the beginning, let's say that we just mount all objects as soon as they are downloaded -- otherwise
# we introduce another distinction between objects that are mounted in splitgraph_meta and objects that
# just exist on a hard drive somewhere.


def delete_objects(client, object_ids):
    """
    Delete objects stored in Minio

    :param client: Minio client
    :param object_ids: List of Splitgraph object IDs to delete
    """

    # Expand the list of objects into actual files we store in Minio
    all_object_ids = [o + suffix for o in object_ids for suffix in ("", ".schema", ".footer")]
    list(client.remove_objects(S3_ACCESS_KEY, all_object_ids))


def list_objects(client):
    """
    List objects stored in Minio

    :param client: Minio client
    :return: List of Splitgraph object IDs
    """

    return [
        o.object_name
        for o in client.list_objects(bucket_name=S3_ACCESS_KEY)
        if not o.object_name.endswith(".footer") and not o.object_name.endswith(".schema")
    ]


class S3ExternalObjectHandler(ExternalObjectHandler):
    """Uploads/downloads the objects to/from S3/S3-compatible host using the Minio client.
        The parameters for this handler (overriding the .sgconfig) are:

        * host: default SG_S3_HOST
        * port: default SG_S3_PORT
        * access_key: default SG_S3_KEY
        * bucket: default same as access_key
        * secret_key: default SG_S3_PWD

        You can also specify the number of worker threads (`threads`) used to upload the
        objects.
    """

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
                "SELECT splitgraph_api.upload_object(%s, %s, %s, %s, %s)",
                (
                    object_id,
                    endpoint,
                    bucket,
                    access_key,
                    self.params.get("secret_key", S3_SECRET_KEY),
                ),
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
                    "SELECT splitgraph_api.download_object(%s, %s, %s, %s)",
                    (object_id, object_url, access_key, secret_key),
                )
            except DatabaseError:
                logging.exception("Error downloading object %s", object_id)
                return
            engine._mount_object(object_id)
            # Commit and release the connection (each is keyed by the thread ID)
            # back into the pool.
            # NB this should only be done when the loader is running in a different thread, since
            # otherwise this will also commit the transaction that's opened by the ObjectManager, messing
            # with its refcounting.
            engine.commit()
            engine.close()

        with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
            # Evaluate the results so that exceptions thrown by the downloader get raised
            list(tpe.map(_do_download, objects))
