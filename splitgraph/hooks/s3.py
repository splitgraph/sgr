"""
Plugin for uploading Splitgraph objects from the cache to an external S3-like object store
"""
import logging
from concurrent.futures import ThreadPoolExecutor

from psycopg2 import DatabaseError

from splitgraph.config import CONFIG
from splitgraph.engine import get_engine, ResultShape
from splitgraph.hooks.external_objects import ExternalObjectHandler


# Downloading/uploading objects to/from S3.
# In the beginning, let's say that we just mount all objects as soon as they are downloaded -- otherwise
# we introduce another distinction between objects that are mounted in splitgraph_meta and objects that
# just exist on a hard drive somewhere.


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

    def upload_objects(self, objects, remote_engine):
        """
        Upload objects to Minio

        :param remote_engine: Remote Engine class
        :param objects: List of object IDs to upload
        :return: List of object IDs on Minio
        """
        worker_threads = self.params.get("threads", int(CONFIG["SG_ENGINE_POOL"]) - 1)
        s3_host = self.params.get("host", "%s:%s" % (CONFIG["SG_S3_HOST"], CONFIG["SG_S3_PORT"]))

        # Determine upload URLs
        logging.info("Getting upload URLs from the registry...")
        urls = remote_engine.run_sql(
            "SELECT splitgraph_api.get_object_upload_urls(%s, %s)",
            (s3_host, objects),
            return_shape=ResultShape.ONE_ONE,
        )

        logging.info("Uploading %d object(s)...", len(objects))
        local_engine = get_engine()

        def _do_upload(object_url):
            object_id, url = object_url
            # We get 3 URLs here (one for each of object itself, footer and schema -- emit
            # just the first one for logging)
            logging.info("%s -> %s", object_id, url[0])
            local_engine.run_sql("SELECT splitgraph_api.upload_object(%s, %s)", (object_id, url))
            return object_id

        with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
            urls = tpe.map(_do_upload, zip(objects, urls))

        return list(urls)

    def download_objects(self, objects, remote_engine):
        """
        Download objects from Minio.

        :param objects: List of (object ID, object URL (object ID it's stored under))
        """
        # Maybe here we have to set these to None (anonymous) if the S3 host name doesn't match our own one.
        # By default, take up the whole connection pool with downloaders (less one connection for the main
        # thread that handles metadata)
        worker_threads = self.params.get("threads", int(CONFIG["SG_ENGINE_POOL"]) - 1)
        s3_host = self.params.get("host", "%s:%s" % (CONFIG["SG_S3_HOST"], CONFIG["SG_S3_PORT"]))

        logging.info("Getting download URLs from the registry...")
        object_ids = [o[0] for o in objects]
        remote_object_ids = [o[1] for o in objects]
        urls = remote_engine.run_sql(
            "SELECT splitgraph_api.get_object_download_urls(%s, %s)",
            (s3_host, remote_object_ids),
            return_shape=ResultShape.ONE_ONE,
        )
        logging.info("Downloading %d object(s)...", len(objects))

        local_engine = get_engine()

        def _do_download(object_url):
            object_id, url = object_url
            logging.info("%s -> %s", url[0], object_id)

            try:
                local_engine.run_sql(
                    "SELECT splitgraph_api.download_object(%s, %s)", (object_id, url)
                )
            except DatabaseError:
                logging.exception("Error downloading object %s", object_id)
                return
            local_engine.mount_object(object_id)
            # Commit and release the connection (each is keyed by the thread ID)
            # back into the pool.
            # NB this should only be done when the loader is running in a different thread, since
            # otherwise this will also commit the transaction that's opened by the ObjectManager, messing
            # with its refcounting.
            local_engine.commit()
            local_engine.close()

        with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
            # Evaluate the results so that exceptions thrown by the downloader get raised
            list(tpe.map(_do_download, zip(object_ids, urls)))
