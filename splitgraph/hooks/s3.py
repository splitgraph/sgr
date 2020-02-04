"""
Plugin for uploading Splitgraph objects from the cache to an external S3-like object store
"""
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple

from psycopg2 import DatabaseError
from psycopg2.errors import DuplicateTable
from tqdm import tqdm

from splitgraph.config import CONFIG, get_singleton
from splitgraph.engine import get_engine, ResultShape
from splitgraph.engine.postgres.engine import PostgresEngine
from splitgraph.exceptions import IncompleteObjectUploadError, IncompleteObjectDownloadError
from splitgraph.hooks.external_objects import ExternalObjectHandler


# Downloading/uploading objects to/from S3.
# In the beginning, let's say that we just mount all objects as soon as they are downloaded -- otherwise
# we introduce another distinction between objects that are mounted in splitgraph_meta and objects that
# just exist on a hard drive somewhere.


class S3ExternalObjectHandler(ExternalObjectHandler):
    """Uploads/downloads the objects to/from S3/S3-compatible host using the Minio client.

        The handler is "attached" to a given registry which manages issuing pre-signed
        GET/PUT URLs.

        The handler supports a parameter `threads` specifying the number of threads
        used to upload the objects.
    """

    def upload_objects(
        self, objects: List[str], remote_engine: PostgresEngine
    ) -> List[Tuple[str, str]]:
        """
        Upload objects to Minio

        :param remote_engine: Remote Engine class
        :param objects: List of object IDs to upload
        :return: List of tuples with successfully uploaded objects and their URLs.
        """
        worker_threads = self.params.get(
            "threads", int(get_singleton(CONFIG, "SG_ENGINE_POOL")) - 1
        )

        # Determine upload URLs
        logging.info("Getting upload URLs from the registry...")
        urls = remote_engine.run_sql(
            "SELECT splitgraph_api.get_object_upload_urls(%s, %s)",
            ("default", objects),
            return_shape=ResultShape.ONE_ONE,
        )

        local_engine = get_engine()

        def _do_upload(object_url):
            object_id, url = object_url
            # We get 3 URLs here (one for each of object itself, footer and schema -- emit
            # just the first one for logging)
            logging.debug("%s -> %s", object_id, url[0])
            try:
                local_engine.run_sql(
                    "SELECT splitgraph_api.upload_object(%s, %s)", (object_id, url)
                )
                local_engine.commit()
                local_engine.close()
                return object_id
            except DatabaseError:
                logging.exception("Error uploading object %s", object_id)
                return None

        successful: List[str] = []
        try:
            with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
                pbar = tqdm(
                    tpe.map(_do_upload, zip(objects, urls)),
                    total=len(objects),
                    unit="objs",
                    ascii=True,
                )
                for object_id in pbar:
                    if object_id:
                        successful.append(object_id)
                        pbar.set_postfix(object=object_id[:10] + "...")
            if len(successful) < len(objects):
                raise IncompleteObjectUploadError(
                    reason=None, successful_objects=successful, successful_object_urls=successful,
                )
            # The "URL" in this case is the same object ID: we ask the registry
            # for the actual URL by giving it the object ID.
            return [(s, s) for s in successful]
        except KeyboardInterrupt as e:
            raise IncompleteObjectUploadError(
                reason=e, successful_objects=successful, successful_object_urls=successful,
            )

    def download_objects(
        self, objects: List[Tuple[str, str]], remote_engine: PostgresEngine
    ) -> List[str]:
        """
        Download objects from Minio.

        :param objects: List of (object ID, object URL (object ID it's stored under))
        """
        # By default, take up the whole connection pool with downloaders
        # (less one connection for the main thread that handles metadata)
        worker_threads = self.params.get(
            "threads", int(get_singleton(CONFIG, "SG_ENGINE_POOL")) - 1
        )

        logging.info("Getting download URLs from registry %s...", remote_engine)
        object_ids = [o[0] for o in objects]
        remote_object_ids = [o[1] for o in objects]
        urls = remote_engine.run_sql(
            "SELECT splitgraph_api.get_object_download_urls(%s, %s)",
            ("default", remote_object_ids),
            return_shape=ResultShape.ONE_ONE,
        )

        local_engine = get_engine()

        def _do_download(object_url):
            object_id, url = object_url
            logging.debug("%s -> %s", url[0], object_id)

            try:
                local_engine.run_sql(
                    "SELECT splitgraph_api.download_object(%s, %s)", (object_id, url)
                )
            except DatabaseError:
                logging.exception("Error downloading object %s", object_id)
                return None

            try:
                local_engine.mount_object(object_id)
            except DuplicateTable:
                logging.warning("Object %s already exists locally." % object_id)
            # Commit and release the connection (each is keyed by the thread ID)
            # back into the pool.
            # NB this should only be done when the loader is running in a different thread, since
            # otherwise this will also commit the transaction that's opened by the ObjectManager, messing
            # with its refcounting.
            local_engine.commit()
            local_engine.close()

            return object_id

        successful: List[str] = []

        try:
            with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
                # Evaluate the results so that exceptions thrown by the downloader get raised
                pbar = tqdm(
                    tpe.map(_do_download, zip(object_ids, urls)),
                    total=len(objects),
                    unit="objs",
                    ascii=True,
                )
                for object_id in pbar:
                    if object_id:
                        successful.append(object_id)
                        pbar.set_postfix(object=object_id[:10] + "...")
            if len(successful) < len(object_ids):
                raise IncompleteObjectDownloadError(reason=None, successful_objects=successful)
            return successful
        except KeyboardInterrupt as e:
            raise IncompleteObjectDownloadError(reason=e, successful_objects=successful)
