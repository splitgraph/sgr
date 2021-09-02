"""
Plugin for uploading Splitgraph objects from the cache to an external S3-like object store
"""
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, List, Tuple

from splitgraph.config import CONFIG, SG_CMD_ASCII, get_singleton
from splitgraph.engine import ResultShape, get_engine
from splitgraph.exceptions import (
    IncompleteObjectDownloadError,
    IncompleteObjectUploadError,
)
from splitgraph.hooks.external_objects import ExternalObjectHandler
from tqdm import tqdm

if TYPE_CHECKING:
    from splitgraph.engine.postgres.engine import PsycopgEngine

# Downloading/uploading objects to/from S3.
# In the beginning, let's say that we just mount all objects as soon as they are downloaded -- otherwise
# we introduce another distinction between objects that are mounted in splitgraph_meta and objects that
# just exist on a hard drive somewhere.


def get_object_upload_urls(remote_engine, objects):
    remote_engine.run_sql("SET TRANSACTION READ ONLY")
    urls = remote_engine.run_chunked_sql(
        "SELECT splitgraph_api.get_object_upload_urls(%s, %s)",
        ("default", objects),
        return_shape=ResultShape.ONE_ONE,
        chunk_position=1,
    )
    remote_engine.commit()
    remote_engine.close()
    return urls


def get_object_download_urls(remote_engine, remote_object_ids):
    remote_engine.run_sql("SET TRANSACTION READ ONLY")
    urls = remote_engine.run_chunked_sql(
        "SELECT splitgraph_api.get_object_download_urls(%s, %s)",
        ("default", remote_object_ids),
        return_shape=ResultShape.ONE_ONE,
        chunk_position=1,
    )
    remote_engine.commit()
    remote_engine.close()
    return urls


class S3ExternalObjectHandler(ExternalObjectHandler):
    """Uploads/downloads the objects to/from S3/S3-compatible host using the Minio client.

    The handler is "attached" to a given registry which manages issuing pre-signed
    GET/PUT URLs.

    The handler supports a parameter `threads` specifying the number of threads
    used to upload the objects.
    """

    def upload_objects(
        self, objects: List[str], remote_engine: "PsycopgEngine"
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
        urls = get_object_upload_urls(remote_engine, objects)

        local_engine = get_engine()

        def _do_upload(object_url):
            object_id, url = object_url
            # We get 3 URLs here (one for each of object itself, footer and schema -- emit
            # just the first one for logging)
            logging.debug("%s -> %s", object_id, url[0])
            try:
                local_engine.run_api_call("upload_object", object_id, url)
                return object_id
            except Exception:
                logging.exception("Error uploading object %s", object_id)
                return None

        successful: List[str] = []
        try:
            local_engine.autocommit = True
            with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
                pbar = tqdm(
                    tpe.map(_do_upload, zip(objects, urls)),
                    total=len(objects),
                    unit="objs",
                    ascii=SG_CMD_ASCII,
                )
                for object_id in pbar:
                    if object_id:
                        successful.append(object_id)
                        pbar.set_postfix(object=object_id[:10] + "...")
            if len(successful) < len(objects):
                raise IncompleteObjectUploadError(
                    reason=None,
                    successful_objects=successful,
                    successful_object_urls=successful,
                )
            # The "URL" in this case is the same object ID: we ask the registry
            # for the actual URL by giving it the object ID.
            return [(s, s) for s in successful]
        except KeyboardInterrupt as e:
            raise IncompleteObjectUploadError(
                reason=e,
                successful_objects=successful,
                successful_object_urls=successful,
            )
        finally:
            local_engine.autocommit = False
            local_engine.close_others()

    def download_objects(
        self, objects: List[Tuple[str, str]], remote_engine: "PsycopgEngine"
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
        urls = get_object_download_urls(remote_engine, remote_object_ids)

        local_engine = get_engine()

        def _do_download(object_url):
            object_id, url = object_url
            logging.debug("%s -> %s", url[0], object_id)

            try:
                local_engine.run_api_call("download_object", object_id, url)
                local_engine.mount_object(object_id)
            except Exception as e:
                logging.error("Error downloading object %s: %s", object_id, str(e))

                # Delete the object that we just tried to download to make sure we don't have
                # a situation where the file was downloaded but mounting failed (currently
                # we inspect the filesystem to see the list of downloaded objects).
                # TODO figure out a flow for just remounting objects whose files we already have.
                local_engine.delete_objects([object_id])
                return None

            return object_id

        successful: List[str] = []

        try:
            # Temporarily set the engine into autocommit mode. This is because a transaction
            # commit resets session state and makes the download_object engine API call
            # import all of its Python modules again (which takes about 300ms). It also
            # resets the SD and GD dictionaries so it's not possible to cache those modules
            # there either.
            local_engine.autocommit = True
            with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
                # Evaluate the results so that exceptions thrown by the downloader get raised
                pbar = tqdm(
                    tpe.map(_do_download, zip(object_ids, urls)),
                    total=len(objects),
                    unit="obj",
                    ascii=SG_CMD_ASCII,
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
        finally:
            # Flip the engine back and close all but one pool connection.
            local_engine.autocommit = False
            local_engine.close_others()
