"""
Plugin for uploading Splitgraph objects from the cache to an external S3-like object store
"""
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from minio import Minio
from minio.error import (BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)
from splitgraph.config import SPLITGRAPH_META_SCHEMA, CONFIG
from splitgraph.engine import get_engine
from splitgraph.hooks.external_objects import ExternalObjectHandler

S3_HOST = CONFIG["SG_S3_HOST"]
S3_PORT = CONFIG["SG_S3_PORT"]
S3_ACCESS_KEY = CONFIG["SG_S3_KEY"]
S3_SECRET_KEY = CONFIG["SG_S3_PWD"]


def _ensure_bucket(client, bucket):
    # Make a bucket with the make_bucket API call.
    try:
        client.make_bucket(bucket)
    except BucketAlreadyOwnedByYou:
        pass
    except BucketAlreadyExists:
        pass


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
        access_key = self.params.get('access_key', S3_ACCESS_KEY)
        endpoint = '%s:%s' % (self.params.get('host', S3_HOST), self.params.get('port', S3_PORT))
        bucket = self.params.get('bucket', access_key)
        worker_threads = self.params.get('threads', 4)

        logging.info("Uploading %d object(s) to %s/%s" % (len(objects), endpoint, bucket))
        client = Minio(endpoint,
                       access_key=access_key,
                       secret_key=self.params.get('secret_key', S3_SECRET_KEY),
                       secure=False)
        _ensure_bucket(client, bucket)

        # Psycopg connection objects are not threadsafe -- make sure two threads can't use it at the same time.
        # In the future, we should replace this with a pg connection pool instead
        pg_conn_lock = Lock()

        with tempfile.TemporaryDirectory() as tmpdir:
            def _do_upload(object_id):
                # First cut: dump the object to file and then upload it using Minio
                # We can't seem to currently be able to pipe the object directly, since the S3 API
                # required content-length to be sent before the actual object gets uploaded
                # and we don't know its size in advance.
                tmp_path = tmpdir + '/' + object_id

                with pg_conn_lock:
                    with open(tmp_path, 'wb') as f:
                        get_engine().dump_object(SPLITGRAPH_META_SCHEMA, object_id, f)
                # Minio pushes can happen concurrently, no need to hold the lock here.
                client.fput_object(bucket, object_id, tmp_path)

                return '%s/%s/%s' % (endpoint, bucket, object_id)

            with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
                urls = tpe.map(_do_upload, objects)

        return urls

    def download_objects(self, objects):
        """
        Download objects from Minio.

        :param objects: List of (object ID, object URL of form <endpoint>/<bucket>/<key>)
        """
        # Maybe here we have to set these to None (anonymous) if the S3 host name doesn't match our own one.
        access_key = self.params.get('access_key', S3_ACCESS_KEY)
        secret_key = self.params.get('secret_key', S3_SECRET_KEY)
        worker_threads = self.params.get('threads', 16)

        pg_conn_lock = Lock()

        def _do_download(obj_id_url):
            object_id, object_url = obj_id_url
            endpoint, bucket, remote_object = object_url.split('/')
            client = Minio(endpoint,
                           access_key=access_key,
                           secret_key=secret_key,
                           secure=False)

            object_response = client.get_object(bucket, remote_object)

            with pg_conn_lock:
                get_engine().load_object(SPLITGRAPH_META_SCHEMA, object_id, object_response)

        with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
            tpe.map(_do_download, objects)
