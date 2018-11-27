import tempfile
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from minio import Minio
from minio.error import (BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)

from splitgraph.constants import S3_ACCESS_KEY, S3_SECRET_KEY, S3_PORT, S3_HOST
from splitgraph.objects.dumping import dump_object, load_object


def _ensure_bucket(client, bucket):
    # Make a bucket with the make_bucket API call.
    try:
        client.make_bucket(bucket)
    except BucketAlreadyOwnedByYou:
        pass
    except BucketAlreadyExists:
        pass


def s3_upload_objects(objects_to_push, params):
    """Uploads the objects to S3/S3-compatible host using the Minio client.

    :param objects_to_push: List of object IDs to upload
    :param params: A dictionary of parameters overriding the .sgconfig:

        * host: default SG_S3_HOST
        * port: default SG_S3_PORT
        * access_key: default SG_S3_KEY
        * bucket: default same as access_key
        * secret_key: default SG_S3_PWD

        You can also specify the number of worker threads (`threads`) used to upload the
        objects.
    """
    access_key = params.get('access_key', S3_ACCESS_KEY)
    endpoint = '%s:%s' % (params.get('host', S3_HOST), params.get('port', S3_PORT))
    bucket = params.get('bucket', access_key)
    worker_threads = params.get('threads', 4)
    client = Minio(endpoint,
                   access_key=access_key,
                   secret_key=params.get('secret_key', S3_SECRET_KEY),
                   secure=False)
    _ensure_bucket(client, bucket)

    # Psycopg connection objects are not threadsafe -- make sure two threads can't use it at the same time.
    # In the future, we should replace this with a pg connection pool instead
    pg_conn_lock = Lock()

    with tempfile.TemporaryDirectory() as tmpdir:
        def _do_upload(object_id):
            # First cut: dump the object to file and then upload it using Minio
            tmp_path = tmpdir + '/' + object_id

            with pg_conn_lock:
                with open(tmp_path, 'wb') as f:
                    dump_object(object_id, f)
            # Minio pushes can happen concurrently
            client.fput_object(bucket, object_id, tmp_path)

            return '%s/%s/%s' % (endpoint, bucket, object_id)

        with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
            urls = tpe.map(_do_upload, objects_to_push)

    return urls


def s3_download_objects(objects_to_fetch, params):
    # Maybe here we have to set these to None (anonymous) if the S3 host name doesn't match our own one.
    access_key = params.get('access_key', S3_ACCESS_KEY)
    secret_key = params.get('secret_key', S3_SECRET_KEY)
    worker_threads = params.get('threads', 4)

    pg_conn_lock = Lock()

    with tempfile.TemporaryDirectory() as tmpdir:
        def _do_download(obj_id_url):
            object_id, object_url = obj_id_url
            endpoint, bucket, remote_object = object_url.split('/')
            client = Minio(endpoint,
                           access_key=access_key,
                           secret_key=secret_key,
                           secure=False)

            local_path = tmpdir + '/' + remote_object
            client.fget_object(bucket, remote_object, local_path)

            with pg_conn_lock:
                with open(local_path, 'rb') as f:
                    load_object(object_id, f)

        # Again, first download the objects and then import them (maybe can do streaming later)
        with ThreadPoolExecutor(max_workers=worker_threads) as tpe:
            tpe.map(_do_download, objects_to_fetch)
