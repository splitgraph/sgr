import tempfile

from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)
from splitgraph.constants import S3_ACCESS_KEY, S3_SECRET_KEY, S3_PORT, S3_HOST
from splitgraph.objects.dumping import dump_object_to_file, load_object_from_file


def _ensure_bucket(client, bucket):
    # Make a bucket with the make_bucket API call.
    try:
        client.make_bucket(bucket)
    except BucketAlreadyOwnedByYou:
        pass
    except BucketAlreadyExists:
        pass


def _s3_upload_objects(conn, objects_to_push, params):
    """Uploads the objects to S3/S3-compatible host using the Minio client."""
    access_key = params.get('access_key', S3_ACCESS_KEY)
    endpoint = '%s:%s' % (params.get('host', S3_HOST), params.get('port', S3_PORT))
    bucket = params.get('bucket', access_key)
    client = Minio(endpoint,
                   access_key=access_key,
                   secret_key=params.get('secret_key', S3_SECRET_KEY),
                   secure=False)
    _ensure_bucket(client, bucket)

    urls = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for object_id in objects_to_push:
            # First cut: dump the object to file and then upload it using Minio
            tmp_path = tmpdir + '/' + object_id
            dump_object_to_file(object_id, tmp_path)
            client.fput_object(bucket, object_id, tmp_path)

            urls.append('%s/%s/%s' % (endpoint, bucket, object_id))
    return urls


def _s3_download_objects(conn, objects_to_fetch, params):
    # Maybe here we have to set these to None (anonymous) if the S3 host name doesn't match our own one.
    access_key = params.get('access_key', S3_ACCESS_KEY)
    secret_key = params.get('secret_key', S3_SECRET_KEY)

    with tempfile.TemporaryDirectory() as tmpdir:
        # Again, first download the objects and then import them (maybe can do streaming later)
        for i, obj in enumerate(objects_to_fetch):
            object_id, object_url = obj
            print("(%d/%d) %s -> %s" % (i + 1, len(objects_to_fetch), object_url, object_id))
            endpoint, bucket, remote_object = object_url.split('/')
            client = Minio(endpoint,
                           access_key=access_key,
                           secret_key=secret_key,
                           secure=False)

            local_path = tmpdir + '/' + remote_object
            client.fget_object(bucket, remote_object, local_path)
            load_object_from_file(local_path)
