"""S3 registry-side routines called from the Python stored procedure
that are aware of the actual S3 access creds and generate pre-signed
URLs to upload/download objects."""
from datetime import timedelta

from minio import Minio

from splitgraph.config import CONFIG

S3_HOST = CONFIG["SG_S3_HOST"]
S3_PORT = CONFIG["SG_S3_PORT"]
S3_SECRET_KEY = CONFIG["SG_S3_PWD"]
S3_BUCKET = CONFIG["SG_S3_BUCKET"]
S3_ACCESS_KEY = CONFIG["SG_S3_KEY"]

MINIO = Minio(
    "%s:%s" % (S3_HOST, S3_PORT), access_key=S3_ACCESS_KEY, secret_key=S3_SECRET_KEY, secure=False
)

_EXP = timedelta(seconds=60)


def get_object_upload_urls(object_ids):
    """
    Return a list of pre-signed URLs that each part of an object can be downloaded from.

    :param object_ids: List of object IDs
    :return: A list of lists [(object URL, object footer URL, object schema URL)]
    """
    return [
        [
            MINIO.presigned_put_object(
                bucket_name=S3_BUCKET, object_name=object_id + suffix, expires=_EXP
            )
            for suffix in ("", ".footer", ".schema")
        ]
        for object_id in object_ids
    ]


def get_object_download_urls(object_ids):
    """
    Return a list of pre-signed URLs that each part of an object can be downloaded from.

    :param object_ids: List of object IDs
    :return: A list of lists [(object URL, object footer URL, object schema URL)]
    """

    return [
        [
            MINIO.presigned_get_object(
                bucket_name=S3_BUCKET, object_name=object_id + suffix, expires=_EXP
            )
            for suffix in ("", ".footer", ".schema")
        ]
        for object_id in object_ids
    ]


def delete_objects(client, object_ids):
    """
    Delete objects stored in Minio

    :param client: Minio client
    :param object_ids: List of Splitgraph object IDs to delete
    """

    # Expand the list of objects into actual files we store in Minio
    all_object_ids = [o + suffix for o in object_ids for suffix in ("", ".schema", ".footer")]
    list(client.remove_objects(S3_BUCKET, all_object_ids))


def list_objects(client):
    """
    List objects stored in Minio

    :param client: Minio client
    :return: List of Splitgraph object IDs
    """

    return [
        o.object_name
        for o in client.list_objects(bucket_name=S3_BUCKET)
        if not o.object_name.endswith(".footer") and not o.object_name.endswith(".schema")
    ]
