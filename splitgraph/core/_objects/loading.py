"""
Internal functions for downloading/uploading Splitgraph objects to remote engines/external locations.
"""

import logging
from collections import defaultdict

from splitgraph._data.objects import get_existing_objects, get_downloaded_objects
from splitgraph.engine import get_engine, switch_engine
from splitgraph.hooks.external_objects import get_external_object_handler


def download_objects(remote_engine, objects_to_fetch, object_locations):
    """
    Fetches the required objects from the remote and stores them locally. Does nothing for objects that already exist.

    :param remote_engine: Remote Engine object
    :param objects_to_fetch: List of object IDs to download.
    :param object_locations: List of custom object locations, encoded as tuples (object_id, object_url, protocol).
    :return: Set of object IDs that were fetched.
    """

    existing_objects = get_downloaded_objects()
    objects_to_fetch = set(o for o in objects_to_fetch if o not in existing_objects)
    if not objects_to_fetch:
        return objects_to_fetch

    # We don't actually seem to pass extra handler parameters when downloading objects since
    # we can have multiple handlers in this batch.
    external_objects = _fetch_external_objects(object_locations, objects_to_fetch, {})

    remaining_objects_to_fetch = [o for o in objects_to_fetch if o not in external_objects]
    if not remaining_objects_to_fetch:
        return objects_to_fetch

    print("Fetching remote objects...")
    get_engine().download_objects(remaining_objects_to_fetch, remote_engine)
    return objects_to_fetch


def _fetch_external_objects(object_locations, objects_to_fetch, handler_params):
    non_remote_objects = []
    non_remote_by_method = defaultdict(list)
    for object_id, object_url, protocol in object_locations:
        if object_id in objects_to_fetch:
            non_remote_by_method[protocol].append((object_id, object_url))
            non_remote_objects.append(object_id)
    if non_remote_objects:
        logging.info("Fetching external objects...")
        for method, objects in non_remote_by_method.items():
            handler = get_external_object_handler(method, handler_params)
            handler.download_objects(objects)
    return non_remote_objects


def upload_objects(remote_engine_name, objects_to_push, handler='DB', handler_params=None):
    """
    Uploads physical objects to the remote or some other external location.

    :param remote_engine_name: Name of the remote engine.
    :param objects_to_push: List of object IDs to upload.
    :param handler: Name of the handler to use to upload objects. Use `DB` to push them to the remote, `FILE`
        to store them in a directory that can be accessed from the client and `HTTP` to upload them to HTTP.
    :param handler_params: For `HTTP`, a dictionary `{"username": username, "password", password}`. For `FILE`,
        a dictionary `{"path": path}` specifying the directory where the objects shall be saved.
    :return: A list of (object_id, url, handler) that specifies all objects were uploaded (skipping objects that
        already exist on the remote).
    """
    if handler_params is None:
        handler_params = {}

    # Get objects that exist on the remote engine
    with switch_engine(remote_engine_name):
        existing_objects = get_existing_objects()

    objects_to_push = list(set(o for o in objects_to_push if o not in existing_objects))
    if not objects_to_push:
        logging.info("Nothing to upload.")
        return []
    logging.info("Uploading %d object(s)...", len(objects_to_push))

    if handler == 'DB':
        get_engine().upload_objects(objects_to_push, get_engine(remote_engine_name))
        # We assume that if the object doesn't have an explicit location, it lives on the remote.
        return []

    external_handler = get_external_object_handler(handler, handler_params)
    uploaded = external_handler.upload_objects(objects_to_push)
    return [(oid, url, handler) for oid, url in zip(objects_to_push, uploaded)]
