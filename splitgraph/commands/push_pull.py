"""
Public API for pushing/pulling repositories to/from remote Splitgraph instances.
"""

import logging

from psycopg2.sql import SQL, Identifier

from splitgraph._data.common import ensure_metadata_schema
from splitgraph._data.images import get_all_image_info, add_new_image
from splitgraph._data.objects import register_objects, register_tables, register_object_locations, \
    get_existing_objects, get_external_object_locations, get_object_meta
from splitgraph.commands._common import set_head, manage_audit
from splitgraph.commands.repository import lookup_repo
from splitgraph.commands.tagging import get_all_hashes_tags, set_tags
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.core._objects.loading import download_objects, upload_objects
from splitgraph.engine import get_engine, switch_engine
from splitgraph.exceptions import SplitGraphException


def _get_required_snaps_objects(remote_engine_name, local_repository, remote_repository):
    """
    Inspects the remote Splitgraph engine and gathers metadata missing on the local one required
    for a pull, registering new images locally. Internal function.

    :param remote_engine_name: Name of the remote Splitgraph engine
    :param local_repository: Local Repository object
    :param remote_repository: Remote Repository object
    """
    local_images = {image_hash: parent_id for image_hash, parent_id, _, _, _, _ in
                    get_all_image_info(local_repository)}
    with switch_engine(remote_engine_name):
        remote_images = {image_hash: (parent_id, created, comment, prov_type, prov_data)
                         for image_hash, parent_id, created, comment, prov_type, prov_data in
                         get_all_image_info(remote_repository)}

    # We assume here that none of the remote image hashes have changed (are immutable) since otherwise the remote
    # would have created a new image.
    table_meta = []
    new_images = [i for i in remote_images if i not in local_images]
    remote_engine = get_engine(remote_engine_name)
    for image_hash in new_images:
        # This is not batched but there shouldn't be that many entries here anyway.
        remote_parent, remote_created, remote_comment, remote_prov, remote_provdata = remote_images[image_hash]
        add_new_image(local_repository, remote_parent, image_hash, remote_created, remote_comment, remote_prov,
                      remote_provdata)
        # Get the meta for all objects we'll need to fetch.
        table_meta.extend(remote_engine.run_sql(SQL("""SELECT image_hash, table_name, object_id FROM {0}.tables
                       WHERE namespace = %s AND repository = %s AND image_hash = %s""")
                                                .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                                                (remote_repository.namespace, remote_repository.repository,
                                                 image_hash)))

    # Get the tags too
    existing_tags = [t for s, t in get_all_hashes_tags(local_repository)]
    with switch_engine(remote_engine_name):
        tags = {t: s for s, t in get_all_hashes_tags(remote_repository) if t not in existing_tags}

    # Crawl the object tree to get the IDs and other metadata for all required objects.
    distinct_objects, object_meta = _extract_recursive_object_meta(remote_engine_name, table_meta)

    with switch_engine(remote_engine_name):
        object_locations = get_external_object_locations(list(distinct_objects)) if distinct_objects else []

    return new_images, table_meta, object_locations, object_meta, tags


def _extract_recursive_object_meta(remote_engine_name, table_meta):
    """Since an object can now depend on another object that's not mentioned in the commit tree,
    we now have to follow the objects' links to their parents until we have gathered all the required object IDs."""
    existing_objects = get_existing_objects()
    distinct_objects = set(o[2] for o in table_meta if o[2] not in existing_objects)
    known_objects = set()
    object_meta = []

    with switch_engine(remote_engine_name):
        while True:
            new_parents = [o for o in distinct_objects if o not in known_objects]
            if not new_parents:
                break
            else:
                parents_meta = get_object_meta(new_parents)
                distinct_objects.update(
                    set(o[2] for o in parents_meta if o[2] is not None and o[2] not in existing_objects))
                object_meta.extend(parents_meta)
                known_objects.update(new_parents)
    return distinct_objects, object_meta


def pull(repository, download_all=False):
    """
    Synchronizes the state of the local Splitgraph repository with its upstream, optionally downloading all new
    objects created on the remote.

    :param repository: Repository to pull changes from.
    :param download_all: If True, downloads all objects and stores them locally. Otherwise, will only download required
        objects when a table is checked out.
    """
    remote_info = repository.get_upstream()
    if not remote_info:
        raise SplitGraphException("No upstream found for repository %s!" % repository.to_schema())

    remote_engine, remote_repository = remote_info
    clone(remote_repository=remote_repository, remote_engine=remote_engine, local_repository=repository,
          download_all=download_all)


@manage_audit
def clone(remote_repository, remote_engine=None, local_repository=None, download_all=False):
    """
    Clones a remote Splitgraph repository or synchronizes remote changes with the local ones.

    If the repository has no set upstream engine, the engine becomes its upstream. If `remote_engine`
    instead is a connection string, the repository won't have an upstream, meaning that lazy object
    downloads from the remote engine won't work at checkout time.

    :param remote_repository: Repository to clone.
    :param remote_engine: Name of the remote engine. If unspecified, the current engine
        lookup list is searched for the repository.
    :param local_repository: Local repository to clone into. If None, uses the same name as the remote.
    :param download_all: If True, downloads all objects and stores them locally. Otherwise, will only download required
        objects when a table is checked out.
    """
    ensure_metadata_schema()

    local_repository = local_repository or remote_repository
    get_engine().create_schema(local_repository.to_schema())

    if not remote_engine:
        remote_engine = lookup_repo(remote_repository)

    # Get the remote log and the list of objects we need to fetch.
    logging.info("Gathering remote metadata...")

    # This also registers the new versions locally.
    snaps_to_fetch, table_meta, object_locations, object_meta, tags = _get_required_snaps_objects(remote_engine,
                                                                                                  local_repository,
                                                                                                  remote_repository)

    if not snaps_to_fetch:
        logging.info("Nothing to do.")
        return

    # Don't actually download any real objects until the user tries to check out a revision.
    if download_all:
        # Check which new objects we need to fetch/preregister.
        # We might already have some objects prefetched
        # (e.g. if a new version of the table is the same as the old version)
        logging.info("Fetching remote objects...")
        download_objects(remote_engine, objects_to_fetch=list(set(o[0] for o in object_meta)),
                         object_locations=object_locations)

    # Map the tables to the actual objects no matter whether or not we're downloading them.
    register_objects(object_meta)
    register_object_locations(object_locations)
    register_tables(local_repository, table_meta)
    set_tags(local_repository, tags, force=False)
    # Don't check anything out, keep the repo bare.
    set_head(local_repository, None)

    print("Fetched metadata for %d object(s), %d table version(s) and %d tag(s)." % (len(object_meta),
                                                                                     len(table_meta),
                                                                                     len([t for t in tags if
                                                                                          t != 'HEAD'])))

    if local_repository.get_upstream() is None and remote_engine:
        local_repository.set_upstream(remote_engine, remote_repository)


def local_clone(source, destination):
    """Clones one local repository into another, copying all of its commit history over. Doesn't do any checking out
    or materialization."""
    ensure_metadata_schema()

    _, table_meta, object_locations, object_meta, tags = _get_required_snaps_objects('LOCAL',
                                                                                     destination, source)

    # Map the tables to the actual objects no matter whether or not we're downloading them.
    register_objects(object_meta)
    register_object_locations(object_locations)
    register_tables(destination, table_meta)
    set_tags(destination, tags, force=False)
    # Don't check anything out, keep the repo bare.
    set_head(destination, None)


def push(local_repository, remote_engine=None, remote_repository=None, handler='DB', handler_options=None):
    """
    Inverse of ``pull``: Pushes all local changes to the remote and uploads new objects.

    :param local_repository: Local repository to push changes from.
    :param remote_engine: The name of the remote engine or a connection string. If not specified, the current upstream
        is used.
    :param remote_repository: Remote repository to push changes to. If not specified, the local repository is used.
    :param handler: Name of the handler to use to upload objects. Use `DB` to push them to the remote or `S3`
        to store them in an S3 bucket.
    :param handler_options: Extra options to pass to the handler. For example, see
        :class:`splitgraph.hooks.s3.S3ExternalObjectHandler`
    """

    if handler_options is None:
        handler_options = {}
    ensure_metadata_schema()

    # Maybe consider having a context manager for getting a remote engine instance
    # that auto-commits/closes when needed?

    remote_engine, remote_repository = merge_push_params(local_repository, remote_engine,
                                                         remote_repository)
    try:
        logging.info("Gathering remote metadata...")
        # Flip the two connections here: pretend the remote engine is local and download metadata from the local
        # engine instead of the remote.
        with switch_engine(remote_engine):
            # This also registers new commits remotely. Should make explicit and move down later on.
            snaps_to_push, table_meta, object_locations, object_meta, tags = \
                _get_required_snaps_objects('LOCAL', remote_repository, local_repository)

        if not snaps_to_push:
            logging.info("Nothing to do.")
            return

        new_uploads = upload_objects(remote_engine, list(set(o[0] for o in object_meta)),
                                     handler=handler, handler_params=handler_options)

        # Register the newly uploaded object locations locally and remotely.
        with switch_engine(remote_engine):
            register_objects(object_meta)
            register_object_locations(object_locations + new_uploads)
            register_tables(remote_repository, table_meta)
            set_tags(remote_repository, tags, force=False)

        register_object_locations(new_uploads)

        if local_repository.get_upstream() is None and remote_engine:
            local_repository.set_upstream(remote_engine, remote_repository)

        get_engine(remote_engine).commit()
        print("Uploaded metadata for %d object(s), %d table version(s) and %d tag(s)." % (len(object_meta),
                                                                                          len(table_meta),
                                                                                          len([t for t in tags if
                                                                                               t != 'HEAD'])))
    finally:
        get_engine(remote_engine).close()


def merge_push_params(local_repository, remote_engine, remote_repository):
    """
    Merges remote arguments for push/publish as follows:

    If remote_engine is specified, it's used to get the connection parameters.

    If remote_repository is specified, it's used as a remote repository. Otherwise, the local repository
    name is used.

    Finally, fall back to the repository's upstream.

    :param local_repository: Local Repository object
    :param remote_engine: Remote engine alias
    :param remote_repository: Remote Repository object
    :return: Connection parameters, remote engine name (can be None), remote Repository object
    """
    remote_repository = remote_repository or local_repository
    upstream = local_repository.get_upstream()
    if upstream:
        remote_engine = remote_engine or upstream[0]
        remote_repository = remote_repository or upstream[1]
    if not remote_engine:
        raise SplitGraphException("No upstream found for repository %s and no engine specified!" %
                                  local_repository.to_schema())
    return remote_engine, remote_repository
