import logging

from psycopg2.sql import SQL, Identifier

from splitgraph._data.common import ensure_metadata_schema
from splitgraph._data.images import get_all_images_parents, add_new_image
from splitgraph._data.misc import get_remote_for, add_remote
from splitgraph._data.objects import register_objects, register_tables, register_object_locations, \
    get_existing_objects, get_external_object_locations, get_object_meta
from splitgraph.commands._common import set_head
from splitgraph.commands._objects.loading import download_objects, upload_objects
from splitgraph.commands.tagging import get_all_hashes_tags, set_tags
from splitgraph.config import SPLITGRAPH_META_SCHEMA
from splitgraph.config.repo_lookups import lookup_repo
from splitgraph.connection import override_driver_connection, get_connection, parse_connection_string, \
    serialize_connection_string, make_conn
from splitgraph.exceptions import SplitGraphException
from ._pg_audit import manage_audit


def _get_required_snaps_objects(remote_conn, local_repository, remote_repository):
    local_snap_parents = {image_hash: parent_id for image_hash, parent_id, _, _, _, _ in
                          get_all_images_parents(local_repository)}
    with override_driver_connection(remote_conn):
        remote_snap_parents = {image_hash: (parent_id, created, comment, prov_type, prov_data)
                               for image_hash, parent_id, created, comment, prov_type, prov_data in
                               get_all_images_parents(remote_repository)}

    # We assume here that none of the remote snapshot IDs have changed (are immutable) since otherwise the remote
    # would have created a new snapshot.
    snaps_to_fetch = [s for s in remote_snap_parents if s not in local_snap_parents]
    table_meta = []
    for image_hash in snaps_to_fetch:
        # This is not batched but there shouldn't be that many entries here anyway.
        remote_parent, remote_created, remote_comment, remote_prov, remote_provdata = remote_snap_parents[image_hash]
        add_new_image(local_repository, remote_parent, image_hash, remote_created, remote_comment, remote_prov,
                      remote_provdata)
        # Get the meta for all objects we'll need to fetch.
        with remote_conn.cursor() as cur:
            cur.execute(SQL("""SELECT image_hash, table_name, object_id FROM {0}.tables
                           WHERE namespace = %s AND repository = %s AND image_hash = %s""")
                        .format(Identifier(SPLITGRAPH_META_SCHEMA)),
                        (remote_repository.namespace, remote_repository.repository, image_hash))
            table_meta.extend(cur.fetchall())

    # Get the tags too
    existing_tags = [t for s, t in get_all_hashes_tags(local_repository)]
    with override_driver_connection(remote_conn):
        tags = {t: s for s, t in get_all_hashes_tags(remote_repository) if t not in existing_tags}

    # Crawl the object tree to get the IDs and other metadata for all required objects.
    distinct_objects, object_meta = _extract_recursive_object_meta(remote_conn, table_meta)

    with override_driver_connection(remote_conn):
        object_locations = get_external_object_locations(list(distinct_objects)) if distinct_objects else []

    return snaps_to_fetch, table_meta, object_locations, object_meta, tags


def _extract_recursive_object_meta(remote_conn, table_meta):
    """Since an object can now depend on another object that's not mentioned in the commit tree,
    we now have to follow the objects' links to their parents until we have gathered all the required object IDs."""
    existing_objects = get_existing_objects()
    distinct_objects = set(o[2] for o in table_meta if o[2] not in existing_objects)
    known_objects = set()
    object_meta = []

    with override_driver_connection(remote_conn):
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


def pull(repository, remote, download_all=False):
    """
    Synchronizes the state of the local SplitGraph repository with the remote one, optionally downloading all new
    objects created on the remote.

    :param repository: Repository to pull changes from.
    :param remote: Name of the upstream to pull changes from.
    :param download_all: If True, downloads all objects and stores them locally. Otherwise, will only download required
        objects when a table is checked out.
    :return:
    """
    remote_info = get_remote_for(repository, remote)
    if not remote_info:
        raise SplitGraphException("No remote %s found for repository %s!" % (remote, repository))

    remote_conn_string, remote_repository = remote_info
    clone(remote_repository=remote_repository, remote_conn_string=remote_conn_string, local_repository=repository,
          download_all=download_all)


@manage_audit
def clone(remote_repository, remote_conn_string=None, local_repository=None, download_all=False, remote_conn=None):
    """
    Clones a remote SplitGraph repository or synchronizes remote changes with the local ones.

    :param remote_repository: Repository to clone.
    :param remote_conn_string: If set, overrides the default remote for this repository.
    :param local_repository: Local repository to clone into. If None, uses the same name.
    :param download_all: If True, downloads all objects and stores them locally. Otherwise, will only download required
        objects when a table is checked out.
    :param remote_conn: If not None, must be a psycopg connection object used by the client to connect to the remote
        driver. The local driver will still use the parameters specified in `remote_conn_string` to download the
        actual objects from the remote.
    """
    ensure_metadata_schema()

    local_repository = local_repository or remote_repository
    with get_connection().cursor() as cur:
        cur.execute(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(local_repository.to_schema())))

    conn_params = lookup_repo(remote_repository) if not remote_conn_string \
        else parse_connection_string(remote_conn_string)
    logging.info("Connecting to the remote driver...")
    remote_conn = remote_conn or make_conn(*conn_params)

    # Get the remote log and the list of objects we need to fetch.
    logging.info("Gathering remote metadata...")

    # This also registers the new versions locally.
    snaps_to_fetch, table_meta, object_locations, object_meta, tags = _get_required_snaps_objects(remote_conn,
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
        download_objects(serialize_connection_string(*conn_params),
                         objects_to_fetch=list(set(o[0] for o in object_meta)), object_locations=object_locations,
                         remote_conn=remote_conn)

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

    if get_remote_for(local_repository) is None:
        add_remote(local_repository, serialize_connection_string(*conn_params), remote_repository)


def local_clone(source, destination):
    """Clones one local repository into another, copying all of its commit history over. Doesn't do any checking out
    or materialization."""
    ensure_metadata_schema()

    _, table_meta, object_locations, object_meta, tags = _get_required_snaps_objects(get_connection(),
                                                                                     destination, source)

    # Map the tables to the actual objects no matter whether or not we're downloading them.
    register_objects(object_meta)
    register_object_locations(object_locations)
    register_tables(destination, table_meta)
    set_tags(destination, tags, force=False)
    # Don't check anything out, keep the repo bare.
    set_head(destination, None)


def push(local_repository, remote_conn_string=None, remote_repository=None, handler='DB', handler_options=None):
    """
    Inverse of `pull`: Pushes all local changes to the remote and uploads new objects.

    :param remote_conn_string: Connection string to the remote SG driver of the form
        `username:password@hostname:port/database.`
    :param remote_repository: Remote repository to push changes to.
    :param local_repository: Local repository to push changes from.
    :param handler: Name of the handler to use to upload objects. Use `DB` to push them to the remote or `S3`
        to store them in an S3 bucket.
    :param handler_options: For `S3`, a dictionary `{"username": username, "password", password}`.
    """
    if handler_options is None:
        handler_options = {}
    ensure_metadata_schema()

    remote_repository = remote_repository or local_repository
    conn = get_connection()

    logging.info("Connecting to the remote driver...")
    conn_params = lookup_repo(remote_repository) if not remote_conn_string \
        else parse_connection_string(remote_conn_string)
    remote_conn = make_conn(*conn_params)
    try:
        logging.info("Gathering remote metadata...")
        # Flip the two connections here: pretend the remote driver is local and download metadata from the local
        # driver instead of the remote.
        with override_driver_connection(remote_conn):
            # This also registers new commits remotely. Should make explicit and move down later on.
            snaps_to_push, table_meta, object_locations, object_meta, tags = \
                _get_required_snaps_objects(conn, remote_repository, local_repository)

        if not snaps_to_push:
            logging.info("Nothing to do.")
            return

        new_uploads = upload_objects(serialize_connection_string(*conn_params), list(set(o[0] for o in object_meta)),
                                     handler=handler, handler_params=handler_options, remote_conn=remote_conn)

        # Register the newly uploaded object locations locally and remotely.
        with override_driver_connection(remote_conn):
            register_objects(object_meta)
            register_object_locations(object_locations + new_uploads)
            register_tables(remote_repository, table_meta)
            set_tags(remote_repository, tags, force=False)

        register_object_locations(new_uploads)

        if not get_remote_for(local_repository, 'origin'):
            add_remote(local_repository, serialize_connection_string(*conn_params), remote_repository)

        remote_conn.commit()
        print("Uploaded metadata for %d object(s), %d table version(s) and %d tag(s)." % (len(object_meta),
                                                                                          len(table_meta),
                                                                                          len([t for t in tags if
                                                                                               t != 'HEAD'])))
    finally:
        remote_conn.close()
