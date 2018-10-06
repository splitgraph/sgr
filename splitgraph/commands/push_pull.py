import logging

from psycopg2.sql import SQL, Identifier
from splitgraph.commands.misc import make_conn
from splitgraph.config.repo_lookups import lookup_repo
from splitgraph.constants import SPLITGRAPH_META_SCHEMA, SplitGraphException, serialize_connection_string, \
    parse_connection_string
from splitgraph.meta_handler.common import ensure_metadata_schema
from splitgraph.meta_handler.images import get_all_images_parents, add_new_image
from splitgraph.meta_handler.misc import get_remote_for, add_remote
from splitgraph.meta_handler.objects import register_objects, register_tables, register_object_locations, \
    get_existing_objects, get_external_object_locations, get_object_meta
from splitgraph.meta_handler.tags import get_all_hashes_tags, set_tags, set_head
from splitgraph.objects.loading import download_objects, upload_objects
from splitgraph.pg_audit import manage_audit


def _get_required_snaps_objects(conn, remote_conn, local_mountpoint, remote_mountpoint):
    local_snap_parents = {snap_id: parent_id for snap_id, parent_id, _, _, _, _ in
                          get_all_images_parents(conn, local_mountpoint)}
    remote_snap_parents = {snap_id: (parent_id, created, comment, prov_type, prov_data)
                           for snap_id, parent_id, created, comment, prov_type, prov_data in
                           get_all_images_parents(remote_conn, remote_mountpoint)}

    # We assume here that none of the remote snapshot IDs have changed (are immutable) since otherwise the remote
    # would have created a new snapshot.
    snaps_to_fetch = [s for s in remote_snap_parents if s not in local_snap_parents]
    table_meta = []
    for snap_id in snaps_to_fetch:
        # This is not batched but there shouldn't be that many entries here anyway.
        remote_parent, remote_created, remote_comment, remote_prov, remote_provdata = remote_snap_parents[snap_id]
        add_new_image(conn, local_mountpoint, remote_parent, snap_id, remote_created, remote_comment, remote_prov,
                      remote_provdata)
        # Get the meta for all objects we'll need to fetch.
        with remote_conn.cursor() as cur:
            cur.execute(SQL("""SELECT snap_id, table_name, object_id FROM {0}.tables
                           WHERE mountpoint = %s AND snap_id = %s""").format(Identifier(SPLITGRAPH_META_SCHEMA)),
                        (remote_mountpoint, snap_id))
            table_meta.extend(cur.fetchall())

    # Get the tags too
    existing_tags = [t for s, t in get_all_hashes_tags(conn, local_mountpoint)]
    tags = {t: s for s, t in get_all_hashes_tags(remote_conn, remote_mountpoint) if t not in existing_tags}

    # Crawl the object tree to get the IDs and other metadata for all required objects.
    distinct_objects, object_meta = _extract_recursive_object_meta(conn, remote_conn, table_meta)
    object_locations = get_external_object_locations(remote_conn, list(distinct_objects)) if distinct_objects else []

    return snaps_to_fetch, table_meta, object_locations, object_meta, tags


def _extract_recursive_object_meta(conn, remote_conn, table_meta):
    """Since an object can now depend on another object that's not mentioned in the commit tree,
    we now have to follow the objects' links to their parents until we have gathered all the required object IDs."""
    existing_objects = get_existing_objects(conn)
    distinct_objects = set(o[2] for o in table_meta if o[2] not in existing_objects)
    known_objects = set()
    object_meta = []
    while True:
        new_parents = [o for o in distinct_objects if o not in known_objects]
        if not new_parents:
            break
        else:
            parents_meta = get_object_meta(remote_conn, new_parents)
            distinct_objects.update(
                set(o[2] for o in parents_meta if o[2] is not None and o[2] not in existing_objects))
            object_meta.extend(parents_meta)
            known_objects.update(new_parents)
    return distinct_objects, object_meta


def pull(conn, mountpoint, remote, download_all=False):
    """
    Synchronizes the state of the local SplitGraph repository with the remote one, optionally downloading all new
    objects created on the remote.

    :param conn: psycopg connection objects
    :param mountpoint: Mountpoint to pull changes from.
    :param remote: Name of the upstream to pull changes from.
    :param download_all: If True, downloads all objects and stores them locally. Otherwise, will only download required
        objects when a table is checked out.
    :return:
    """
    remote_info = get_remote_for(conn, mountpoint, remote)
    if not remote_info:
        raise SplitGraphException("No remote %s found for mountpoint %s!" % (remote, mountpoint))

    remote_conn_string, remote_mountpoint = remote_info
    clone(conn, remote_conn_string=remote_conn_string, remote_mountpoint=remote_mountpoint,
          local_mountpoint=mountpoint, download_all=download_all)


@manage_audit
def clone(conn, remote_mountpoint, remote_conn_string=None,
          local_mountpoint=None, download_all=False, remote_conn=None):
    """
    Clones a remote SplitGraph repository or synchronizes remote changes with the local ones.

    :param conn: psycopg connection object.
    :param remote_mountpoint: Repository to clone.
    :param remote_conn_string: If set, overrides the default remote for this repository.
    :param local_mountpoint: Local mountpoint to clone into. If None, uses the same name.
    :param download_all: If True, downloads all objects and stores them locally. Otherwise, will only download required
        objects when a table is checked out.
    :param remote_conn: If not None, must be a psycopg connection object used by the client to connect to the remote
        driver. The local driver will still use the parameters specified in `remote_conn_string` to download the
        actual objects from the remote.
    """
    ensure_metadata_schema(conn)

    local_mountpoint = local_mountpoint or remote_mountpoint
    with conn.cursor() as cur:
        cur.execute(SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(local_mountpoint)))

    conn_params = lookup_repo(conn, remote_mountpoint) if not remote_conn_string \
        else parse_connection_string(remote_conn_string)
    logging.info("Connecting to the remote driver...")
    remote_conn = remote_conn or make_conn(*conn_params)

    # Get the remote log and the list of objects we need to fetch.
    logging.info("Gathering remote metadata...")

    # This also registers the new versions locally.
    snaps_to_fetch, table_meta, object_locations, object_meta, tags = _get_required_snaps_objects(conn, remote_conn,
                                                                                                  local_mountpoint,
                                                                                                  remote_mountpoint)

    if not snaps_to_fetch:
        logging.info("Nothing to do.")
        return

    # Don't actually download any real objects until the user tries to check out a revision.
    if download_all:
        # Check which new objects we need to fetch/preregister.
        # We might already have some objects prefetched
        # (e.g. if a new version of the table is the same as the old version)
        logging.info("Fetching remote objects...")
        download_objects(conn, serialize_connection_string(*conn_params),
                         objects_to_fetch=list(set(o[0] for o in object_meta)),
                         object_locations=object_locations, remote_conn=remote_conn)

    # Map the tables to the actual objects no matter whether or not we're downloading them.
    register_objects(conn, object_meta)
    register_object_locations(conn, object_locations)
    register_tables(conn, local_mountpoint, table_meta)
    set_tags(conn, local_mountpoint, tags, force=False)
    # Don't check anything out, keep the repo bare.
    set_head(conn, local_mountpoint, None)

    print("Fetched metadata for %d object(s), %d table version(s) and %d tag(s)." % (len(object_meta),
                                                                                     len(table_meta),
                                                                                     len([t for t in tags if
                                                                                          t != 'HEAD'])))

    if get_remote_for(conn, local_mountpoint) is None:
        add_remote(conn, local_mountpoint, serialize_connection_string(*conn_params), remote_mountpoint)


def local_clone(conn, source, destination):
    """Clones one local mountpoint into another, copying all of its commit history over. Doesn't do any checking out
    or materialization."""
    ensure_metadata_schema(conn)

    _, table_meta, object_locations, object_meta, tags = _get_required_snaps_objects(conn, conn,
                                                                                     destination,
                                                                                     source)

    # Map the tables to the actual objects no matter whether or not we're downloading them.
    register_objects(conn, object_meta)
    register_object_locations(conn, object_locations)
    register_tables(conn, destination, table_meta)
    set_tags(conn, destination, tags, force=False)
    # Don't check anything out, keep the repo bare.
    set_head(conn, destination, None)


def push(conn, local_mountpoint, remote_conn_string=None, remote_mountpoint=None, handler='DB', handler_options=None):
    """
    Inverse of `pull`: Pushes all local changes to the remote and uploads new objects.

    :param conn: psycopg connection object
    :param remote_conn_string: Connection string to the remote SG driver of the form
        `username:password@hostname:port/database.`
    :param remote_mountpoint: Remote mountpoint to push changes to.
    :param local_mountpoint: Local mountpoint to push changes from.
    :param handler: Name of the handler to use to upload objects. Use `DB` to push them to the remote, `FILE`
        to store them in a directory that can be accessed from the client and `HTTP` to upload them to HTTP.
    :param handler_options: For `HTTP`, a dictionary `{"username": username, "password", password}`. For `FILE`,
        a dictionary `{"path": path}` specifying the directory where the objects shall be saved.
    """
    if handler_options is None:
        handler_options = {}
    ensure_metadata_schema(conn)

    remote_mountpoint = remote_mountpoint or local_mountpoint

    # Could actually be done by flipping the arguments in pull but that assumes the remote SG driver can connect
    # to us directly, which might not be the case. Although tunnels? Still, a lot of code here similar to pull.
    logging.info("Connecting to the remote driver...")
    conn_params = lookup_repo(conn, remote_mountpoint) if not remote_conn_string \
        else parse_connection_string(remote_conn_string)
    remote_conn = make_conn(*conn_params)
    try:
        logging.info("Gathering remote metadata...")
        # This also registers new commits remotely. Should make explicit and move down later on.
        snaps_to_push, table_meta, object_locations, object_meta, tags = _get_required_snaps_objects(remote_conn, conn,
                                                                                                     remote_mountpoint,
                                                                                                     local_mountpoint)

        if not snaps_to_push:
            logging.info("Nothing to do.")
            return

        new_uploads = upload_objects(conn, serialize_connection_string(*conn_params),
                                     list(set(o[0] for o in object_meta)), handler=handler,
                                     handler_params=handler_options, remote_conn=remote_conn)
        # Register the newly uploaded object locations locally and remotely.
        register_objects(remote_conn, object_meta)
        register_object_locations(remote_conn, object_locations + new_uploads)
        register_tables(remote_conn, remote_mountpoint, table_meta)
        set_tags(remote_conn, remote_mountpoint, tags, force=False)
        # Kind of have to commit here in any case?
        remote_conn.commit()
        register_object_locations(conn, new_uploads)

        if not get_remote_for(conn, local_mountpoint, 'origin'):
            add_remote(conn, local_mountpoint, serialize_connection_string(*conn_params), remote_mountpoint)

        logging.info("Uploaded metadata for %d object(s), %d table version(s) and %d tag(s)." % (len(object_meta),
                                                                                                 len(table_meta),
                                                                                                 len([t for t in tags if
                                                                                                      t != 'HEAD'])))
    finally:
        remote_conn.close()
