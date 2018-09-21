import json
from hashlib import sha256
from random import getrandbits

from splitgraph.commands import checkout, init, unmount, clone, import_tables, commit, image_hash_to_sgfile
from splitgraph.commands.mount_handlers import get_mount_handler
from splitgraph.commands.push_pull import local_clone, pull
from splitgraph.config.repo_lookups import lookup_repo
from splitgraph.constants import SplitGraphException, serialize_connection_string
from splitgraph.meta_handler.misc import mountpoint_exists
from splitgraph.meta_handler.provenance import store_import_provenance, store_sql_provenance, store_mount_provenance, \
    store_from_provenance
from splitgraph.meta_handler.tags import get_current_head, tag_or_hash_to_actual_hash
from splitgraph.pg_utils import execute_sql_in
from splitgraph.sgfile.parsing import parse_commands, extract_nodes, get_first_or_none, parse_repo_source, \
    extract_all_table_aliases


def _canonicalize(sql):
    return ' '.join(sql.lower().split())


def _combine_hashes(hashes):
    return sha256(''.join(hashes).encode('ascii')).hexdigest()


def _checkout_or_calculate_layer(conn, output, image_hash, calc_func):
    # Have we already calculated this hash?
    try:
        checkout(conn, output, image_hash)
        print("Using the cache.")
    except SplitGraphException:
        calc_func()


def execute_commands(conn, commands, params=None, output=None, output_base='0' * 32):
    """
    Executes a series of SGFile commands.

    :param conn: psycopg connection object
    :param commands: A string with the raw SGFile.
    :param params: A dictionary of parameters to be applied to the SGFile (`${PARAM}` is replaced with the specified
        parameter value).
    :param output: Output mountpoint to execute the SGFile against.
    :param output_base: If not None, a revision that gets checked out for all SGFile actions to be committed
        on top of it.
    """
    if params is None:
        params = {}
    if output and mountpoint_exists(conn, output) and output_base is not None:
        checkout(conn, output, output_base)
    # Use a random target schema if unspecified.
    output = output or "output_%0.2x" % getrandbits(16)

    # Don't initialize the output until a command writing to it asks us to
    # (otherwise we might have a FROM ... AS output_name change it).

    def _initialize_output(output):
        if not mountpoint_exists(conn, output):
            init(conn, output)

    node_list = parse_commands(commands, params=params)
    for i, node in enumerate(node_list):
        print("\n-> %d/%d %s" % (i + 1, len(node_list), node.text))
        if node.expr_name == 'from':
            output = _execute_from(conn, node, output)

        elif node.expr_name == 'import':
            _initialize_output(output)
            _execute_import(conn, node, output)

        elif node.expr_name == 'sql' or node.expr_name == 'sql_file':
            _initialize_output(output)
            _execute_sql(conn, node, output)
    conn.commit()


def _execute_sql(conn, node, output):
    # Calculate the hash of the layer we are trying to create.
    # Since we handle the "input" hashing in the import step, we don't need to care about the sources here.
    # Later on, we could enhance the caching and base the hash of the command on the hashes of objects that
    # definitely go there as sources.
    node_contents = extract_nodes(node, ['non_newline'])[0].text
    if node.expr_name == 'sql_file':
        print("Loading the SQL commands from %s" % node_contents)
        with open(node_contents, 'r') as f:
            # Possibly use a different method to calculate the image hash for commands originating from
            # SQL files instead?
            # Don't "canonicalize" it here to get rid of whitespace, just hash the whole file.
            sql_command = f.read()
    else:
        sql_command = node_contents
    output_head = get_current_head(conn, output)
    target_hash = _combine_hashes([output_head, sha256(sql_command.encode('utf-8')).hexdigest()])
    print('%s:%s -> %s' % (output, output_head[:12], target_hash[:12]))

    def _calc():
        print("Executing SQL...")
        execute_sql_in(conn, output, sql_command)
        commit(conn, output, target_hash, comment=sql_command)
        store_sql_provenance(conn, output, target_hash, sql_command)

    _checkout_or_calculate_layer(conn, output, target_hash, _calc)


def _execute_from(conn, node, output):
    interesting_nodes = extract_nodes(node, ['repo_source', 'mountpoint'])
    repo_source = get_first_or_none(interesting_nodes, 'repo_source')
    output_node = get_first_or_none(interesting_nodes, 'mountpoint')
    if output_node:
        # AS (output) detected, change the current output mountpoint to it.
        output = output_node.match.group(0)
        print("Changed output mountpoint to %s" % output)

        # NB this destroys all data in the case where we ran some commands in the sgfile and then
        # did FROM (...) without AS mountpoint_name
        if mountpoint_exists(conn, output):
            print("Clearing all output from %s" % output)
            unmount(conn, output)
    if not mountpoint_exists(conn, output):
        init(conn, output)
    if repo_source:
        mountpoint, tag_or_hash = parse_repo_source(repo_source)
        print("Resolving repository %s" % mountpoint)
        location = lookup_repo(conn, mountpoint, include_local=True)

        if location != 'LOCAL':
            clone(conn, mountpoint, remote_conn_string=serialize_connection_string(*location),
                  local_mountpoint=output, download_all=False)
            checkout(conn, output, tag_or_hash_to_actual_hash(conn, output, tag_or_hash))
        else:
            # For local repositories, first try to pull them to see if they are clones of a remote.
            try:
                pull(conn, mountpoint, remote='origin')
            except SplitGraphException:
                pass
            # Get the target snap ID from the source repo: otherwise, if the tag is, say, 'latest' and
            # the output has just had the base commit (000...) created in it, that commit will be the latest.
            to_checkout = tag_or_hash_to_actual_hash(conn, mountpoint, tag_or_hash)
            print("Cloning %s into %s..." % (mountpoint, output))
            local_clone(conn, mountpoint, output)
            checkout(conn, output, to_checkout)
        store_from_provenance(conn, output, get_current_head(conn, output), mountpoint)
    else:
        # FROM EMPTY AS mountpoint -- initializes an empty mountpoint (say to create a table or import
        # the results of a previous stage in a multistage build.
        # In this case, if AS mountpoint has been specified, it's already been initialized. If not, this command
        # literally does nothing
        if not output_node:
            raise SplitGraphException("FROM EMPTY without AS (mountpoint) does nothing!")
    return output


def _execute_import(conn, node, output):
    interesting_nodes = extract_nodes(node, ['repo_source', 'mount_source', 'tables'])
    table_names, table_aliases, table_queries = extract_all_table_aliases(interesting_nodes[-1])
    if interesting_nodes[0].expr_name == 'repo_source':
        # Import from a repository (local or remote)
        mountpoint, tag_or_hash = parse_repo_source(interesting_nodes[0])
        _execute_repo_import(conn, mountpoint, table_names, tag_or_hash, output, table_aliases, table_queries)
    else:
        # Extract the identifier (FDW name), the connection string and the FDW params (JSON-encoded, everything
        # between the single quotes).
        mount_nodes = extract_nodes(interesting_nodes[0],
                                    ['identifier', 'no_db_conn_string', 'non_single_quote'])
        fdw_name = mount_nodes[0].match.group(0)
        conn_string = mount_nodes[1].match
        fdw_params = mount_nodes[2].match.group(0).replace("\\'", "'")  # Unescape the single quote

        _execute_db_import(conn, conn_string, fdw_name, fdw_params, table_names, output, table_aliases, table_queries)


def _execute_db_import(conn, conn_string, fdw_name, fdw_params, table_names, target_mountpoint, table_aliases,
                       table_queries):
    mount_handler = get_mount_handler(fdw_name)
    tmp_mountpoint = fdw_name + '_tmp_staging'
    unmount(conn, tmp_mountpoint)
    try:
        handler_kwargs = json.loads(fdw_params)
        handler_kwargs.update(dict(server=conn_string.group(3), port=int(conn_string.group(4)),
                                   username=conn_string.group(1),
                                   password=conn_string.group(2)))
        mount_handler(conn, tmp_mountpoint, **handler_kwargs)
        # The foreign database is a moving target, so the new image hash is random.
        # Maybe in the future, when the object hash is a function of its contents, we can be smarter here...
        output_head = get_current_head(conn, target_mountpoint)
        target_hash = "%0.2x" % getrandbits(256)
        print('%s:%s -> %s' % (target_mountpoint, output_head[:12], target_hash[:12]))

        import_tables(conn, tmp_mountpoint, table_names, target_mountpoint, table_aliases, image_hash=target_hash,
                      foreign_tables=True, table_queries=table_queries)
        store_mount_provenance(conn, target_mountpoint, target_hash)
    finally:
        unmount(conn, tmp_mountpoint)


def _execute_repo_import(conn, mountpoint, table_names, tag_or_hash, target_mountpoint, table_aliases,
                         table_queries):
    # Don't use the actual routine here as we want more control: clone the remote repo in order to turn
    # the tag into an actual hash
    tmp_mountpoint = mountpoint + '_clone_tmp'
    try:
        # Calculate the hash of the new layer by combining the hash of the previous layer,
        # the hash of the source and all the table names/aliases getting imported.
        # This can be made more granular later by using, say, the object IDs of the tables
        # that are getting imported (so that if there's a new commit with some of the same objects,
        # we don't invalidate the downstream).
        # If table_names actually contains queries that generate data from tables, we can still use
        # it for hashing: we assume that the queries are deterministic, so if the query is changed,
        # the whole layer is invalidated.
        print("Resolving repository %s" % mountpoint)
        location = lookup_repo(conn, mountpoint, include_local=True)

        if location != 'LOCAL':
            clone(conn, mountpoint, remote_conn_string=serialize_connection_string(*location),
                  local_mountpoint=tmp_mountpoint, download_all=False)
            source_hash = tag_or_hash_to_actual_hash(conn, tmp_mountpoint, tag_or_hash)
            source_mountpoint = tmp_mountpoint
        else:
            # For local repositories, first try to pull them to see if they are clones of a remote.
            try:
                pull(conn, mountpoint, remote='origin')
            except SplitGraphException:
                pass
            source_hash = tag_or_hash_to_actual_hash(conn, mountpoint, tag_or_hash)
            source_mountpoint = mountpoint
        output_head = get_current_head(conn, target_mountpoint)
        target_hash = _combine_hashes(
            [output_head, source_hash] + [sha256(n.encode('utf-8')).hexdigest() for n in
                                          table_names + table_aliases])

        print('%s:%s, %s:%s -> %s' % (mountpoint, source_hash[:12],
                                      target_mountpoint, output_head[:12], target_hash[:12]))

        def _calc():
            print("Importing tables %r:%s from %s into %s" % (
                table_names, source_hash[:12], mountpoint, target_mountpoint))
            import_tables(conn, source_mountpoint, table_names, target_mountpoint, table_aliases,
                          image_hash=source_hash, target_hash=target_hash, table_queries=table_queries)
            store_import_provenance(conn, target_mountpoint, target_hash, mountpoint, source_hash, table_names,
                                    table_aliases, table_queries)

        _checkout_or_calculate_layer(conn, target_mountpoint, target_hash, _calc)
    finally:
        unmount(conn, tmp_mountpoint)


def rerun_image_with_replacement(conn, mountpoint, image_hash, source_replacement):
    """
    Recreates the sgfile used to create a given image and reruns it, replacing its dependencies with a different
    set of versions.

    :param conn: Psycopg connection object.
    :param mountpoint: Local repository where the image is located.
    :param image_hash: Hash of the image to rerun
    :param source_replacement: A map that specifies replacement images/tags for repositories that the image depends on
    :return:
    """
    sgfile_commands = image_hash_to_sgfile(conn, mountpoint, image_hash, err_on_end=False,
                                           source_replacement=source_replacement)
    # Params are supposed to be stored in the commands already (baked in) -- what if there's sensitive data there?
    execute_commands(conn, '\n'.join(sgfile_commands), output=mountpoint)