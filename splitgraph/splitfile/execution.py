"""
Functions for executing Splitfiles.
"""

import json
from hashlib import sha256
from importlib import import_module
from random import getrandbits

from splitgraph.config import CONFIG
from splitgraph.core.engine import repository_exists, lookup_repository
from splitgraph.core.repository import Repository, clone
from splitgraph.engine import get_engine
from splitgraph.exceptions import SplitGraphException
from splitgraph.hooks.mount_handlers import get_mount_handler
from ._parsing import parse_commands, extract_nodes, get_first_or_none, parse_image_spec, \
    extract_all_table_aliases, parse_custom_command


def _combine_hashes(hashes):
    return sha256(''.join(hashes).encode('ascii')).hexdigest()


def _checkout_or_calculate_layer(output, image_hash, calc_func):
    # Future optimization here: don't actually check the layer out if it exists -- only do it at Splitfile execution
    # end or when a command needs it.

    # Have we already calculated this hash?
    try:
        output.images.by_hash(image_hash).checkout()
        print(" ---> Using cache")
    except SplitGraphException:
        calc_func()
    print(" ---> %s" % image_hash[:12])


def execute_commands(commands, params=None, output=None, output_base='0' * 32):
    """
    Executes a series of Splitfile commands.

    :param commands: A string with the raw Splitfile.
    :param params: A dictionary of parameters to be applied to the Splitfile (`${PARAM}` is replaced with the specified
        parameter value).
    :param output: Output repository to execute the Splitfile against.
    :param output_base: If not None, a revision that gets checked out for all Splitfile actions to be committed
        on top of it.
    """
    if params is None:
        params = {}
    if output and repository_exists(output) and output_base is not None:
        output.images.by_hash(output_base).checkout()
    # Use a random target schema if unspecified.
    output = output or "output_%0.2x" % getrandbits(16)

    # Don't initialize the output until a command writing to it asks us to
    # (otherwise we might have a FROM ... AS output_name change it).

    def _initialize_output(output):
        if not repository_exists(output):
            output.init()

    from splitgraph.commandline._common import Color, truncate_line
    node_list = parse_commands(commands, params=params)
    for i, node in enumerate(node_list):
        print(Color.BOLD + "\nStep %d/%d : %s" % (i + 1, len(node_list), truncate_line(node.text, length=60))
              + Color.END)
        if node.expr_name == 'from':
            output = _execute_from(node, output)

        elif node.expr_name == 'import':
            _initialize_output(output)
            _execute_import(node, output)

        elif node.expr_name == 'sql' or node.expr_name == 'sql_file':
            _initialize_output(output)
            _execute_sql(node, output)

        elif node.expr_name == 'custom':
            _initialize_output(output)
            _execute_custom(node, output)

    get_engine().commit()


def _execute_sql(node, output):
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
    output_head = output.head.image_hash
    target_hash = _combine_hashes([output_head, sha256(sql_command.encode('utf-8')).hexdigest()])

    def _calc():
        print("Executing SQL...")
        output.run_sql(sql_command)
        output.commit(target_hash, comment=sql_command)
        output.images.by_hash(target_hash).set_provenance('SQL', sql=sql_command)

    _checkout_or_calculate_layer(output, target_hash, _calc)


def _execute_from(node, output):
    interesting_nodes = extract_nodes(node, ['repo_source', 'repository'])
    repo_source = get_first_or_none(interesting_nodes, 'repo_source')
    output_node = get_first_or_none(interesting_nodes, 'repository')
    if output_node:
        # AS (output) detected, change the current output repository to it.
        output = Repository.from_schema(output_node.match.group(0))
        print("Changed output repository to %s" % str(output))

        # NB this destroys all data in the case where we ran some commands in the Splitfile and then
        # did FROM (...) without AS repository
        if repository_exists(output):
            print("Clearing all output from %s" % str(output))
            output.rm()
    if not repository_exists(output):
        output.init()
    if repo_source:
        repository, tag_or_hash = parse_image_spec(repo_source)
        source_repo = lookup_repository(repository.to_schema(), include_local=True)

        if source_repo.engine.name == 'LOCAL':
            # For local repositories, make sure to update them if they've an upstream
            try:
                source_repo.pull()
            except SplitGraphException:
                pass

        # Get the target snap ID from the source repo: otherwise, if the tag is, say, 'latest' and
        # the output has just had the base commit (000...) created in it, that commit will be the latest.
        clone(source_repo, local_repository=output, download_all=False)
        output.images.by_hash(source_repo.images[tag_or_hash].image_hash).checkout()
        output.head.set_provenance('FROM', source=source_repo)
    else:
        # FROM EMPTY AS repository -- initializes an empty repository (say to create a table or import
        # the results of a previous stage in a multistage build.
        # In this case, if AS repository has been specified, it's already been initialized. If not, this command
        # literally does nothing
        if not output_node:
            raise SplitGraphException("FROM EMPTY without AS (repository) does nothing!")
    return output


def _execute_import(node, output):
    interesting_nodes = extract_nodes(node, ['repo_source', 'mount_source', 'tables'])
    table_names, table_aliases, table_queries = extract_all_table_aliases(interesting_nodes[-1])
    if interesting_nodes[0].expr_name == 'repo_source':
        # Import from a repository (local or remote)
        repository, tag_or_hash = parse_image_spec(interesting_nodes[0])
        _execute_repo_import(repository, table_names, tag_or_hash, output, table_aliases, table_queries)
    else:
        # Extract the identifier (FDW name), the connection string and the FDW params (JSON-encoded, everything
        # between the single quotes).
        mount_nodes = extract_nodes(interesting_nodes[0],
                                    ['identifier', 'no_db_conn_string', 'non_single_quote'])
        fdw_name = mount_nodes[0].match.group(0)
        conn_string = mount_nodes[1].match
        fdw_params = mount_nodes[2].match.group(0).replace("\\'", "'")  # Unescape the single quote

        _execute_db_import(conn_string, fdw_name, fdw_params, table_names, output, table_aliases, table_queries)


def _execute_db_import(conn_string, fdw_name, fdw_params, table_names, target_mountpoint, table_aliases, table_queries):
    mount_handler = get_mount_handler(fdw_name)
    tmp_mountpoint = Repository.from_schema(fdw_name + '_tmp_staging')
    tmp_mountpoint.rm()
    try:
        handler_kwargs = json.loads(fdw_params)
        handler_kwargs.update(dict(server=conn_string.group(3), port=int(conn_string.group(4)),
                                   username=conn_string.group(1),
                                   password=conn_string.group(2)))
        mount_handler(tmp_mountpoint.to_schema(), **handler_kwargs)
        # The foreign database is a moving target, so the new image hash is random.
        # Maybe in the future, when the object hash is a function of its contents, we can be smarter here...
        target_hash = "%0.2x" % getrandbits(256)
        target_mountpoint.import_tables(table_aliases, tmp_mountpoint, table_names, target_hash=target_hash,
                                        foreign_tables=True, table_queries=table_queries)
        target_mountpoint.images.by_hash(target_hash).set_provenance('MOUNT')
    finally:
        tmp_mountpoint.rm()


def _execute_repo_import(repository, table_names, tag_or_hash, target_repository, table_aliases, table_queries):
    # Don't use the actual routine here as we want more control: clone the remote repo in order to turn
    # the tag into an actual hash
    tmp_repo = Repository(repository.namespace, repository.repository + '_clone_tmp')
    try:
        # Calculate the hash of the new layer by combining the hash of the previous layer,
        # the hash of the source and all the table names/aliases getting imported.
        # This can be made more granular later by using, say, the object IDs of the tables
        # that are getting imported (so that if there's a new commit with some of the same objects,
        # we don't invalidate the downstream).
        # If table_names actually contains queries that generate data from tables, we can still use
        # it for hashing: we assume that the queries are deterministic, so if the query is changed,
        # the whole layer is invalidated.
        print("Resolving repository %s" % str(repository))
        source_repo = lookup_repository(repository.to_schema(), include_local=True)

        if source_repo.engine.name != 'LOCAL':
            clone(source_repo, local_repository=tmp_repo, download_all=False)
            source_hash = tmp_repo.images[tag_or_hash].image_hash
            source_mountpoint = tmp_repo
        else:
            # For local repositories, first try to pull them to see if they are clones of a remote.
            try:
                source_repo.pull()
            except SplitGraphException:
                pass
            source_hash = source_repo.images[tag_or_hash].image_hash
            source_mountpoint = source_repo
        output_head = target_repository.head.image_hash
        target_hash = _combine_hashes(
            [output_head, source_hash] + [sha256(n.encode('utf-8')).hexdigest() for n in
                                          table_names + table_aliases])

        def _calc():
            print("Importing tables %r:%s from %s into %s" % (
                table_names, source_hash[:12], str(repository), str(target_repository)))
            target_repository.import_tables(table_aliases, source_mountpoint, table_names, image_hash=source_hash,
                                            target_hash=target_hash, table_queries=table_queries)
            target_repository.images.by_hash(target_hash).set_provenance('IMPORT', source_repository=repository,
                                                                         source_hash=source_hash,
                                                                         tables=table_names,
                                                                         table_aliases=table_aliases,
                                                                         table_queries=table_queries)

        _checkout_or_calculate_layer(target_repository, target_hash, _calc)
    finally:
        tmp_repo.rm()


def _execute_custom(node, output):
    command, args = parse_custom_command(node)

    # Locate the command in the config file and instantiate it.
    cmd_fq_class = CONFIG.get('commands', {}).get(command)
    if not cmd_fq_class:
        raise SplitGraphException("Custom command {0} not found in the config! Make sure you add an entry to your"
                                  " config like so:\n  [commands]  \n{0}=path.to.command.Class".format(command))

    ix = cmd_fq_class.rindex('.')
    try:
        cmd_class = getattr(import_module(cmd_fq_class[:ix]), cmd_fq_class[ix + 1:])
    except AttributeError as e:
        raise SplitGraphException("Error loading custom command {0}".format(command), e)
    except ImportError as e:
        raise SplitGraphException("Error loading custom command {0}".format(command), e)

    get_engine().run_sql("SET search_path TO %s", (output.to_schema(),))
    command = cmd_class()

    # Pre-flight check: get the new command hash and see if we can short-circuit and just check the image out.
    command_hash = command.calc_hash(repository=output, args=args)
    output_head = output.head.image_hash

    if command_hash is not None:
        image_hash = _combine_hashes([output_head, command_hash])
        try:
            output.images.by_hash(image_hash).checkout()
            print(" ---> Using cache")
            return
        except SplitGraphException:
            pass

    print(" Executing custom command...")
    exec_hash = command.execute(repository=output, args=args)
    command_hash = command_hash or exec_hash or "%0.2x" % getrandbits(256)

    image_hash = _combine_hashes([output_head, command_hash])
    print(" ---> %s" % image_hash[:12])

    # Check just in case if the new hash produced by the command already exists.
    try:
        output.images.by_hash(image_hash).checkout()
    except SplitGraphException:
        # Full command as a commit comment
        output.commit(image_hash, comment=node.text)
        # Worth storing provenance here anyway?


def rebuild_image(image, source_replacement):
    """
    Recreates the Splitfile used to create a given image and reruns it, replacing its dependencies with a different
    set of versions.

    :param image: Image object
    :param source_replacement: A map that specifies replacement images/tags for repositories that the image depends on
    """
    splitfile_commands = image.to_splitfile(err_on_end=False, source_replacement=source_replacement)
    # Params are supposed to be stored in the commands already (baked in) -- what if there's sensitive data there?
    execute_commands('\n'.join(splitfile_commands), output=image.repository)
