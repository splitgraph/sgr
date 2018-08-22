import json
import re
from hashlib import sha256
from random import getrandbits

from parsimonious.grammar import Grammar

from splitgraph.commands import init, commit, checkout, import_tables, clone, unmount
from splitgraph.commands.mounting import get_mount_handler
from splitgraph.commands.push_pull import local_clone
from splitgraph.constants import SplitGraphException
from splitgraph.meta_handler import get_current_head, get_tagged_id, mountpoint_exists
from splitgraph.pg_replication import _replication_slot_exists

SGFILE_GRAMMAR = Grammar(r"""
    commands = space command space (newline space command space)*
    command = comment / import / from / sql 
    comment = space "#" non_newline
    from = "FROM" space ("EMPTY" / repo_source) (space "AS" space mountpoint)?
    import = "FROM" space source space "IMPORT" space tables
    sql = "SQL" space sql_statement
    
    table = ((table_name / table_query) space "AS" space table_alias) / table_name
    
    table_query = "{" non_curly_brace "}"
    tables = "ALL" / (table space ("," space table)*)
    source = mount_source / repo_source
    repo_source = (conn_string space)? mountpoint (":" tag)?
    mount_source = "MOUNT" space handler space no_db_conn_string space handler_options
    
    image_hash = ~"[0-9a-f]*"i
    
    # TBH these ones that map to "identifier" aren't realy necessary since parsimonious calls those nodes
    # "identifier" anyway. This is so that the grammar is slightly more readable. 
    
    handler = identifier
    mountpoint = identifier
    table_name = identifier
    table_alias = identifier
    tag = identifier
    handler_options = "'" non_single_quote "'"
    sql_statement = non_newline
    
    newline = ~"\n*"
    non_newline = ~"[^\n]*"
    
    # I've no idea why we need so many slashes here. The purpose of this regex is to consume anything
    # that's not a closing curly brace or \} (an escaped curly brace).
    non_curly_brace = ~"(\\\\}|[^}])*"
    
    # Yeah, six slashes should be about enough to capture \'
    non_single_quote = ~"(\\\\\\'|[^'])*"
    
    conn_string = ~"\S+:\S+@.+:\d+\/\S+"
    no_db_conn_string = ~"(\S+):(\S+)@(.+):(\d+)"
    identifier = ~"[_a-zA-Z0-9\-]+"
    space = ~"\s*"
""")


def _canonicalize(sql):
    return ' '.join(sql.lower().split())


def _combine_hashes(hashes):
    return sha256(''.join(hashes).encode('ascii')).hexdigest()


def preprocess(commands, params={}):
    # Also replaces all $PARAM in the sgfile text with the params in the dictionary.
    commands = commands.replace("\\\n", "")
    for k, v in params.items():
        # Regex fun: if the replacement is '\1' + substitution (so that we put back the previously-consumed
        # possibly-escape character) and the substitution begins with a number (like an IP address),
        # then it gets treated as a match group (say, \11) which fails silently and adds weird gibberish
        # to the result.
        commands = re.sub(r'([^\\])\${' + re.escape(k) + '}', r'\g<1>' + str(v), commands, flags=re.MULTILINE)
    # Search for any unreplaced $-parameters
    unreplaced = set(re.findall(r'[^\\](\${\S+})', commands, flags=re.MULTILINE))
    if unreplaced:
        raise SplitGraphException("Unknown values for parameters " + ', '.join(unreplaced) + '!')
    # Finally, replace the escaped $
    return commands.replace('\\$', '$')


def parse_commands(commands, params={}):
    # Unpacks the parse tree into a list of command nodes.
    commands = preprocess(commands, params)
    parse_tree = SGFILE_GRAMMAR.parse(commands)
    return [n.children[0] for n in _extract_nodes(parse_tree, ['command']) if n.children[0].expr_name != 'comment']


def _extract_nodes(node, types):
    # Crawls the parse tree and only extracts nodes of given types.
    # Doesn't crawl further down if it reaches a required type.
    if node.expr_name in types:
        return [node]
    result = []
    for child in node.children:
        result.extend(_extract_nodes(child, types))
    return result


def _get_first_or_none(node_list, node_type):
    # Gets the first node of type node_type from node_list, returns None if it doesn't exist.
    for n in node_list:
        if n.expr_name == node_type:
            return n


def _parse_table_alias(table_node):
    # Extracts the table name (or a query forming the table) and its alias from the parse tree
    table_name_alias = _extract_nodes(table_node, ['identifier', 'non_curly_brace'])
    table_name = table_name_alias[0].match.group(0)
    table_is_query = table_name_alias[0].expr_name == 'non_curly_brace'
    if table_is_query:
        # Unescape the closing curly brace that marked the end of the query
        table_name = table_name.replace('\\}', '}')
    if len(table_name_alias) > 1:
        table_alias = table_name_alias[1].match.group(0)
        return table_name, table_alias, table_is_query
    return table_name, table_name, table_is_query


def _extract_all_table_aliases(node):
    tables = _extract_nodes(node, ['table'])
    if not tables:
        # No tables specified (imports all tables from the mounted db / repo)
        return [], [], []
    return zip(*[_parse_table_alias(table) for table in tables])


def _checkout_or_calculate_layer(conn, output, image_hash, calc_func):
    # Have we already calculated this hash?
    try:
        checkout(conn, output, image_hash)
        print("Using the cache.")
    except SplitGraphException:
        calc_func()


def execute_commands(conn, commands, params={}, output=None, output_base='0'*32):
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
            interesting_nodes = _extract_nodes(node, ['repo_source', 'identifier'])
            repo_source = _get_first_or_none(interesting_nodes, 'repo_source')
            output_node = _get_first_or_none(interesting_nodes, 'identifier')

            if output_node:
                # AS (output) detected, change the current output mountpoint to it.
                output = output_node.match.group(0)
                print("Changed output mountpoint to %s" % output)

                # NB this destroys all data in the case where we ran some commands in the sgfile and then
                # did FROM (...) without AS mountpoint_name
                if mountpoint_exists(conn, output):
                    print("Clearing all output from %s" % output)
                    unmount(conn, output)
                    init(conn, output)

            _initialize_output(output)

            if repo_source:
                conn_string, mountpoint, tag = _parse_repo_source(repo_source)
                if conn_string:
                    # At some point here we'll also actually search for the image hash locally and not clone it?
                    clone(conn, conn_string, mountpoint, output, download_all=False)
                    checkout(conn, output, tag=tag)
                else:
                    # Get the target snap ID from the source repo: otherwise, if the tag is, say, 'latest' and
                    # the output has just had the base commit (000...) created in it, that commit will be the latest.
                    to_checkout = get_tagged_id(conn, mountpoint, tag)
                    print("Cloning %s into %s..." % (mountpoint, output))
                    local_clone(conn, mountpoint, output)
                    checkout(conn, output, to_checkout)
            else:
                # FROM EMPTY AS mountpoint -- initializes an empty mountpoint (say to create a table or import
                # the results of a previous stage in a multistage build.
                # In this case, if AS mountpoint has been specified, it's already been initialized. If not, this command
                # literally does nothing
                if not output_node:
                    raise SplitGraphException("FROM EMPTY without AS (mountpoint) does nothing!")

        elif node.expr_name == 'import':
            interesting_nodes = _extract_nodes(node, ['repo_source', 'mount_source', 'tables'])
            table_names, table_aliases, table_queries = _extract_all_table_aliases(interesting_nodes[-1])
            _initialize_output(output)
            if interesting_nodes[0].expr_name == 'repo_source':
                # Import from a repository (local or remote)
                conn_string, mountpoint, tag = _parse_repo_source(interesting_nodes[0])

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
                    if conn_string:
                        clone(conn, conn_string, mountpoint, tmp_mountpoint, download_all=False)
                        source_hash = get_tagged_id(conn, tmp_mountpoint, tag)
                    else:
                        source_hash = get_tagged_id(conn, mountpoint, tag)
                    output_head = get_current_head(conn, output)
                    target_hash = _combine_hashes(
                        [output_head, source_hash] + [sha256(n.encode('utf-8')).hexdigest() for n in
                                                      table_names + table_aliases])

                    print('%s:%s -> %s' % (output, output_head[:12], target_hash[:12]))

                    def _calc():
                        print("Importing tables %r:%s from %s into %s" % (
                            table_names, source_hash[:12], mountpoint, output))
                        import_tables(conn, tmp_mountpoint if conn_string else mountpoint, table_names, output,
                                      table_aliases,
                                      image_hash=source_hash, target_hash=target_hash, table_queries=table_queries)

                    _checkout_or_calculate_layer(conn, output, target_hash, _calc)
                finally:
                    unmount(conn, tmp_mountpoint)
            else:
                # Extract the identifier (FDW name), the connection string and the FDW params (JSON-encoded, everything
                # between the single quotes).
                mount_nodes = _extract_nodes(interesting_nodes[0],
                                             ['identifier', 'no_db_conn_string', 'non_single_quote'])
                fdw_name = mount_nodes[0].match.group(0)
                conn_string = mount_nodes[1].match
                fdw_params = mount_nodes[2].match.group(0).replace("\\'", "'")  # Unescape the single quote

                mount_handler = get_mount_handler(fdw_name)
                tmp_mountpoint = fdw_name + '_tmp_staging'
                unmount(conn, tmp_mountpoint)
                try:
                    mount_handler(conn, server=conn_string.group(3), port=int(conn_string.group(4)),
                                  username=conn_string.group(1),
                                  password=conn_string.group(2),
                                  mountpoint=tmp_mountpoint, extra_options=json.loads(fdw_params))
                    # The foreign database is a moving target, so the new image hash is random.
                    # Maybe in the future, when the object hash is a function of its contents, we can be smarter here...
                    output_head = get_current_head(conn, output)
                    target_hash = "%0.2x" % getrandbits(256)
                    print('%s:%s -> %s' % (output, output_head[:12], target_hash[:12]))

                    import_tables(conn, tmp_mountpoint, table_names, output, table_aliases, image_hash=target_hash,
                                  foreign_tables=True, table_queries=table_queries)
                finally:
                    unmount(conn, tmp_mountpoint)

        elif node.expr_name == 'sql':
            _initialize_output(output)
            # Calculate the hash of the layer we are trying to create.
            # Since we handle the "input" hashing in the import step, we don't need to care about the sources here.
            # Later on, we could enhance the caching and base the hash of the command on the hashes of objects that
            # definitely go there as sources.
            sql_command = _canonicalize(_extract_nodes(node, ['non_newline'])[0].text)
            output_head = get_current_head(conn, output)
            target_hash = _combine_hashes([output_head, sha256(sql_command.encode('utf-8')).hexdigest()])

            print('%s:%s -> %s' % (output, output_head[:12], target_hash[:12]))

            def _calc():
                print("Executing SQL...")
                with conn.cursor() as cur:
                    # Make sure we'll record the actual change.
                    assert _replication_slot_exists(conn)
                    # Execute all queries against the output by default.
                    cur.execute("SET search_path TO %s", (output,))
                    cur.execute(sql_command)
                    cur.execute("SET search_path TO public")
                commit(conn, output, target_hash, comment=sql_command)

            _checkout_or_calculate_layer(conn, output, target_hash, _calc)


def _parse_repo_source(remote_repo_node):
    repo_nodes = _extract_nodes(remote_repo_node, ['conn_string', 'identifier'])
    if repo_nodes[0].expr_name == 'conn_string':
        conn_string = repo_nodes[0].match.group(0)
        mountpoint = repo_nodes[1].match.group(0)
    else:
        conn_string = None
        mountpoint = repo_nodes[0].match.group(0)
    # See if we got given a tag
    if len(repo_nodes) == 3:
        tag = repo_nodes[2].match.group(0)
    else:
        tag = 'latest'
    return conn_string, mountpoint, tag
