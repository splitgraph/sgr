import re

from parsimonious import Grammar

from splitgraph.constants import SplitGraphException, to_repository

SGFILE_GRAMMAR = Grammar(r"""
    commands = space command space (newline space command space)*
    command = comment / import / from / sql_file / sql 
    comment = space "#" non_newline
    from = "FROM" space ("EMPTY" / repo_source) (space "AS" space repository)?
    import = "FROM" space source space "IMPORT" space tables
    sql_file = "SQL" space "FILE" space non_newline
    sql = "SQL" space sql_statement
    
    table = ((table_name / table_query) space "AS" space table_alias) / table_name
    
    table_query = "{" non_curly_brace "}"
    tables = "ALL" / (table space ("," space table)*)
    source = mount_source / repo_source
    repo_source = repository (":" tag_or_hash)?
    mount_source = "MOUNT" space handler space no_db_conn_string space handler_options
    
    image_hash = ~"[0-9a-f]*"i
    
    # TBH these ones that map to "identifier" aren't realy necessary since parsimonious calls those nodes
    # "identifier" anyway. This is so that the grammar is slightly more readable. 
    
    handler = identifier
    repository = ~"[_a-zA-Z0-9\-/]+"
    table_name = identifier
    table_alias = identifier
    tag_or_hash = identifier
    handler_options = "'" non_single_quote "'"
    sql_statement = non_newline
    
    newline = ~"\n*"
    non_newline = ~"[^\n]*"
    
    # I've no idea why we need so many slashes here. The purpose of this regex is to consume anything
    # that's not a closing curly brace or \} (an escaped curly brace).
    non_curly_brace = ~"(\\\\}|[^}])*"
    
    # Yeah, six slashes should be about enough to capture \'
    non_single_quote = ~"(\\\\\\'|[^'])*"
    
    no_db_conn_string = ~"(\S+):(\S+)@(.+):(\d+)"
    identifier = ~"[_a-zA-Z0-9\-]+"
    space = ~"\s*"
""")


def preprocess(commands, params=None):
    # Also replaces all $PARAM in the sgfile text with the params in the dictionary.
    if params is None:
        params = {}
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


def parse_commands(commands, params=None):
    # Unpacks the parse tree into a list of command nodes.
    if params is None:
        params = {}
    commands = preprocess(commands, params)
    parse_tree = SGFILE_GRAMMAR.parse(commands)
    return [n.children[0] for n in extract_nodes(parse_tree, ['command']) if n.children[0].expr_name != 'comment']


def extract_nodes(node, types):
    """Crawls the parse tree and only extracts nodes of given types. Doesn't crawl further down if it reaches a
    sought type."""
    if node.expr_name in types:
        return [node]
    result = []
    for child in node.children:
        result.extend(extract_nodes(child, types))
    return result


def get_first_or_none(node_list, node_type):
    """Gets the first node of type node_type from node_list, returns None if it doesn't exist."""
    for n in node_list:
        if n.expr_name == node_type:
            return n
    return None


def _parse_table_alias(table_node):
    """Extracts the table name (or a query forming the table) and its alias from the parse tree."""
    table_name_alias = extract_nodes(table_node, ['identifier', 'non_curly_brace'])
    table_name = table_name_alias[0].match.group(0)
    table_is_query = table_name_alias[0].expr_name == 'non_curly_brace'
    if table_is_query:
        # Unescape the closing curly brace that marked the end of the query
        table_name = table_name.replace('\\}', '}')
    if len(table_name_alias) > 1:
        table_alias = table_name_alias[1].match.group(0)
        return table_name, table_alias, table_is_query
    return table_name, table_name, table_is_query


def parse_repo_source(remote_repo_node):
    repo_nodes = extract_nodes(remote_repo_node, ['repository', 'identifier', 'image_hash'])
    repository = to_repository(repo_nodes[0].match.group(0))
    # See if we got given a tag / hash (the executor will try to interpret it as both).
    if len(repo_nodes) == 2:
        tag_or_hash = repo_nodes[1].match.group(0)
    else:
        tag_or_hash = 'latest'
    return repository, tag_or_hash


def extract_all_table_aliases(node):
    tables = extract_nodes(node, ['table'])
    if not tables:
        # No tables specified (imports all tables from the mounted db / repo)
        return [], [], []
    return zip(*[_parse_table_alias(table) for table in tables])
