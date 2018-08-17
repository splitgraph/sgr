from hashlib import sha256

from splitgraph.commands import init, commit, pull, checkout, import_tables
from splitgraph.constants import SplitGraphException
from splitgraph.meta_handler import get_current_head, get_canonical_snap_id, get_all_snap_parents

from parsimonious.grammar import Grammar

SGFILE_GRAMMAR = Grammar(r"""
    commands = command space (newline space command)*
    command = comment / output / import_local / import_remote / sql
    comment = space "#" !newline
    output = "OUTPUT" space mountpoint space image_hash?
    import_local = "FROM" space mountpoint space "IMPORT" space tables
    table = (table_name space "AS" space table_alias) / table_name
    tables = table space ("," space table)*
    import_remote = "FROM" space conn_string space "/" space mountpoint space (":" space tag)? space "IMPORT" space tables
    sql = "SQL" space sql_statement
    
    image_hash = ~"[0-9a-f]*"i
    mountpoint = identifier
    table_name = identifier
    table_alias = identifier
    tag = identifier
    sql_statement = !newline
    
    newline = ~"\n+"
    conn_string = ~"\S+:\S+@.+:\d+"
    identifier = ~"[_a-zA-Z0-9\-]*"
    space = ~"\s*"
""")

def _canonicalize(sql):
    return ' '.join(sql.lower().split())


def _combine_hashes(hashes):
    return sha256(''.join(hashes).encode('ascii')).hexdigest()


def parse_commands(commands):
    # Unpacks the parse tree into a list of command nodes.
    parse_tree = SGFILE_GRAMMAR.parse(commands)
    commands = []
    node = parse_tree.children[0]
    if node.children[0].expr_name != 'comment':
        commands.append(node.children[0])
    # Visit the one-or-many node
    for node in parse_tree.children[2].children:
        if node.expr_name != 'command':
            continue
        if node.children[0].expr_name == 'comment':
            continue
        commands.append(node.children[0])
    return commands


def _parse_table_alias(table_node):
    # Extracts the table name and its alias from the parse tree.
    actual_node = table_node.children[0].children[0]
    table_name = actual_node.children[0].match(0)
    if len(actual_node.children) > 1:
        table_alias = actual_node.children[-1].match(0)
        return table_name, table_alias
    return table_name, table_name


def execute_commands(conn, commands):
    sources = {}
    output = None

    node_list = parse_commands(commands)
    for i, node in enumerate(node_list):

        print("-> %d/%d %s" % (i + 1, len(commands), node.full_text))
        if node.expr_name == 'output':
            # Slightly weird tree traversal here
            output = node.children[2].match.group(0)
            print("Committing results to mountpoint %s" % output)
            try:
                get_current_head(conn, output)
            except SplitGraphException:
                # Output doesn't exist, create it.
                init(conn, output)

            # By default, use the current output HEAD. Check if it's overridden.
            if len(node.children) > 2:
                output_head = get_canonical_snap_id(conn, output, node.children[4].children[0].match.group(0))
                checkout(conn, output, output_head)
        elif node.expr_name == 'import_local':
            mountpoint = node.children[2].match.group(0)
            tables = node.children[-1]

            table_names = []
            table_aliases = []

            tn, ta = _parse_table_alias(tables.children[0])
            table_names.append(tn)
            table_aliases.append(ta)
            for table_node in tables.children[-1].children:
                if table_node.expr_name != 'table':
                    continue
            import_tables(conn, mountpoint, table_names, output, table_aliases)

        elif node.expr_name == 'import_remote':
            pass
        elif node.expr_name == 'sql':
            if output is None:
                raise SplitGraphException("Error: no OUTPUT specified. OUTPUT mountpoint is snapshot during file execution.")
            # Calculate the hash of the layer we are trying to create.
            # It's the xor of all input + output layer hashes as well as the hash of the SQL command itself.
            sql_command = _canonicalize(operands[0])
            output_head = get_current_head(conn, output)
            target_hash = _combine_hashes(list(sources.values()) + [output_head, sha256(sql_command.encode('utf-8')).hexdigest()])

            print(', '.join('%s:%s' % (mp, si[:12]) for mp, si in iter(sources.items())) + ', %s:%s' % (output, output_head[:12]) + \
                ' -> %s:%s' % (output, target_hash[:12]))

            # Have we already calculated this hash?
            try:
                checkout(conn, output, target_hash)
                print("Using the cache.")
            except SplitGraphException:
                # Check out all the input layers
                for mountpoint, snap_id in sources.items():
                    print("Using %s snap %s" % (mountpoint, snap_id[:12]))
                    checkout(conn, mountpoint, snap_id)
                print("Executing SQL...")
                with conn.cursor() as cur:
                    cur.execute(sql_command)

                commit(conn, output, target_hash, comment=sql_command)
                conn.commit()
