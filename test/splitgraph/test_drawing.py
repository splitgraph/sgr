from splitgraph.drawing import render_tree
from splitgraph.sgfile import execute_commands
from test.splitgraph.test_sgfile import BASE_COMMANDS, sg_pg_mg_conn


def test_drawing(sg_pg_mg_conn):
    # Doesn't really check anything, mostly used to make sure the tree drawing code doesn't throw.
    execute_commands(sg_pg_mg_conn, BASE_COMMANDS[:5])
    render_tree(sg_pg_mg_conn, 'output')
