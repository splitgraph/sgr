from splitgraph.drawing import render_tree
from splitgraph.sgfile import execute_commands
from test.splitgraph.test_sgfile import _load_sgfile


def test_drawing(sg_pg_mg_conn):
    # Doesn't really check anything, mostly used to make sure the tree drawing code doesn't throw.
    execute_commands(sg_pg_mg_conn, _load_sgfile('import_local.sgfile'), output='output')
    render_tree(sg_pg_mg_conn, 'output')
