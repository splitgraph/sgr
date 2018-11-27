from splitgraph.drawing import render_tree
from splitgraph.sgfile.execution import execute_commands
from test.splitgraph.conftest import OUTPUT, load_sgfile


def test_drawing(sg_pg_mg_conn):
    # Doesn't really check anything, mostly used to make sure the tree drawing code doesn't throw.
    execute_commands(load_sgfile('import_local.sgfile'), output=OUTPUT)
    render_tree(OUTPUT)
