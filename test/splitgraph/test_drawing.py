from splitgraph.commands._drawing import render_tree
from splitgraph.splitfile.execution import execute_commands
from test.splitgraph.conftest import OUTPUT, load_splitfile


def test_drawing(local_engine_with_pg_and_mg):
    # Doesn't really check anything, mostly used to make sure the tree drawing code doesn't throw.
    execute_commands(load_splitfile('import_local.splitfile'), output=OUTPUT)
    render_tree(OUTPUT)
