from splitgraph.core._drawing import render_tree
from splitgraph.splitfile.execution import execute_commands
from test.splitgraph.conftest import OUTPUT, load_splitfile


def test_drawing(pg_repo_local, mg_repo_local):
    # Doesn't really check anything, mostly used to make sure the tree drawing code doesn't throw.
    execute_commands(load_splitfile('import_local.splitfile'), output=OUTPUT)
    render_tree(OUTPUT)
