from splitgraph.core._drawing import render_tree
from splitgraph.splitfile.execution import execute_commands, rebuild_image
from test.splitgraph.conftest import OUTPUT, load_splitfile


def test_drawing(pg_repo_local):
    # Doesn't really check anything, mostly used to make sure the tree drawing code doesn't throw.
    execute_commands(load_splitfile("import_local.splitfile"), output=OUTPUT)

    # Make another branch to check multi-branch repositories can render.
    pg_repo_local.images()[1].checkout()
    pg_repo_local.run_sql("INSERT INTO fruits VALUES (3, 'kiwi')")
    pg_repo_local.commit()

    rebuild_image(OUTPUT.head, {pg_repo_local: pg_repo_local.head.image_hash})

    render_tree(OUTPUT)
