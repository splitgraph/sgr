from splitgraph.commands.checkout import checkout
from splitgraph.commands.mounting import mount
from splitgraph.commands.commit import commit
from splitgraph.commands.importing import import_tables
from splitgraph.commands.diff import diff
from splitgraph.commands.push_pull import push, pull, clone
from splitgraph.commands.misc import get_parent_children, get_log, init, unmount

# Commands to import a foreign schema locally, with version control.
# Tables that are created in the local schema:
# <table>_origin      -- an FDW table that is directly connected to the origin table
# <table>             -- the current staging table/HEAD -- gets overwritten every time we check out a commit.
# <object_id>         -- "git objects" that are either table snapshots or diffs. Diffs use the JSON format and
#                     -- are populated by dumping the WAL.

