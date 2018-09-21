Inspecting the status of a repository
=====================================

There are various (commandline and API) commands that can be used to inspect the status of a repository.

The commandline commands are listed here, together with their API counterparts. :mod:`splitgraph.meta_handler` contains
more low-level commands that fetch data directly from the metadata tables without processing it.

`sg show MOUNTPOINT IMAGE_HASH`
    Outputs the information about a given image. The verbose mode (`-v`) also lists all the actual objects
    the image depends on.

`sg diff MOUNTPOINT IMAGE_HASH_1 [IMAGE_HASH_2]`
    Also see: :mod:`splitgraph.commands.diff`

    Shows the difference between two images in a mountpoint. If the two images are on the same path in `snap_tree`, it
    concatenates their DIFFs and displays that (or the aggregation of total inserts/deletes/updates).
    Note this might give wrong results if there's been a schema change.

    If the images are on different branches), it temporarily materializes both revisions and compares them row-by-row.

`sg log MOUNTPOINT`
    Also see: :func:`splitgraph.commands.misc.get_log`

    Returns the log of changes to a given mountpoint, starting from the current HEAD revision and crawling down.
    If `--tree` (`-t`) is passed, outputs the full image tree of the schema.
    Otherwise, and if nothing in the mountpoint is checked out, raises an error.

`sg status`
    Lists the currently mounted schemata and their checked out images (if any).
