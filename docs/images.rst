====================================
Image management and version control
====================================

Creating images and committing
==============================

See :mod:`splitgraph.commands.commit`

`sg commit` creates a new commit hash (currently picked at random, unless explicitly specified by the SGFile executor or the user)
and records all changes to the tables in a given schema.

Implementation
--------------

  * If the table's schema (or primary keys) has changed (or the table has been created), copies the table into a
    new SNAP object (currently, the IDs of all objects are also picked at random). This kind of sidesteps the problem
    of storing column names and types in a DIFF object.
  * If the table hasn't changed at all, links the table (by adding entries to `splitgraph_meta.tables`) to all objects
    pointed to by the parent of this table.
  * Otherwise, goes through the `pending_changes` to conflate the changes and create a new DIFF object.

    * Replica identity is PG terminology for something identifying the exact tuple for the purposes of logical
      replication. In this case, the RI is either the PK of the tuple (if it exists) or the whole tuple.
    * Currently, DIFF objects are of the format `(the whole RI, change kind, change data)`:

      * Change kind is 0 for insert (then change data is a JSON of inserted column names and values), 1 for delete
        (change data is empty) or 2 for update (change data are the column names and their new values).
      * Conflation means that each RI is mapped to exactly one action (insert, update or delete), which means that
        changes in a single DIFF can be applied in any order (as long as all deletes are done before all inserts).
    * If there is an update in the audit log that changes the RI (user suspended constraint checking or the tuple had no
      PK and was updated), the update is changed into an insert + delete.
    * All changes are conflated using a straightforward algorithm in `splitgraph.objects.utils.conflate_changes`.
  * The meta tables this touches are `object_tree` (to register the new objects and link them to their parents),
    `tables` (to link tables in the new commit to existing/new objects), `snap_tree` (to register the new commit) and
    `snap_tags` (to move the HEAD pointer to the new commit).

Checking out commits and downloading objects
============================================

See :mod:`splitgraph.commands.checkout` for the detailed documentation.

`sg checkout` checks out a given commit into the schema, first deleting any uncommitted chances. Then,
every table in the given Splitgraph image is materialized (copied into the mountpoint as an actual table).

As a part of this process, extra physical objects that are required to materialize the image can be downloaded.

Implementation
--------------

  * The `tables` table is inspected to find out which object is required to start materializing the table.
  * Then, `object_tree` is crawled to find a chain of DIFF objects that ends with a SNAP
    (`splitgraph.pg_replication.get_closest_parent_snap_object`).
  * The SNAP is copied into the mountpoint and the DIFFs applied to it. Checkouts/repository clones are
    lazy by default, so an object might not even exist locally. The lookup path for a physical object is:

      * Search locally in the `splitgraph_meta` schema for a cached/predownloaded object.
      * Check the `object_locations` table and download the object from an external location

        * Currently, only `FILE` is supported: the object is dumped into a user-specified directory as an uncompressed
          SQL file.
      * Download the object from the upstream (by inspecting the `remotes` table).
  * `snap_tags` is changed to update the HEAD pointer.

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

Fetching and pushing repositories
=================================

Also see :mod:`splitgraph.commands.push_pull`

`sg pull` is currently a shorthand for `clone` that uses the remote name specified in `splitgraph_meta.remotes` instead of
a full connection string.

`sg clone` brings the metadata for the local mountpoint up to date with a remote one, optionally downloading the actual
physical objects.

`sg push` does the opposite.

`sg publish` TODO

Implementation
--------------

The `sg clone` command is implemented as follows:

  * First, it connects to the remote and inspects its `splitgraph_meta` table to gather the commits, tags and objects
    (`snap_tree`, `snap_tags`, `object_tree`, `tables` and `object_locations`) that don't exist in the local
    `splitgraph_meta`. See `splitgraph.commands.push_pull._get_required_snaps_objects`.
  * As part of that, it also crawls the remote `object_tree` to make sure it actually has the list of all required
    objects and their dependencies.
  * Optionally, it downloads the new objects and stores them in `splitgraph_meta`.
  * Finally, it writes the new metadata locally. Currently, it doesn't check for clashes or conflicts, instead
    letting the constraints on `splitgraph_meta` handle that. In particular:

    * Existing commits/objects aren't gathered at all by `_get_required_snaps_objects`, hence the remote can't rewrite
      local history.
    * Tags on existing commits can't be removed and if the same tag is given to another commit by the remote, it will
      cause a PK violation on the local schema.
    * Changes to existing object locations won't be reflected locally.

To fetch and update metadata, the local SG Python client initializes a direct connection to the remote driver (origin).
However, to actually download objects from the remote, the Python client gets the local SG driver to mount the remote
via FDW and use driver-to-driver `SELECT` queries to save some roundtrips between the client and the driver. This
doesn't happen in the case of objects stored externally: those dumps are fetched by the client and sent to the server
to be executed.

Pushes are very similar to pulls with reversed roles, since we are currently assuming that the client has equal access
rights to their local and the remote driver. This might not be the case in the future.

Currently, the only difference is that for uploading objects to the remote, the local client has to use its own
connection to create the tables that will house the objects remotely, then mount those tables on the local driver and
then use the driver-to-driver `SELECT` queries to send the object contents over. In the case of externally stored
objects, the client first uploads them to an external location and only then registers the new metadata (commits,
tags, objects and their locations) on the remote.