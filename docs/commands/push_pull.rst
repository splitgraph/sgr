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