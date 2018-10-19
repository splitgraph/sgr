====================
Splitgraph internals
====================

This chapter contains various implementation details of Splitgraph that are not required for its operation if you're
a data scientist or a casual user. However, if you wish to fine tune your Splitgraph installation, contribute to
the source code or are just curious, read on!

The `splitgraph_meta` schema
============================

Most of the metadata related to Splitgraph is stored in the `splitgraph_meta` schema on the driver, including
version and tag information, relationships between images and downloaded tables.

Here's an overview of the tables in this schema:

  * `images`: Describes all image hashes and their parents, as well as extra
    data about a given commit (the creation timestamp, the commit message and the details of the sgfile command that
    generated this image). PKd on the mountpoint and the image hash, so the same image can exist in multiple schemas
    at the same time.
  * `tables`: an image consists of multiple tables. Each table in a given version is represented by one or more objects.
    An object can be one of two types: SNAP (a snapshot, a full copy of the table) and a DIFF (list of changes to a parent
    object). This is also mountpoint-specific.
  * `objects`: Lists the type and the parent of every object. A SNAP object doesn't have a parent and a DIFF object
    might have multiple parents (for example, the SNAP and the DIFF of a previous commit). This is not necessarily
    the object linked to the parent commit of a given object: if we're importing a table from a different repository,
    we would pull in its chain of DIFF objects without tying them to commits those objects were created in.
  * `remotes`: Currently, stores the connection string for the upstream repository a given repository was cloned from.
  * `snap_tags`: maps images and their mountpoints to one or more tags. Tags (apart from HEAD) are pushed and pulled
    to/from upstream repositories and are immutable (this is weakly enforced by the push/pull code).
    HEAD is a special tag: it points out to the currently checked-out local image.
  * `object_locations`: If a given object is not stored in the remote, this table specifies where to find it (protocol
    and location). More on this later.

Implementation of various Splitgraph commands
=============================================

`commit`
--------

  * If the table's schema (or primary keys) has changed (or the table has been created), copy the table into a
    new SNAP object (currently, the IDs of all objects are also picked at random). This kind of sidesteps the problem
    of storing column names and types in a DIFF object.
  * If the table hasn't changed at all, link the table (by adding entries to `splitgraph_meta.tables`) to all objects
    pointed to by the parent of this table.
  * Otherwise, goe through the `pending_changes` to conflate the changes and create a new DIFF object.

    * *Replica identity* is PG terminology for something identifying the exact tuple for the purposes of logical
      replication. In this case, the RI is either the PK of the tuple (if it exists) or the whole tuple.
    * Currently, DIFF objects are of the format `(the whole RI, change kind, change data)`:

      * Change kind is 0 for insert (then change data is a JSON of inserted column names and values), 1 for delete
        (change data is empty) or 2 for update (change data are the column names and their new values).
      * Conflation means that each RI is mapped to exactly one action (insert, update or delete), which means that
        changes in a single DIFF can be applied in any order (as long as all deletes are done before all inserts).
    * If there is an update in the audit log that changes the RI (user suspended constraint checking or the tuple had no
      PK and was updated), the update is changed into an insert + delete.
    * All changes are conflated using a straightforward algorithm in `splitgraph.objects.utils.conflate_changes`.
  * The meta tables this touches are `objects` (to register the new objects and link them to their parents),
    `tables` (to link tables in the new commit to existing/new objects), `images` (to register the new commit) and
    `snap_tags` (to move the HEAD pointer to the new commit).

`checkout`
----------

  * The `tables` table is inspected to find out which object is required to start materializing the table.
  * Then, `objects` is crawled to find a chain of DIFF objects that ends with a SNAP
    (`splitgraph.pg_replication.get_closest_parent_snap_object`).
  * The SNAP is copied into the mountpoint and the DIFFs applied to it. Checkouts/repository clones are
    lazy by default, so an object might not even exist locally. The lookup path for a physical object is:

      * Search locally in the `splitgraph_meta` schema for a cached/predownloaded object.
      * Check the `object_locations` table and download the object from an external location

        * Currently, only `FILE` is supported: the object is dumped into a user-specified directory as an uncompressed
          SQL file.
      * Download the object from the upstream (by inspecting the `remotes` table).
  * `snap_tags` is changed to update the HEAD pointer.

`clone/push/pull`
-----------------

`sgr clone` is implemented as follows:

  * First, it connect to the remote and inspect its `splitgraph_meta` table to gather the commits, tags and objects
    (`images`, `snap_tags`, `objects`, `tables` and `object_locations`) that don't exist in the local
    `splitgraph_meta`. See `splitgraph.commands.push_pull._get_required_snaps_objects`.
  * As part of that, also crawl the remote `objects` to gather the list of all required objects
    and their dependencies.
  * Optionally, download the new objects and store them in `splitgraph_meta`.
  * Finally, write the new metadata locally. Currently, this command doesn't check for clashes or conflicts, instead
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

`import`
---------

  * Add the new commit into `images`
  * Copy the required rows from `tables` linking the required objects to the new commit (both the tables in the
    current HEAD and the newly imported tables).
  * Change the HEAD pointer to point to the new commit and optionally materialize the new tables (which might involve
    actual object downloads).
