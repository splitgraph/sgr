# Splitgraph prototype: schema, terminology, commands

Good luck, you'll need it.

## Schema and terminology

All Splitgraph-specific data and metadata is located in `splitgraph_meta`, including locally cached objects/images
and details on commits, tags, objects etc. The full schema is in `splitgraph.meta_handler._create_metadata_schema`.

Here's an overview:

  * `snap_tree`: should really be called `image_tree`. Describes all image hashes and their parents, as well as extra
    data about a given commit (the creation timestamp and the commit message). PKd on the mountpoint and the image hash,
    so the same image can exist in multiple schemas at the same time.
  * `tables`: an image consists of multiple tables. Each table in a given version is represented by one or more objects.
    An object can be one of two types: SNAP (a snapshot, a full copy of the table) and a DIFF (list of changes to a parent
    object). This is also mountpoint-specific.
  * `object_tree`: Lists the type and the parent of every object. A SNAP object doesn't have a parent and a DIFF object
    might have multiple parents (for example, the SNAP and the DIFF of a previous commit). This is not necessarily
    the object linked to the parent commit of a given object: if we're importing a table from a different repository,
    we would pull in its chain of DIFF objects without tying them to commits those objects were created in.
  * `remotes`: Currently, stores the connection string for the upstream repository a given repository was cloned from.
  * `snap_tags`: maps images and their mountpoints to one or more tags. Tags (apart from HEAD) are pushed and pulled
    to/from upstream repositories and are immutable (this is weakly enforced by the push/pull code).
    HEAD is a special tag: it points out to the currently checked-out local image.
  * `object_locations`: If a given object is not stored in the remote, this table specifies where to find it (protocol
    and location). More on this later.
  * `pending_changes`: Changes to tracked mountpoints and tables are consumed from the WAL and stored here when certain
    SG commands are run.

## Operations and implementation overview

### Commit

`splitgraph.commands.commit` and `splitgraph.pg_replication`.

Creates a new commit hash (currently picked at random, unless explicitly specified by the SGFile executor or the user)
and records all changes to the tables in a given mountpoint:

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
    * If there is an update in the WAL that changes the RI (user suspended constraint checking or the tuple had no
      PK and was updated), the update is changed into an insert + delete.
    * All changes are conflated using a straightforward algorithm in `splitgraph.pg_replication._conflate_changes`.
  * The meta tables this touches are `object_tree` (to register the new objects and link them to their parents),
    `tables` (to link tables in the new commit to existing/new objects), `snap_tree` (to register the new commit) and
    `snap_tags` (to move the HEAD pointer to the new commit).

### Checkout

`splitgraph.commands.checkout` and `splitgraph.commands.object_loading`

Checks out a given commit into the mountpoint. All uncommitted changes are deleted. After that, every table at a given
revision is materialized (copied into the mountpoint as an actual table).

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

### `sg status / log / show / diff`

`splitgraph.commands.misc` and `splitgraph.commands.diff`

Various commands that inspect the state of a mountpoint by getting data from the meta tables. The verbose mode of
`show` lists all the actual objects a given commit depends on. `sg diff` is currently only used from the commandline and
shows the difference between two images in a mountpoint. If the two images are on the same path in `snap_tree`, it
concatenates their DIFFs and displays that (or the aggregation of total inserts/deletes/updates): note this might give
wrong results of there's been a schema change. Otherwise (if the images are on different branches), it temporarily
materializes both revisions and compares those row-by-row.

### Import

`splitgraph.commands.importing`

Grafts one or more tables from one mountpoint into another, creating a new single commit on top of the current HEAD.
This doesn't explicitly preserve the imported tables' history. If the new table(s) isn't/aren't materialized, this
doesn't consume extra space apart from the new entries in the metadata tables. It also doesn't discard any pending
changes.

  * Adds the new commit into `snap_tree`
  * Copies the required rows from `tables` linking the required objects to the new commit (both the tables in the
    current HEAD and the newly imported tables).
  * Changes the HEAD pointer to point to the new commit and optionally materializes the new tables (which might involve
    actual object downloads).

This function has an optional `foreign_tables` flag that treats the imported tables as normal Postgres tables and not
SplitGraph-controlled ones, meaning that it literally copies the whole table(s) over into the new SNAP object(s). This
is useful for adding brand new tables (for example, from an FDW-mounted table).

There's another helper function for importing from remote repositories (`import_table_from_unmounted`). Currently it has
a naive implementation of cloning the remote repository (just the metadata without materializing anything), calling
the former function to do the import and then destroying the clone.

### Mount/unmount

`splitgraph.commands.mounting`

Uses the Postgres FDW to mount a foreign Postgres/Mongo database as a set of tables into a temporary location and then
imports those tables into the target mountpoint as a snapshot. This function used to concern itself with both producing
commands to create the foreign tables and snapshotting them, but the snapshotting/importing functionality has been
moved to `splitgraph.commands.importing`. 

Unmounting destroys the schema and all mountpoint-related information in `snap_tree`, `tables`, `remotes` and `snap_tags`.
It doesn't delete the actual physical objects in `splitgraph_meta` or references to them in
`object_tree` / `object_locations`. There's a separate function, `splitgraph.commands.misc.cleanup_objects`
that crawls the `splitgraph_meta` for objects not required by a current mountpoint and does that.

### Push/pull/clone

`splitgraph.commands.push_pull` and `splitgraph.commands.object_loading`

`pull` is currently a shorthand for `clone` that uses the remote name specified in `splitgraph_meta.remotes` instead of
a full connection string.

`clone` brings the metadata for the local mountpoint up to date with a remote one, optionally downloading the actual
physical objects.

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

Technically, this could be done a lot easier by simply cloning the remote metadata tables and replacing the data in the
local ones with that, however if we have the code that explicitly enumerates things we are about to insert, we'll be
able to put some extra logic there later.

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

## SGFile execution

`splitgraph.sgfile`

There's a couple of example SGFiles in `architecture/data/pgclient/` that show most of the things the current SGFile
executor can do. Similar to a Dockerfile, each command produces a new commit with a deterministic hash that depends
on the current hash and the particulars of a command that's being executed.

The SGFile is parsed using parsimonious, a Python parser library. The exact grammar is in
`splitgraph.sgfile.SGFILE_GRAMMAR` and there is some quality-of-life preprocessing that gets done to the file before
it's interpreted:

  * Newlines can be escaped to make a command multiline (`"\\n"` gets replaced with `""`)
  * Parameters are supported (`${PARAM}` is replaced with the value of the parameter that's either passed
    to the `execute_commands` as a dict or to the commandline `sg file` as a series of arguments
    (`-a key1 val1 -a key2 val2...`)).

There are currently only 3 commands supported by the interpreter:

### `OUTPUT mountpoint [hash]`

Sets the PG default schema search path to a given mountpoint, checks out the hash in order to base new hash calculations
 / image commits off of it.

As discussed, we'll probably be replacing it with an explicit parameter passed to the executor. However, this
currently allows for pseudo-multistage builds by changing the OUTPUT midway through the file and doing an `IMPORT`
from the just-built output.

### `FROM ([remote URL] mountpoint[:tag])/(MOUNT handler conn_string handler_options) IMPORT table1/{query1} [AS table1_alias], [table2/{query2}...]`

Uses the `sg import` command to import one or more tables from either a local mountpoint, a remote one, or an
FDW-mounted database.

Optionally, the table name can be replaced with a SELECT query in curly braces that will get executed against the
source mountpoint in order to create a table. This will be stored as a snapshot. For example:

`FROM internal_data:latest IMPORT {SELECT name, age FROM staff WHERE is_restricted = FALSE} AS visible_staff`

will create a new table that contains non-restricted staff names and ages in `internal_data.staff` without including
any other entries in the table history.

In the case of imports from FDW, the commit hash produced by this command is random. Otherwise, the commit hash will be
a combination of the current `OUTPUT` hash, the hash of the source mountpoint and the hashes of the names
(or source SQL queries) and aliases of all imported tables.

This is crude, but means that the layer is invalidated if there's a change on the remote or we import a different
table/name it differently/use a different query to create a table.  We can improve on this by perhaps only considering
the objects and table aliases that are actually imported (as opposed to the source image hash: maybe the tables
we're importing haven't changed even if other parts of the mountpoint have).

### `SQL command`

Runs a (potentially arbitrary) SQL statement. Doesn't enforce any constraints on the SQL yet, but the spirit of this
command is performing actions on tables in the current `OUTPUT` mountpoint (the command is executed with the `OUTPUT`
schema being the default one) and not changing/reading data from any other schemas.

The image hash produced by this command is a combination of the current `OUTPUT` hash and the hash of the
"canonicalized" SQL statement (all lowercase with excess whitespace removed). This might cause issues with us not
invalidating layers when an identifier inside a command changed case: then the results would change but the image
returned by the command would still have the same hash.

In the future, it might be worth basing the new hash on the hash of the objects that the query actually interacts with
(as inputs or outputs), but this will require actually parsing the query.
