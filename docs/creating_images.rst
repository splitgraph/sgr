===============
Creating images
===============

Importing tables across repositories
====================================

`sg import SOURCE_MOUNTPOINT SOURCE_TABLE TARGET_MOUNTPOINT [TARGET_TABLE] [SOURCE_IMAGE_OR_TAG]`
    Grafts one or more tables from one mountpoint into another, creating a new single commit on top of the current HEAD.
    This doesn't explicitly preserve the imported tables' history. If the new table(s) isn't/aren't materialized, this
    doesn't consume extra space apart from the new entries in the metadata tables. It also doesn't discard any pending
    changes.

The API version of this function in :mod:`splitgraph.commands.importing` also allows more sophisticated importing,
such as importing only some data from the source table or simply copying the table over without its physical history.

This command usually doesn't need to used directly and is instead called by the :ref:`SGFile interpreter<sgfile>`.

Implementation
--------------

  * Adds the new commit into `snap_tree`
  * Copies the required rows from `tables` linking the required objects to the new commit (both the tables in the
    current HEAD and the newly imported tables).
  * Changes the HEAD pointer to point to the new commit and optionally materializes the new tables (which might involve
    actual object downloads).

Mounting foreign databases
==========================

See also :mod:`splitgraph.commands.mounting`.

`sg mount` uses the Postgres FDW to mount a foreign Postgres/Mongo database as a set of tables into a temporary location
and then imports those tables into the target mountpoint as a new Splitgraph image.

On the other hand, `sg unmount` destroys the local copy of a repository and all the metadata related to it in
`snap_tree`, `tables`, `remotes` and `snap_tags`. This command doesn't delete the actual physical objects in
`splitgraph_meta` or references to them in
`object_tree` / `object_locations`. There's a separate function, `sg cleanup`
(or :func:`splitgraph.commands.misc.cleanup_objects`) that crawls the `splitgraph_meta` for objects not required
by a current mountpoint and does that.

Custom mount handlers
---------------------

:mod:`splitgraph.commands.mount_handlers`

It is possible to write custom mount handlers for Splitgraph that create foreign tables using the Postgres FDW. A
mount handler is a function that takes a Psycopg connection object, the target schema and any handler-specific
keyword arguments that are passed directly from the user to the handler.

For an example, see the Postgres mount handler in :func:`splitgraph.commands.mount_handlers.mount_postgres`