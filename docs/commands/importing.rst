Importing tables across repositories
====================================

`sg import SOURCE_MOUNTPOINT SOURCE_TABLE TARGET_MOUNTPOINT [TARGET_TABLE] [SOURCE_IMAGE_OR_TAG]`
    Grafts one or more tables from one mountpoint into another, creating a new single commit on top of the current HEAD.
    This doesn't explicitly preserve the imported tables' history. If the new table(s) isn't/aren't materialized, this
    doesn't consume extra space apart from the new entries in the metadata tables. It also doesn't discard any pending
    changes.

The API version of this function in :mod:`splitgraph.commands.importing` also allows more sophisticated importing,
such as importing only some data from the source table or simply copying the table over without its physical history.

This command usually doesn't need to used directly and is instead called by the :ref:`SGFile interpeter<sgfile>`.

Implementation
--------------

  * Adds the new commit into `snap_tree`
  * Copies the required rows from `tables` linking the required objects to the new commit (both the tables in the
    current HEAD and the newly imported tables).
  * Changes the HEAD pointer to point to the new commit and optionally materializes the new tables (which might involve
    actual object downloads).
