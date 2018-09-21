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