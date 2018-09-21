Checking out commits and downloading objects
============================================

See :mod:`splitgraph.commands.checkout` for the detailed documentation.

`sg checkout` checks out a given commit into the schema, first deleting any uncommitted chances. Then,
every table in the given Splitgrpah image is materialized (copied into the mountpoint as an actual table).

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
