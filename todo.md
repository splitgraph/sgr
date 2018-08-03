# TODO

  * Commits: add times and tags + (maybe) messages
  * Single WAL consumption slot, write back to splitgraph_meta.pending_changes on various commands. Flow:
    * Commit: move changes from the pending table to the diff table
    * Checkout: err if pending changes, if force, delete pending changes to the schema
    * Diff: if HEAD -> staging, dump the pending changes table.
  * Disallow storing non-base images (with parents) as only snaps (OK if snap is an extra to speed up checkouts). Maybe
    snap_tree should be called img_tree.
  * Diff: combine WAL diffs if on the same path.
  * Somehow (?) record sgfiles in the snap_tree as well (multiple ways to materialize a given image)
  * Indirection on object location on push/pull:
    * ability to have an object URL/protocol when pulling from remote
    * push: only push diffs/sgfiles to remote. Allow uploading actual objects to a different location and pushing
      that to the remote instead.
  * Stretch goal: gathering object locations and metadata on pull to see which materialization strategy (copy an image,
    apply some sgfiles or some diffs) is better based on our known remotes.