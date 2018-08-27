# TODO
  * consider a command that re-derives a dataset based on its sgfile tree?
  * think hard about the repo lookup paths so we don't need to store origin details in sgfiles.
  * Err on checkout if there are pending changes?
  * Somehow (?) record sgfiles/image hashes things were imported from in the snap_tree (?) as well
    (multiple ways to materialize a given image).
    * Advanced hash calculation for IMPORT/SQL layers (only invalidate if the actual table objects the IMPORT
      / SQL layer relies on has changed.
  * Schema changes: come up with a better method of keeping track than producing a snap
  * Object location indirection: actually test HTTP or replace with a different upload mechanism.
  * Stretch goal: gathering object locations and metadata on pull to see which materialization strategy (copy an image,
    apply some sgfiles or some diffs) is better based on our known remotes.
  * Add logging instead of print statements?
