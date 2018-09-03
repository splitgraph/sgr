# TODO
  * Repository lookup paths: store settings for one or more remote registries. They might be overridden by
    the locally stored images (wrt looking up the repository namespace/name -> images belonging to it).
  * Store provenance in snap_tree:
    * If made by an sgfile:
      * SQL command stored directly since it's only based on the previous image (unless there's an extra dependency)
      * IMPORT stored as original image + list of tables and aliases. Main issue is somehow converting that image hash
        to an actual location (mounted or remote) in order to rerun the sgfile + the original repository if we want
        to rederive the dataset (rebase it).
      * No need to store FROM since it just bases derivations on a different image.
    * If made by `sg sql`:
      * Either don't do it at all (then we still maintain the invariant of `sg sql` is just a shorthand for querying the 
        database directly) or store the command applied to the schema. Need to somehow know which schema
        the command is being applied to.
    * Allows us to have a command that rederives one dataset (rerunning all of the sgfile commands) based on the
      newest/tagged version of the things it depends on.
    * There can be two types of rebases: logical (reapplies the sgfile derivation to a different set of source datasets)
      and physical (reapplies the DIFF objects to a different parent. Not doable if we don't have a chain of DIFFS
      that we can apply (e.g. there are SNAP objects in the path)).
  * There's a possibility we don't need to actually store the downloaded objects locally once we've materialized them,
    since if we're deriving something based on that we're never checking out that version again.
      * If we store diffs so that we can reverse them, it's easier since then we can just travel the diff tree
        up and down by applying/unapplying diffs.
  * Advanced hash calculation for IMPORT/SQL layers (only invalidate if the actual table objects the IMPORT
      / SQL layer relies on have changed).
  * Schema changes: come up with a better method of keeping track than producing a snap
  * Object location indirection: actually test HTTP or replace with a different upload mechanism.
  * Err on checkout if there are pending changes?
  * Stretch goal: gathering object locations and metadata on pull to see which materialization strategy (copy an image,
    apply some sgfiles or some diffs) is better based on our known remotes.
