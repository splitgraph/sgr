# TODO
  * Sanitize provenance on push to registries that aren't aware of the repos we depend on?
  * Physical rebase (apply the DIFF objects to a different branch)
  * Advanced hash calculation for IMPORT/SQL layers (only invalidate if the actual table objects the IMPORT
      / SQL layer relies on have changed).
  * Schema changes: come up with a better method of keeping track than producing a snap
  * Object location indirection: actually test HTTP or replace with a different upload mechanism.
  * Err on checkout if there are pending changes?
  * Stretch goal: gathering object locations and metadata on pull to see which materialization strategy (copy an image,
    apply some sgfiles or some diffs) is better based on our known remotes.
