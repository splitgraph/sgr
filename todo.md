# TODO
  * Importing:
    * Importing from yet unmounted/unpulled repositories/databases:
      * Unmounted DB: mount (no copying), copy + commit, destroy (optional)
    * sgfile command: parsing/execution.
  * More hardcore push/pull tests (run an sgfile that imports from the snapper, push back to it, destroy
    the world, pull and make sure we still can materialize everything).
  * Err on checkout if there are pending changes?
  * Somehow (?) record sgfiles in the snap_tree (?) as well (multiple ways to materialize a given image).
    * Is it an issue that an image can now have several parents of different types? Its commit tree parent,
      the previous SQL statement (+ source images) in an sgfile that made it and a link to the whole sgfile as well as
      the original source images.
    * somehow add the sources to the commit message as well?
    * maybe parse the query (not just from sgfile) to see which mountpoints it touches (both read and write) in order
      to know when to invalidate and which actual mountpoint to commit.
    * image hashes are actually globally unique. maybe we just replace the schema qualifiers with them somehow? is it
      sufficient to just have the image hashes there and then do a search across the driver for which mountpoint
      it's actually on?
  * Schema changes: come up with a better method of keeping track than producing a snap
  * Object location indirection: actually test HTTP or replace with a different upload mechanism.
  * Stretch goal: gathering object locations and metadata on pull to see which materialization strategy (copy an image,
    apply some sgfiles or some diffs) is better based on our known remotes.
  * Add logging instead of print statements?
