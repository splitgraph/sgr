# TODO

  * Commits: add times + (maybe) messages
  * Tags: checkout by tag, refine the UI maybe (like sg tag mountpoint just tags the head)
  * Err on checkout if there are pending changes?
  * Somehow (?) record sgfiles in the snap_tree (?) as well (multiple ways to materialize a given image).
    * Is it an issue that an image can now have several parents of different types? Its commit tree parent,
      the previous SQL statement (+ source images) in an sgfile that made it and a link to the whole sgfile
      + the original source images.
  * Record schema changes in the DIFF table
  * Figure out a better diff format for both the actual table and displaying it from sg diff
  * Indirection on object location on push/pull:
    * ability to have an object URL/protocol when pulling from remote
    * when we pull the catalog from remote, we don't actually fetch the objects: we only lazily do it on
      checkout. Maybe have an extra table mapping object IDs to their actual locations (and maybe
      then we can put the object formats there, too).
    * push: only push diffs/sgfiles to remote. Allow uploading actual objects to a different location and pushing
      that to the remote instead.
  * Stretch goal: gathering object locations and metadata on pull to see which materialization strategy (copy an image,
    apply some sgfiles or some diffs) is better based on our known remotes.
  * Add logging instead of print statements?
