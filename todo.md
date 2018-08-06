# TODO

  * Commits: add times + (maybe) messages
  * Tags: checkout by tag, refine the UI maybe (like sg tag mountpoint just tags the head)
  * Err on checkout if there are pending changes?
  * diff command: aggregate
  * checkout slowness: investigate
    * pks? delete statements? insert statements?
    * make a quick script to insert N rows into the table and do come commits/explains

  * Somehow (?) record sgfiles in the snap_tree (?) as well (multiple ways to materialize a given image).
    * Is it an issue that an image can now have several parents of different types? Its commit tree parent,
      the previous SQL statement (+ source images) in an sgfile that made it and a link to the whole sgfile
      + the original source images.
  * Record schema changes in the DIFF table
  * Figure out a better diff format for both the actual table and displaying it from sg diff
  * Indirection on object location on push:
      * push: only push diff meta/sgfiles to remote. Allow uploading actual objects to a different location and pushing
          that to the remote instead (EC2 python client + envvars with AWS id/key)
  * Pull: On checkout, allow downloading from different locations other than the origin.
  * Stretch goal: gathering object locations and metadata on pull to see which materialization strategy (copy an image,
    apply some sgfiles or some diffs) is better based on our known remotes.
  * Add logging instead of print statements?
