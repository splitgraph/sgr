# TODO

  * Commits: add times and tags + (maybe) messages
  * Err on checkout if there are pending changes?
  * Diff: combine WAL diffs if on the same path, dump pending changes if HEAD -> staging
  * Somehow (?) record sgfiles in the snap_tree as well (multiple ways to materialize a given image)
  * Indirection on object location on push/pull:
    * ability to have an object URL/protocol when pulling from remote
    * push: only push diffs/sgfiles to remote. Allow uploading actual objects to a different location and pushing
      that to the remote instead.
  * Stretch goal: gathering object locations and metadata on pull to see which materialization strategy (copy an image,
    apply some sgfiles or some diffs) is better based on our known remotes.