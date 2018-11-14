class PluginCommand(object):
    """
    A framework for custom sgfile commands. The execution flow is as follows:

      * When the sgfile executor finds an unknown command, it looks for an entry in the config file:
          [commands]
          RUN=splitgraph.plugins.Run

      * The command class must extend this class. It's initialized at every invocation time
        with the psycopg connection object.
      * The command's `calc_hash()` method is run. The resultant command context hash is combined with the current
        image hash to produce the new image hash: if it already exists (or if calc_hash is undefined or returns None),
        then the image is simply checked out.
      * Otherwise, `execute()`, where the actual command should be implemented, is run. If it returns a hash,
        this hash is used for the new image. If this hash already exists, the existing image is checked out instead.
        If the command returns None, a random hash is generated for the new image.
    """

    def __init__(self, conn):
        self.conn = conn

    def calc_hash(self, repository, args):
        """
        Calculates the command context hash for this custom command. If either the command context hash or the
        previous image hash has changed, then the image hash produced by this command will change.
        Consequently, two commands with the same command context hashes are assumed to have the same effect
        on any Splitgraph images.

        This is supposed to be a lightweight method intended for pre-flight image hash calculations
        (without performing the actual transformation). If it returns None, the actual transformation is run anyway.

        For example, for a command that imports some data from an external URL, this could be the hash of the last
        modified timestamp provided by the external data vendor. If the timestamp is unchanged, the data is unchanged
        and so actual command won't be re-executed.

        :param repository: SG Repository object pointed to a schema with the checked out image
          the command is being run against.
        :param args: Positional arguments to the command
        :return: Command context hash (a string of 64 hexadecimal digits)
        """
        pass

    def execute(self, repository, args):
        """
        Execute the custom command against the target schema, optionally returning the new image hash. The contract
        for the command is as follows (though it is not currently enforced by the runtime):
          * Has to use self.conn for any interaction with the database.
          * Can only write to the schema with the checked-out repository (conn is already assumed to have its
            search_path set to the correct schema).
          * Can inspect splitgraph_meta (e.g. to find the current HEAD) for the repository.
          * Can't alter the versioning of the repository.

        :param repository: SG Repository object pointed to a schema with the checked out image
          the command is being run against.
        :param args: Positional arguments to the command
        :return: Command context hash (a string of 64 hexadecimal digits). If calc_hash() had previously returned
          a hash, this hash is ignored. If both this command and calc_hash() return None, the hash is randomly
          generated.
        """
        pass
