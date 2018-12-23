"""
    A framework for custom Splitfile commands. The execution flow is as follows:

      * When the Splitfile executor finds an unknown command, it looks for an entry in the config file::

          [commands]
          RUN=splitgraph.plugins.Run

      * The command class must extend this class, initialized at every invocation time.
      * The command's `calc_hash()` method is run. The resultant command context hash is combined with the current
        image hash to produce the new image hash: if it already exists, then the image is simply checked out.
      * Otherwise (or if calc_hash is undefined or returns None), `execute()`, where the actual command should be
        implemented, is run. If it returns a hash, this hash is used for the new image. If this hash already exists,
        the existing image is checked out instead. If the command returns None, a random hash is generated for the
        new image.
"""


class PluginCommand:
    """Base class for custom Splitfile commands."""

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

    def execute(self, repository, args):
        """
        Execute the custom command against the target schema, optionally returning the new image hash. The contract
        for the command is as follows (though it is not currently enforced by the runtime):

          * Has to use get_engine().run_sql (or run_sql_batch) to interact with the engine.
          * Can only write to the schema with the checked-out repository (run_sql runs non-schema-qualified
            statements against the correct schema).
          * Can inspect splitgraph_meta (e.g. to find the current HEAD) for the repository.
          * Can't alter the versioning of the repository.

        :param repository: SG Repository object pointed to a schema with the checked out image
            the command is being run against.
        :param args: Positional arguments to the command

        :return: Command context hash (a string of 64 hexadecimal digits). If calc_hash() had previously returned
            a hash, this hash is ignored. If both this command and calc_hash() return None, the hash is randomly
            generated.
        """
