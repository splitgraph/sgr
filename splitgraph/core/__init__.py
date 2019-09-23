"""Core Splitgraph functionality: versioning and sharing tables.

The main point of interaction with the Splitgraph API is a :class:`splitgraph.core.repository.Repository` object
representing a local or a remote Splitgraph repository. Repositories can be created using one of the
following methods:

  * Directly by invoking `Repository(namespace, name, engine)` where `engine` is the engine that the repository
    belongs to (that can be gotten with `get_engine(engine_name)`. If the created repository doesn't actually exist
    on the engine, it must first be initialized with `repository.init()`.
  * By using :func:`splitgraph.core.engine.lookup_repository` which will search for the repository on the current
    lookup path.
"""
