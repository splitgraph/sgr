"""
Various hooks for extending Splitgraph, including:

  * External object handlers (:mod:`splitgraph.hooks.external_objects`) allowing to download/upload objects
    to locations other than the remote Splitgraph engine.
  * Data sources (:mod:`splitgraph.hooks.data_sources`) that allow to add data to Splitgraph, e.g.
   using the Postgres engine's FDW interface to mount other external databases on the engine.
  * Splitfile commands (:mod:`splitgraph.hooks.splitfile_commands`) to define custom data transformation steps
    compatible with the Splitfile framework.
"""
