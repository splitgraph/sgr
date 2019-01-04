"""
Splitfile interpreter: a set of tools on top of the core Splitgraph versioning and image management to give
the user a Dockerfile-like experience for building Splitgraph images (caching, consistent hashing, a declarative
language).
"""

from splitgraph.splitfile.execution import execute_commands, rebuild_image
