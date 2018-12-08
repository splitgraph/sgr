#!/bin/bash

# We expect the splitgraph source code to be in a volume at /src
# Copy it to /splitgraph/ so any file edits (or volume mounts)
# in the directory do do not persist to the host.
# (For example, we mount .sgconfig as a volume at /splitgraph/.sgconfig
# ... if we operarted on /src then .sgconfig would appear in the host)
#

mkdir -p /splitgraph
cp -r /src/* /splitgraph

cd /splitgraph

nothing_to_install() {
    poetry install --dry-run | grep -i 'nothing to install' >/dev/null && return 0
    return 1
}

nothing_to_install || poetry install

exec poetry run sgr "$@"
