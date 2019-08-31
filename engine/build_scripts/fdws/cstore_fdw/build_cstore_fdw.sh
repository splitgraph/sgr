#!/bin/bash

git clone https://github.com/citusdata/cstore_fdw.git
cd cstore_fdw
# Check out the current tip of "development" containing amongst other things
# a patch to make it possible to use cstore tables in parallel scans.
git checkout develop_v1x

# Apply a patch that disables CStore deleting the actual physical files when a table
# or the database is dropped (in case we spin up an engine to query CStore files directly).
git apply /build_scripts/fdws/cstore_fdw/do_not_delete_objects_on_drop.patch

make
make install