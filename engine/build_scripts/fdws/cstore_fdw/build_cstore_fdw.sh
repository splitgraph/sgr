#!/bin/bash -ex

cd /build
git clone https://github.com/citusdata/cstore_fdw.git
cd cstore_fdw
# Check out the current tip of "development" containing amongst other things
# a patch to make it possible to use cstore tables in parallel scans.
git checkout develop_v1x

# Apply a patch that changes some CStore behaviour to make the FDW independent from its data files
# (to let us hotswap them):
#
# * don't delete the actual physical files when a table or the database is dropped
# * don't recreate the footer and open the table file in append mode if it already exists
git apply ../fdw_fixes.patch

export DESTDIR=/output/root
make install
