#!/bin/bash

git clone https://github.com/citusdata/cstore_fdw.git
cd cstore_fdw
# Check out the current tip of "development" containing amongst other things
# a patch to make it possible to use cstore tables in parallel scans.
git checkout develop_v1x

make
make install