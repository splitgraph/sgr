#!/bin/bash

git clone git://github.com/splitgraph/Multicorn.git
cd Multicorn

# Fork of official Multicorn with a few cherry-picked patches:
# * https://github.com/Kozea/Multicorn/pull/214   (pg11 build)
# * https://github.com/Kozea/Multicorn/issues/136  (plpython interaction)

# Do "make CFLAGS=-DDEBUG install" instead to enable debug output for scans.
PYTHON_OVERRIDE=python3 make install
