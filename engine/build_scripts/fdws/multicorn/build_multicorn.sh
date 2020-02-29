#!/bin/bash -ex

cd /src/Multicorn

# Fork of official Multicorn with a few cherry-picked patches:
# * https://github.com/Kozea/Multicorn/pull/214   (pg11 build)
# * https://github.com/Kozea/Multicorn/issues/136  (plpython interaction)

export DESTDIR=/output/root
export PYTHON_OVERRIDE=python3

# Do "make CFLAGS=-DDEBUG install" instead to enable debug output for scans.
make install
