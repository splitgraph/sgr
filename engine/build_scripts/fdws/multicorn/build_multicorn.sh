#!/bin/bash -ex

cd /src/Multicorn

# Fork of official Multicorn with a few cherry-picked patches:
# * https://github.com/Kozea/Multicorn/pull/214   (pg11 build)
# * https://github.com/Kozea/Multicorn/issues/136  (plpython interaction)

export DESTDIR=/output/root
export PYTHON_OVERRIDE=python3.8
export PYTHON_CONFIG=x86_64-linux-gnu-python3.8-config

# Do "make CFLAGS=-DDEBUG install" instead to enable debug output for scans.
# From python 3.8, we have to call python3.8-config with --embed to get the
# flags for embedded python but I don't really want to change that makefile
# to keep it compiling on both Pythons.
make PY_ADDITIONAL_LIBS="-lpython3.8" install
