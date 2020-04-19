#!/bin/bash -ex

cd /src/Multicorn

# Fork of official Multicorn with a few cherry-picked patches:
# * https://github.com/Kozea/Multicorn/pull/214   (pg11 build)
# * https://github.com/Kozea/Multicorn/issues/136  (plpython interaction)
# * scanning directly through cstore_fdw fragments rather than
#   passing data through python

export DESTDIR=/output/root
export PYTHON_OVERRIDE=python3.8
export PYTHON_CONFIG=x86_64-linux-gnu-python3.8-config

# Do "make CFLAGS=-DDEBUG install" instead to enable debug output for scans.
# Include and dynamically link to cstore_fdw
make \
  CPPFLAGS="-I ../cstore_fdw" \
  SHLIB_LINK="-L/output/root/usr/local/lib -lcstore_fdw -lpython3.8" \
  install
