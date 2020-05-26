#!/bin/bash -ex

cd /src/Multicorn

# Fork of official Multicorn with the ability to scan directly through cstore_fdw
# fragments rather than passing data through python.

export DESTDIR=/output/root
export PYTHON_OVERRIDE=python3.7
export PYTHON_CONFIG=x86_64-linux-gnu-python3.7-config

# Do "make CFLAGS=-DDEBUG install" instead to enable debug output for scans.
# Include and dynamically link to cstore_fdw
make \
  CPPFLAGS="-I ../cstore_fdw" \
  SHLIB_LINK="-L/output/root/usr/local/lib -lcstore_fdw -lpython3.7m" \
  install
