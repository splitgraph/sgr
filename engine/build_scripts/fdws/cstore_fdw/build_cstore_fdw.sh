#!/bin/bash -ex

cd /src/cstore_fdw

export DESTDIR=/output/root
make install

# Slightly ugly hack: we want Multicorn to link against CStore
# and ld requires the library to be named libLIBRARY.so,
# so we copy cstore_fdw.so into /usr/local/lib as well.
mkdir -p $DESTDIR/usr/local/lib
cp cstore_fdw.so $DESTDIR/usr/local/lib/libcstore_fdw.so
