#!/bin/bash -ex

cd /src/cstore_fdw

export DESTDIR=/output/root
make install
