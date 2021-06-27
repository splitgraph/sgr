#!/bin/bash -ex

echo "Building parquet_fdw..."
cd /build
git clone https://github.com/adjust/parquet_fdw.git
cd parquet_fdw
export DESTDIR=/output/root
make clean && make && make install
