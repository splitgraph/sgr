#!/bin/bash -ex

echo "Building duckdb_fdw..."
cd /build
git clone https://github.com/duckdb/duckdb.git
cd duckdb
git checkout v0.2.7
export DESTDIR=/output/root
make clean && make

# Copy libsqlite3 stuff (linked statically into the FDW)
cp build/release/tools/sqlite3_api_wrapper/libsqlite3_api_wrapper.so /usr/lib
cp tools/sqlite3_api_wrapper/include/sqlite3.h /usr/include

cd build/release
make install
cd ../../..

git clone https://github.com/alitrack/duckdb_fdw.git
cd duckdb_fdw
make USE_PGXS=1
make install USE_PGXS=1
