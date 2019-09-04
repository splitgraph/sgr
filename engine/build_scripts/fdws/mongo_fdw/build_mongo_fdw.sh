#!/bin/bash -ex

echo "Install mongo_fdw extension..."

echo "Download mongo_fdw source code..."
cd /build
git clone https://github.com/EnterpriseDB/mongo_fdw.git

cd mongo_fdw

echo "Build mongo_fdw..."

git checkout 6d06a82b9071c2a8e92d80e07ef7f9d1c4e1e69d
git apply ../build-fixes.patch

# Build the prerequisites (libmongoc/json-c): these won't make it into
# the final image though as they're difficult to put in the right place whilst also
# getting mongo_fdw to still build against them.
./autogen.sh --with-master

# Build the actual FDW.
export DESTDIR=/output/root
make clean && make && make install
