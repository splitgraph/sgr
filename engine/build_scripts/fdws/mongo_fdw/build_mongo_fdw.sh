#!/bin/bash

echo "Install mongo_fdw extension..."

fatal_error() {
    echo "Fatal:" "$@" 1>&2 ;
    exit 1;
}

mkdir /tmp/mongo-fdw-staging || {
    fatal_error "Failed to mkdir /tmp/mongo-fdw-staging" ;
}


cd /tmp/mongo-fdw-staging || {
    fatal_error "Failed to cd /tmp/mongo-fdw-staging" ;
}

echo "Download mongo_fdw source code..."
git clone https://github.com/EnterpriseDB/mongo_fdw.git

cd mongo_fdw || {
    fatal_error "Failed to cd /tmp/mongo-fdw-staging/mongo_fdw" ;
}

echo "Build mongo_fdw..."

# roll back to Nov 2018 -- build breaks at next commit after this one, TODO investigate
git checkout 83b7134

# Build the prerequisites
./autogen.sh --with-master

# Build the actual FDW
make clean && make && make install

echo "Finished building mongo_fdw."

echo "Cleanup mongo_fdw..."
cd / || {
    fatal_error "Failed to cd to / for cleanup operation" ;
}

rm -rf /tmp/mongo-fdw-staging || true
