#!/bin/bash

echo "Install mysql_fdw extension..."

fatal_error() {
    echo "Fatal:" "$@" 1>&2 ;
    exit 1;
}

mkdir /tmp/mysql-fdw-staging || {
    fatal_error "Failed to mkdir /tmp/mysql-fdw-staging" ;
}


cd /tmp/mysql-fdw-staging || {
    fatal_error "Failed to cd /tmp/mysql-fdw-staging" ;
}

echo "Download mysql_fdw source code..."
git clone https://github.com/EnterpriseDB/mysql_fdw.git
cd mysql_fdw || {
    fatal_error "Failed to cd /tmp/mysql-fdw-staging/mysql_fdw" ;
}

echo "Build mysql_fdw..."

USE_PGXS=1 make && USE_PGXS=1 make install

echo "Finished building mysql_fdw."

echo "Cleanup mysql_fdw..."
cd / || {
    fatal_error "Failed to cd to / for cleanup operation" ;
}

rm -rf /tmp/mysql-fdw-staging || true
