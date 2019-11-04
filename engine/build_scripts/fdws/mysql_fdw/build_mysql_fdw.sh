#!/bin/bash -ex

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

export USE_PGXS=1
export DESTDIR=/output/root
make install
