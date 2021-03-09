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

# Pin mysql_fdw to the last known good build on Jan 25, 2021. The next commit, 5c80ff8743a02af95cd97d6ff5b925617a3e9f01, makes it segfault when querying more than one row
# with a binary type (see test_mount_mysql)
git checkout cf88939d1e3f54e3fa9cf03010fa48dff8035560

export USE_PGXS=1
export DESTDIR=/output/root
make install
