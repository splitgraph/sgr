#!/bin/bash -ex

echo "Building Apache ORC..."
cd /build

wget https://github.com/apache/orc/archive/refs/tags/rel/release-1.6.8.tar.gz
git clone https://github.com/HighgoSoftware/orc_fdw.git

tar -xzf release-1.6.8.tar.gz
cd orc-rel-release-1.6.8
wget https://raw.githubusercontent.com/HighgoSoftware/orc_fdw/master/build/orc/build-orc.sh

chmod +x ./build-orc.sh
./build-orc.sh
./build-orc.sh copy_orc_install /build/orc_fdw

cd /build/orc_fdw

echo "Installing orc_fdw..."

export DESTDIR=/output/root
make clean && make && make install
