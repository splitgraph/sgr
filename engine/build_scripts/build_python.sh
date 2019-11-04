#!/bin/bash -e

mkdir -p /build/python
cd /build/python

wget https://www.python.org/ftp/python/3.7.4/Python-3.7.4.tgz
tar -xzf Python-3.7.4.tgz
cd Python-3.7.4

# We need to configure the Python build to install into /usr/local (default) but actually
# make install it into a staging area, as the engine image build won't be able to easily
# pull Python binaries out otherwise.

./configure --enable-shared
make -j4

export DESTDIR=/output/python
make install

# Delete unneeded files (like test code and compiled bytecode): takes up about 60MB (out of ~160MB) in the target
# Taken from https://github.com/docker-library/python/blob/c3233a936f58bee7c6899d3e381f23ed12cfc7a8/3.7/alpine3.10/Dockerfile
find /output/python/usr/local -depth \
		\( \
			\( -type d -a \( -name test -o -name tests \) \) \
			-o \
			\( -type f -a \( -name '*.pyc' -o -name '*.pyo' \) \) \
		\) -exec rm -rf '{}' +

# "Install" Python locally too.
echo "Copying the Python installation into root..."
cp -r /output/python/. /
echo "Done."
