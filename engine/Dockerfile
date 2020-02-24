# Multistage Dockerfile to build the Splitgraph engine: because of all the compilation of
# FDWs/libraries needed, we have to download a few hundred MB of dev tooling which we don't
# need at runtime. So, we define a single "toolchain" image, multiple images that build various
# FDWs and other extensions from it and then the final engine image that cherry-picks required
# binaries from the previous build stages: this is done by "make installing" them into a different
# prefix (PGXS's feature) and then copying that over into the filesystem root in the target.


#####
##### toolchain
#####

FROM postgres:11.7 AS toolchain

RUN apt-get update -qq && \
    apt-get install -y --allow-downgrades \
        build-essential \
        curl \
        wget \
        git \
        libssl-dev \
        libsasl2-dev \
        pkgconf \
        autoconf \
        libtool \
        # https://github.com/docker-library/postgres/issues/678#issuecomment-586888013
        libpq5=${PG_MAJOR}.* \
        libpq-dev=${PG_MAJOR}.* \
        postgresql-server-dev-$PG_MAJOR \
        libmongoc-1.0.0 \
        libmongoc-dev \
        protobuf-c-compiler \
        libprotobuf-c0-dev \
        zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev \
        cmake && \
    rm -rf /var/lib/apt/lists/*

# Fix taken from https://github.com/f-secure-foundry/usbarmory-debian-base_image/issues/9
RUN mkdir ~/.gnupg && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf && \
    apt-key adv --homedir ~/.gnupg --keyserver keys.gnupg.net --recv-keys 5072E1F5 && \
    echo "deb http://repo.mysql.com/apt/debian/ stretch mysql-8.0" > /etc/apt/sources.list.d/mysql.list && \
    apt-get update -qq && apt-get install -y libmysqlclient-dev && rm -rf /var/lib/apt/lists/*

# Build scripts for subsequent FDW builder images
RUN mkdir -p /build

# Also build and install Python
COPY ./engine/build_scripts/build_python.sh /build/
RUN /build/build_python.sh

RUN ldconfig

# Output root for the FDW builders (e.g. /output/root/usr/lib/postgresql/11/extensions...)
RUN mkdir -p /output/root

#####
##### cstore_fdw
#####

FROM toolchain AS builder_cstore
COPY ./engine/build_scripts/fdws/cstore_fdw /build
RUN /build/build_cstore_fdw.sh


#####
##### multicorn
#####

FROM toolchain AS builder_multicorn
COPY ./engine/build_scripts/fdws/multicorn /build
RUN /build/build_multicorn.sh


#####
##### mongo_fdw
#####

FROM toolchain AS builder_mongo
COPY ./engine/build_scripts/fdws/mongo_fdw /build
RUN /build/build_mongo_fdw.sh


#####
##### mysql_fdw
#####

FROM toolchain AS builder_mysql
COPY ./engine/build_scripts/fdws/mysql_fdw /build
RUN /build/build_mysql_fdw.sh


#####
##### splitgraph/engine
#####

FROM postgres:11.7

# We still have to install some runtime libraries here, but no dev.

RUN apt-get update -qq && \
    apt-get install -y \
        curl \
        libprotobuf-c1 \
        libmongoc-1.0-0 \
        wget && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir ~/.gnupg && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf && \
    apt-key adv --homedir ~/.gnupg --keyserver keys.gnupg.net --recv-keys 5072E1F5 && \
    echo "deb http://repo.mysql.com/apt/debian/ stretch mysql-8.0" > /etc/apt/sources.list.d/mysql.list && \
    apt-get update -qq && apt-get install -y libmysqlclient-dev && rm -rf /var/lib/apt/lists/*

# Extract and copy over all binaries from the builder containers

COPY --from=toolchain /output/python /
COPY --from=builder_cstore /output/root /

# This is slightly sad: whilst multicorn's make install respects DESTDIR,
# python setup.py doesn't and installs it to /usr/local.
# Since nothing else in here uses easy_install, we grab the .pth file and the unpacked egg.

COPY --from=builder_multicorn /output/root /
COPY --from=builder_multicorn \
    /usr/local/lib/python3.7/site-packages/easy-install.pth \
    /usr/local/lib/python3.7/site-packages/easy-install.pth
COPY --from=builder_multicorn \
    /usr/local/lib/python3.7/site-packages/multicorn-1.3.4.dev0-py3.7-linux-x86_64.egg/ \
    /usr/local/lib/python3.7/site-packages/

COPY --from=builder_mongo /output/root /
COPY --from=builder_mysql /output/root /

RUN ldconfig

# Set up Postgres config files/envvars/init scripts

EXPOSE 5432
ENV POSTGRES_DB splitgraph
ENV POSTGRES_USER sgr

COPY ./engine/etc /etc/
COPY ./engine/init_scripts /docker-entrypoint-initdb.d/

# Install Splitgraph itself (required for layered querying)

RUN mkdir -p /var/lib/splitgraph/objects && chmod 777 /var/lib/splitgraph/objects

# Do the installation in two phases to use Docker caching: first,
# install all the dependencies and then copy the Splitgraph code over. That way,
# if we just change the code, we only need to rebuild one layer rather than
# reinstall all dependencies from scratch.

# However, we need to make Poetry think that we actually have a genuine package
# that we want to install in editable mode in here, so we create and copy some files to fool it.
RUN mkdir -p /splitgraph/splitgraph && touch /splitgraph/splitgraph/__init__.py
COPY poetry.lock pyproject.toml README.md /splitgraph
RUN mkdir /build
COPY ./engine/build_scripts/build_splitgraph.sh /build/
RUN /build/build_splitgraph.sh

# pl/python
# The postgresql-plpython3-11 in stretch (debian that the pg11 image is currently based on)
# is built against python 3.5 whereas we want to use at least 3.6 and already build multicorn against it.
# Building just plpython3 ourselves is non-trivial, as it requires
# the whole Postgres source tree to be in place (can't just do a shallow clone
# and build only that extension.)
#
# So, hack time, get the archive from debian sid and install just the
# package itself without dependencies (deps are libc>=2.14 -- we have 2.24 -- and libpython3.7
# which we compiled earlier)

RUN wget http://http.us.debian.org/debian/pool/main/p/postgresql-11/postgresql-plpython3-11_11.6-2~sid1_amd64.deb && \
    echo "59980cdd6b7e30187ba851fbc3123c47b9a8ff99fd1327a1531f07dcc93dbefb  postgresql-plpython3-11_11.6-2~sid1_amd64.deb" | sha256sum -c - && \
    dpkg --force-all -i postgresql-plpython3-11_11.6-2~sid1_amd64.deb
# Fix to make the dpkg status file usable (so that APT doesn't fail on future installs in the container)
RUN sed -i "s/postgresql-11 (= 11.6-2~sid1), libc6 (>= 2.14), libpython3.7 (>= 3.7.0)/Depends: /" -i /var/lib/dpkg/status

# Copy the actual Splitgraph code over at this point.
COPY ./splitgraph /splitgraph/splitgraph

# postgis
# Install from stretch-backports rather than pgdg repo to prevent SCFGAL pulling in OSG and X11:
#   see https://github.com/Oslandia/SFCGAL/issues/136

ARG with_postgis
RUN test -z "${with_postgis}" || (\
    echo "deb http://deb.debian.org/debian stretch-backports main" >> /etc/apt/sources.list && \
    apt-get update -qq && \
    apt-get install -t stretch-backports --no-install-recommends -y \
        postgresql-11-postgis-2.5 \
        postgresql-11-postgis-2.5-scripts && \
    rm -rf /var/lib/apt/lists/* && \
    echo "CREATE EXTENSION postgis;" >> /docker-entrypoint-initdb.d/000_create_extensions.sql)

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
