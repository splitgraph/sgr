# syntax = docker/dockerfile:experimental

# Multistage Dockerfile to build the Splitgraph engine: because of all the compilation of
# FDWs/libraries needed, we have to download a few hundred MB of dev tooling which we don't
# need at runtime. So, we define a single "toolchain" image, multiple images that build various
# FDWs and other extensions from it and then the final engine image that cherry-picks required
# binaries from the previous build stages: this is done by "make installing" them into a different
# prefix (PGXS's feature) and then copying that over into the filesystem root in the target.


#####
##### toolchain
#####

FROM postgres:12.2 AS toolchain

# Add Debian bullseye (testing) repo since it has Python 3.8 and postgresql-plpython3-12
RUN echo "deb http://deb.debian.org/debian bullseye main" >> /etc/apt/sources.list.d/debian-bullseye.list

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
        libprotobuf-c-dev \
        libpython3.8-dev \
        python3.8 \
        python3-setuptools \
        cmake \
        clang-7 && \
    rm -rf /var/lib/apt/lists/*

# python3.8 executable required by Multicorn to install itself
# python3-setuptools also needed by Multicorn and it pulls in python3.7 but
# we'll get rid of all this junk in the next build stage.

# Set locale to C instead of en-US, Postgres initdb breaks with default
# locale otherwise + we get build warnings for FDWs.
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# Fix taken from https://github.com/f-secure-foundry/usbarmory-debian-base_image/issues/9
RUN mkdir ~/.gnupg && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf && \
    apt-key adv --homedir ~/.gnupg --keyserver hkp://keyserver.ubuntu.com --recv-keys 5072E1F5 && \
    echo "deb http://repo.mysql.com/apt/debian/ buster mysql-8.0" > /etc/apt/sources.list.d/mysql.list && \
    apt-get update -qq && apt-get install -y libmysqlclient-dev && rm -rf /var/lib/apt/lists/*

# Build scripts for subsequent FDW builder images
RUN mkdir -p /build

RUN ldconfig

# Output root for the FDW builders (e.g. /output/root/usr/lib/postgresql/12/extensions...)
RUN mkdir -p /output/root

#####
##### cstore_fdw and multicorn
#####

FROM toolchain AS builder_cstore_multicorn
COPY ./engine/src/cstore_fdw /src/cstore_fdw
COPY ./engine/src/Multicorn /src/Multicorn

COPY ./engine/build_scripts/fdws/multicorn /build
COPY ./engine/build_scripts/fdws/cstore_fdw /build

RUN /build/build_cstore_fdw.sh
RUN /build/build_multicorn.sh

#####
##### mongo_fdw
#####

FROM toolchain AS builder_mongo_fdw
COPY ./engine/build_scripts/fdws/mongo_fdw /build
RUN /build/build_mongo_fdw.sh


#####
##### mysql_fdw
#####

FROM toolchain AS builder_mysql_fdw
COPY ./engine/build_scripts/fdws/mysql_fdw /build
RUN /build/build_mysql_fdw.sh


#####
##### splitgraph/engine
#####

FROM postgres:12.2

# We still have to install some runtime libraries here, but no dev.

RUN echo "deb http://deb.debian.org/debian bullseye main" >> /etc/apt/sources.list.d/debian-bullseye.list
RUN apt-get update -qq && \
    apt-get install -y \
        curl \
        libprotobuf-c1 \
        libmongoc-1.0-0 \
        libpython3.8 \
        python3.8 \
        wget && \
    # Featuring a hack to install distutils + pip in a way that doesn't pull in all of py3.7 and apparmor
    # (even though we get python3-distutils_3.8.2-2_all.deb, an actual install grabs all of that)
    apt-get download python3-distutils && \
    dpkg-deb -x python3-distutils*.deb / && \
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python3.8 get-pip.py && \
    rm python3-distutils*.deb get-pip.py && \

    # Same hack for plpython3 for pg12 that uses 3.8 rather than 3.7:
    # normal apt install causes compat issues.
    apt-get download -t bullseye postgresql-plpython3-12 && \
    dpkg-deb -x postgresql-plpython3-12*.deb / && \
    rm postgresql-plpython3-12*.deb && \
    rm -rf /var/lib/apt/lists/*

# Set locale to C instead of en-US, Postgres initdb breaks with default
# locale otherwise + we get build warnings for FDWs.
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# Install Splitgraph itself (required for layered querying)

RUN mkdir -p /var/lib/splitgraph/objects && chmod 777 /var/lib/splitgraph/objects

# Do the installation in two phases to use Docker caching: first,
# install all the dependencies and then copy the Splitgraph code over (way later in the build).
# That way, if we just change the code, we only need to rebuild one last layer rather than
# reinstall all dependencies from scratch. We also do dependency installation before
# copying over various FDW SOs, since those are cheaper to copy and can get rebuilt more often.

# However, we need to make Poetry think that we actually have a genuine package
# that we want to install in editable mode in here, so we create and copy some files to fool it.
RUN mkdir -p /splitgraph/splitgraph && touch /splitgraph/splitgraph/__init__.py
COPY poetry.lock pyproject.toml README.md /splitgraph/
RUN mkdir /build
COPY ./engine/build_scripts/build_splitgraph.sh /build/
RUN --mount=type=cache,id=pip-cache,target=/root/.cache/pip /build/build_splitgraph.sh


RUN mkdir ~/.gnupg && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf && \
    apt-key adv --homedir ~/.gnupg --keyserver hkp://keyserver.ubuntu.com --recv-keys 5072E1F5 && \
    echo "deb http://repo.mysql.com/apt/debian/ buster mysql-8.0" > /etc/apt/sources.list.d/mysql.list && \
    apt-get update -qq && apt-get install -y libmysqlclient-dev && rm -rf /var/lib/apt/lists/*

# Extract and copy over all binaries from the builder stages
# This is slightly sad: whilst multicorn's make install respects DESTDIR,
# python setup.py doesn't and installs it to /usr/local.
# Since nothing else in here uses easy_install, we grab the .pth file and the unpacked egg.

COPY --from=builder_cstore_multicorn /output/root /
COPY --from=builder_cstore_multicorn \
    /usr/local/lib/python3.8/dist-packages/easy-install.pth \
    /usr/local/lib/python3.8/dist-packages/easy-install.pth
COPY --from=builder_cstore_multicorn \
    /usr/local/lib/python3.8/dist-packages/multicorn-1.4.0.dev0-py3.8-linux-x86_64.egg \
    /usr/local/lib/python3.8/dist-packages/multicorn-1.4.0.dev0-py3.8-linux-x86_64.egg

COPY --from=builder_mongo_fdw /output/root /
COPY --from=builder_mysql_fdw /output/root /

RUN ldconfig

# Set up Postgres config files/envvars/init scripts

EXPOSE 5432
ENV POSTGRES_DB splitgraph
ENV POSTGRES_USER sgr

COPY ./engine/etc /etc/
COPY ./engine/init_scripts /docker-entrypoint-initdb.d/

# Delete bullseye from the apt sources list -- it can break things that inherit from
# this image (and definitely breaks postgis install).
RUN rm /etc/apt/sources.list.d/debian-bullseye.list

ENV PYTHONPATH "${PYTHONPATH}:/splitgraph"

# Copy the actual Splitgraph code over at this point.
COPY ./splitgraph /splitgraph/splitgraph

# https://github.com/postgis/docker-postgis/blob/master/12-3.0/Dockerfile
ARG with_postgis
RUN test -z "${with_postgis}" || (\
    export POSTGIS_MAJOR=3 && \
    export POSTGIS_VERSION=3.0.1+dfsg-2.pgdg100+1 && \
    apt-get update \
      && apt-cache showpkg postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
      && apt-get install -y --no-install-recommends \
           postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR=$POSTGIS_VERSION \
           postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts=$POSTGIS_VERSION \
      && rm -rf /var/lib/apt/lists/* && \
    echo "CREATE EXTENSION postgis;" >> /docker-entrypoint-initdb.d/000_create_extensions.sql)

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
