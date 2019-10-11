# Multistage Dockerfile to build the Splitgraph engine: because of all the compilation of
# FDWs/libraries needed, we have to download a few hundred MB of dev tooling which we don't
# need at runtime. So, we define a single "toolchain" image, multiple images that build various
# FDWs and other extensions from it and then the final engine image that cherry-picks required
# binaries from the previous build stages: this is done by "make installing" them into a different
# prefix (PGXS's feature) and then copying that over into the filesystem root in the target.


#####
##### toolchain
#####

FROM postgres:11.5 AS toolchain

RUN apt-get update -qq && \
    apt-get install -y \
        build-essential \
        curl \
        wget \
        git \
        libssl-dev \
        libsasl2-dev \
        pkgconf \
        autoconf \
        libtool \
        postgresql-server-dev-11 \
        libmongoc-1.0.0 \
        libmongoc-dev \
        protobuf-c-compiler \
        libprotobuf-c0-dev \
        zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev \
        cmake && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://dev.mysql.com/get/mysql-apt-config_0.8.13-1_all.deb
RUN echo mysql-apt-config mysql-apt-config/repo-codename  select stretch | debconf-set-selections && \
    echo mysql-apt-config mysql-apt-config/select-server  select mysql-8.0 | debconf-set-selections && \
    echo mysql-apt-config mysql-apt-config/select-product select Apply | debconf-set-selections
RUN DEBIAN_FRONTEND=noninteractive dpkg -i mysql-apt-config_0.8.13-1_all.deb
RUN apt-get update -qq && apt-get install -y libmysqlclient-dev && rm -rf /var/lib/apt/lists/*

# Build scripts for subsequent FDW builder images
RUN mkdir -p /build

# Also build and install Python
COPY ./build_scripts/build_python.sh /build/
RUN /build/build_python.sh

RUN ldconfig

# Output root for the FDW builders (e.g. /output/root/usr/lib/postgresql/11/extensions...)
RUN mkdir -p /output/root

#####
##### cstore_fdw
#####

FROM toolchain AS builder_cstore
COPY build_scripts/fdws/cstore_fdw /build
RUN /build/build_cstore_fdw.sh


#####
##### multicorn
#####

FROM toolchain AS builder_multicorn
COPY build_scripts/fdws/multicorn /build
RUN /build/build_multicorn.sh


#####
##### mongo_fdw
#####

FROM toolchain AS builder_mongo
COPY build_scripts/fdws/mongo_fdw /build
RUN /build/build_mongo_fdw.sh


#####
##### mysql_fdw
#####

FROM toolchain AS builder_mysql
COPY build_scripts/fdws/mysql_fdw /build
RUN /build/build_mysql_fdw.sh


#####
##### splitgraph/engine
#####

FROM postgres:11.5

# We still have to install some runtime libraries here, but no dev.

RUN apt-get update -qq && \
    apt-get install -y \
        curl \
        libprotobuf-c1 \
        libmongoc-1.0-0 \
        wget && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://dev.mysql.com/get/mysql-apt-config_0.8.13-1_all.deb
RUN echo mysql-apt-config mysql-apt-config/repo-codename  select stretch | debconf-set-selections && \
    echo mysql-apt-config mysql-apt-config/select-server  select mysql-8.0 | debconf-set-selections && \
    echo mysql-apt-config mysql-apt-config/select-product select Apply | debconf-set-selections
RUN DEBIAN_FRONTEND=noninteractive dpkg -i mysql-apt-config_0.8.13-1_all.deb
RUN apt-get update -qq && apt-get install -y libmysqlclient-dev && rm -rf /var/lib/apt/lists/*

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

COPY etc /etc/
COPY init_scripts /docker-entrypoint-initdb.d/

# Install Splitgraph itself (required for layered querying)

RUN mkdir -p /var/lib/splitgraph/objects && chmod 777 /var/lib/splitgraph/objects

# Git submodule
COPY splitgraph /splitgraph
RUN mkdir /build
COPY ./build_scripts/build_splitgraph.sh /build/
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

RUN wget http://http.us.debian.org/debian/pool/main/p/postgresql-11/postgresql-plpython3-11_11.5-3sid2_amd64.deb && \
    echo "1b0cdca6aa97a07b96481623f5d58ce4a5d24d96eea5af004aa4baaabd4a2311  postgresql-plpython3-11_11.5-3sid2_amd64.deb" | sha256sum -c - && \
    dpkg --force-all -i postgresql-plpython3-11_11.5-3sid2_amd64.deb
# Fix to make the dpkg status file usable (so that APT doesn't fail on future installs in the container)
RUN sed -i "s/Depends: postgresql-11 (= 11.5-3sid2), libc6 (>= 2.14), libpython3.7 (>= 3.7.0)/Depends: /" -i /var/lib/dpkg/status

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
