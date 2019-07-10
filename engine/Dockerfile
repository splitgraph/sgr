FROM postgres:11.4

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
        cmake

RUN wget https://dev.mysql.com/get/mysql-apt-config_0.8.13-1_all.deb
RUN echo mysql-apt-config mysql-apt-config/repo-codename  select stretch | debconf-set-selections && \
    echo mysql-apt-config mysql-apt-config/select-server  select mysql-8.0 | debconf-set-selections && \
    echo mysql-apt-config mysql-apt-config/select-product select Apply | debconf-set-selections
RUN DEBIAN_FRONTEND=noninteractive dpkg -i mysql-apt-config_0.8.13-1_all.deb
RUN apt-get update -qq && apt-get install -y libmysqlclient-dev && rm -rf /var/lib/apt/lists/*

# Install Python 3.7.3 globally.
RUN curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
RUN CFLAGS="-fPIC -O2" CXXFLAGS=-fPIC PYTHON_CONFIGURE_OPTS="--enable-shared" \
    ~/.pyenv/plugins/python-build/bin/python-build 3.7.3 /usr/local/

# This is to get postgresql-plpython3 to see the newly built libpython
RUN ldconfig

RUN mkdir -p /build_scripts
COPY build_scripts /build_scripts/

# cstore_fdw
RUN ./build_scripts/fdws/cstore_fdw/build_cstore_fdw.sh
# Mongo FDW
RUN ./build_scripts/fdws/mongo_fdw/build_mongo_fdw.sh
# MySQL FDW
RUN ./build_scripts/fdws/mysql_fdw/build_mysql_fdw.sh
# Multicorn
RUN ./build_scripts/fdws/multicorn/build_multicorn.sh

EXPOSE 5432
ENV POSTGRES_DB cachedb
ENV POSTGRES_USER sgr

# Postgres config files
COPY etc /etc/

RUN mkdir -p /var/lib/splitgraph/objects
RUN chmod 777 /var/lib/splitgraph/objects

# Splitgraph core libraries (required for the layered querying FDW)
# Git submodule
COPY splitgraph /splitgraph
RUN ./build_scripts/build_splitgraph.sh

# Splitgraph init scripts
COPY init_scripts /docker-entrypoint-initdb.d/

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

RUN wget http://http.us.debian.org/debian/pool/main/p/postgresql-11/postgresql-plpython3-11_11.4-1_amd64.deb && \
    echo "fef3a643b255e80e1d471800ba337774df80968fd2a26c89cd90bd9709efaa21  postgresql-plpython3-11_11.4-1_amd64.deb" | shasum -c && \
    dpkg --force-all -i postgresql-plpython3-11_11.4-1_amd64.deb

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
