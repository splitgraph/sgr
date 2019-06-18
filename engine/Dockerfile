FROM postgres:11.2

RUN apt-get update -qq && \
    apt-get install -y \
        build-essential \
        curl \
        wget \
        python3-pip \
        python3-venv \
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
        libprotobuf-c0-dev

RUN wget https://dev.mysql.com/get/mysql-apt-config_0.8.12-1_all.deb
RUN echo mysql-apt-config mysql-apt-config/select-server  select  mysql-8.0 | debconf-set-selections && \
    echo mysql-apt-config mysql-apt-config/select-product select Apply | debconf-set-selections
RUN DEBIAN_FRONTEND=noninteractive dpkg -i mysql-apt-config_0.8.12-1_all.deb
RUN apt-get update -qq && apt-get install -y libmysqlclient-dev && rm -rf /var/lib/apt/lists/*

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

# Splitgraph core libraries (required for the layered querying FDW)
# Git submodule

RUN apt-get update -qq && apt-get install -y zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev

# splitgraph requires python 3.6
RUN curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash

COPY splitgraph /splitgraph
RUN ./build_scripts/build_splitgraph.sh

# Splitgraph init scripts
COPY init_scripts /docker-entrypoint-initdb.d/
RUN mkdir -p /var/lib/splitgraph/objects
RUN chmod 777 /var/lib/splitgraph/objects

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
