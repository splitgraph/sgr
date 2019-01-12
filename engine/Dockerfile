FROM postgres:10.5

# We need the mysql C client libs to link the FDW against.
# We could install MySQL's apt repo deb package, but it's interactive, so it's easier
# to just add the sources.list entry we want.
RUN apt-key adv --keyserver pgp.mit.edu --recv-keys 5072E1F5
RUN echo "deb http://repo.mysql.com/apt/debian/ stretch mysql-8.0" > /etc/apt/sources.list.d/mysql.list

# Make sure to just get the pg10 toolchain, otherwise the extensions build against pg11
RUN apt-get update -qq && \
    apt-get install -y \
        build-essential \
        wget \
        python3-pip \
        python3-venv \
        git \
        libssl-dev \
        libsasl2-dev \
        pkgconf \
        autoconf \
        libtool \
        postgresql-server-dev-10 \
        libmongoc-1.0.0 \
        libmongoc-dev \
        libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /build_scripts
COPY build_scripts /build_scripts/
RUN find /build_scripts -type f -name '*.sh' -exec chmod a+x {} \;

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
RUN pip3 install poetry

# Git submodule
COPY splitgraph /splitgraph
RUN ./build_scripts/build_splitgraph.sh

# Splitgraph init scripts
COPY init_scripts /docker-entrypoint-initdb.d/

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
