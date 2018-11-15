FROM postgres:10.5

# We need the mysql C client libs to link the FDW against.
# We could install MySQL's apt repo deb package, but it's interactive, so it's easier
# to just add the sources.list entry we want.
RUN apt-key adv --keyserver hkp://pgp.mit.edu:80 --recv-keys 5072E1F5
RUN echo "deb http://repo.mysql.com/apt/debian/ stretch mysql-8.0" > /etc/apt/sources.list.d/mysql.list

RUN apt-get update -qq && \
    apt-get install -y \
        build-essential \
        wget \
        python-dev \
        python-pip \
        git \
        libssl-dev \
        libsasl2-dev \
        pkgconf \
        autoconf \
        libtool \
        postgresql-server-dev-all \
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

EXPOSE 5432
ENV POSTGRES_DB cachedb
ENV POSTGRES_USER sgr

# Postgres config files
COPY etc /etc/

# Splitgraph init scripts
COPY init_scripts /docker-entrypoint-initdb.d/

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
