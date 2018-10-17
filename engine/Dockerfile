FROM postgres:10.5

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
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /build_scripts
COPY build_scripts /build_scripts/
RUN find /build_scripts -type f -name '*.sh' -exec chmod a+x {} \;

# Mongo FDW
RUN ./build_scripts/fdws/mongo_fdw/build_mongo_fdw.sh

EXPOSE 5432
ENV POSTGRES_DB cachedb
ENV POSTGRES_USER sgr

# Postgres config files
COPY etc /etc/

# Splitgraph init scripts
COPY init_scripts /docker-entrypoint-initdb.d/

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
