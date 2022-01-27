#!/bin/bash

ORIGIN_USER=${ORIGIN_USER}
ORIGIN_PASS=${ORIGIN_PASS}
ORIGIN_PG_DB=${ORIGIN_PG_DB}

SU='/bin/su postgres -c'

rm /var/lib/postgresql/10/main/postmaster.pid /var/run/postgresql/.*.lock
service postgresql start

if [ ! -e '/done_setup' ]; then
    ${SU} "createdb ${ORIGIN_PG_DB}"
    ${SU} "createuser -d -s -r -l ${ORIGIN_USER}"
    ${SU} "psql postgres -c \"ALTER USER ${ORIGIN_USER} WITH ENCRYPTED PASSWORD '${ORIGIN_PASS}'\""

    if [ -e '/src/setup.sql' ]; then
        ${SU} "psql ${ORIGIN_PG_DB} < /src/setup.sql"
        ${SU} "psql ${ORIGIN_PG_DB} < /src/load_account_data.sql"
    fi

    echo 1 > /done_setup
fi

tail -f /var/log/postgresql/postgresql-10-main.log
