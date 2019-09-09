#!/bin/bash

ORIGIN_USER=${ORIGIN_USER}
ORIGIN_PASS=${ORIGIN_PASS}
ORIGIN_MONGO_DB=${ORIGIN_MONGO_DB}

mongod --fork --logpath /var/log/mongodb/mongod.log --bind_ip_all

if [ ! -e '/done_setup' ]; then
    mongo ${ORIGIN_MONGO_DB} --eval "db.createUser({\"user\": \"${ORIGIN_USER}\", \"pwd\": \"${ORIGIN_PASS}\", \"roles\": [\"readWrite\"]})"
    
    if [ -e '/src/setup.js' ]; then
        mongo ${ORIGIN_MONGO_DB} < /src/setup.js
    fi

    echo 1 > /done_setup
fi

tail -F /var/log/mongodb/mongod.log
