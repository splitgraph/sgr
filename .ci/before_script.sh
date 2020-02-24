#!/usr/bin/env bash

CI_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
REPO_ROOT_DIR="${CI_DIR}/.."
TEST_DIR="${REPO_ROOT_DIR}/test"
ARCHITECTURE_DIR="${TEST_DIR}/architecture"

CORE_ARCHITECTURE="docker-compose.core.yml"
MOUNTING_ARCHITECTURE="docker-compose.mounting.yml"

# Stop the PG/MySQL that ship with Travis and run our own integration test
# SG engine/remote architecture instead.
pushd "$REPO_ROOT_DIR" \
    && ( sudo /etc/init.d/postgresql stop || true ; ) \
    && ( sudo /etc/init.d/mysql stop || true ; ) \
    && pushd "${ARCHITECTURE_DIR}" \
    && docker-compose -f $CORE_ARCHITECTURE build \
    && docker-compose -f $CORE_ARCHITECTURE up -d \
    && { {
        echo "Building the mounting test architecture in the background: " $(date)
        docker-compose -f $MOUNTING_ARCHITECTURE build
        docker-compose -f $MOUNTING_ARCHITECTURE up -d
        echo "Background mounting test architecture build complete: " $(date)
    } & } \
    && popd \
    && echo "Wait for core test architecture..." \
    && pushd "${ARCHITECTURE_DIR}" \
    && ( grep local_engine /etc/hosts >/dev/null || echo "127.0.0.1 local_engine" | sudo tee -a /etc/hosts ; ) \
    && ( grep remote_engine /etc/hosts >/dev/null || echo "127.0.0.1 remote_engine" | sudo tee -a /etc/hosts ; ) \
    && ( grep objectstorage /etc/hosts >/dev/null || echo "127.0.0.1 objectstorage" | sudo tee -a /etc/hosts ; ) \
    && ./wait-for-architecture.sh \
    && popd \
    && exit 0

exit 1
