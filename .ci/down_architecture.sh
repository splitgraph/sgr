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
    && pushd "${ARCHITECTURE_DIR}" \
    && echo "Bringing down the test architecture..." \
    && docker-compose -f $CORE_ARCHITECTURE -f $MOUNTING_ARCHITECTURE down -v \
    && echo "Test architecture down." \
    && popd \
    && exit 0

exit 1
