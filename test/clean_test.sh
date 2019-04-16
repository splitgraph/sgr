#!/usr/bin/env bash

# ./clean_test.sh
#
# Run a "clean" test. Intended for running in local development to give a close
# simulation of the test suite running in travis. Also accounts for the
# possibility that some e.g. splitgraphci images are outdated.
#
#   - Destroy and recreate docker environment (using down -v)
#   - Run tests from within container
#   - Exit 0 on all tests pass
#   - Exit 1 otherwise

TEST_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
ARCHITECTURE_DIR="${TEST_DIR}/architecture"
REPO_ROOT_DIR="${TEST_DIR}/.."
CORE_ARCHITECTURE="docker-compose.core.yml"
MOUNTING_ARCHITECTURE="docker-compose.mounting.yml"

pushd "$REPO_ROOT_DIR" \
    && pushd "${ARCHITECTURE_DIR}" \
    && docker-compose -f $CORE_ARCHITECTURE -f $MOUNTING_ARCHITECTURE pull \
    && docker-compose -f $CORE_ARCHITECTURE -f $MOUNTING_ARCHITECTURE down -v \
    && docker-compose -f $CORE_ARCHITECTURE -f $MOUNTING_ARCHITECTURE build \
    && docker-compose -f $CORE_ARCHITECTURE -f $MOUNTING_ARCHITECTURE up -d --force-recreate --remove-orphans \
    && popd \
    && echo "Wait for test architecture..." \
    && pushd "${ARCHITECTURE_DIR}" \
    && ./wait-for-architecture.sh --mounting \
    && docker-compose -f $CORE_ARCHITECTURE -f $MOUNTING_ARCHITECTURE -f docker-compose.dev.yml run test \
    && echo "Tests passed" \
    && popd \
    && exit 0

popd

echo "Tests (or something) failed"
exit 1
