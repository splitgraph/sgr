#!/usr/bin/env bash

THIS_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
TEST_DIR="${THIS_DIR}/.."
REPO_ROOT_DIR="${TEST_DIR}/.."
DEFAULT_SG_CONFIG_FILE="${TEST_DIR}/resources/.sgconfig"

export SG_CONFIG_FILE=${SG_CONFIG_FILE-"${DEFAULT_SG_CONFIG_FILE}"}

echo "Wait for architecture to be up"
echo "Using config file at $SG_CONFIG_FILE ..."

_init_engines() {
  ( SG_ENGINE_HOST=local_engine SG_ENGINE_PORT=5432 sgr init ) \
    && ( SG_ENGINE_HOST=remote_engine SG_ENGINE_PORT=5431 sgr init ) \
    && return 0

  return 1
}

_run_health_check() {
    pushd "$REPO_ROOT_DIR" \
        && python -c "import test.splitgraph.conftest as c; c.healthcheck()" \
        && return 0

    return 1
}

_attempt_init() {
  ( _init_engines >/dev/null 2>&1 ; )\
    && ( _run_health_check >/dev/null 2>&1 ; ) \
    && return 0
}

_wait_for_test_architecture() {
    local counter=0
    while true ; do

        if test $counter -eq 10 ; then
            echo
            echo "FATAL: Could not connect to test-architecture after 10 tries"
            exit 1
        fi

        if _attempt_init ; then
            echo
            echo "Architecture is ready"
            break;
        else
            echo -n "."
            let counter=counter+1
            sleep 5
        fi
    done

    return 0
}

_wait_for_test_architecture && exit 0
exit 1
