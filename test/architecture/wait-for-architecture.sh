#!/usr/bin/env bash

THIS_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
TEST_DIR="${THIS_DIR}/.."
REPO_ROOT_DIR="${TEST_DIR}/.."
DEFAULT_SG_CONFIG_FILE="${TEST_DIR}/resources/.sgconfig"

if [[ "$1" == "--mounting" ]]; then
    echo "Wait for mounting architecture to be up"
    HEALTHCHECK="import test.splitgraph.conftest as c; c.healthcheck_mounting()"
else
    echo "Wait for core architecture to be up"
    HEALTHCHECK="import test.splitgraph.conftest as c; c.healthcheck()"
fi

export SG_CONFIG_FILE=${SG_CONFIG_FILE-"${DEFAULT_SG_CONFIG_FILE}"}

echo "Using config file at $SG_CONFIG_FILE ..."

_run_health_check() {
    pushd "$REPO_ROOT_DIR" \
        && python -c "$HEALTHCHECK" \
        && return 0

    return 1
}


_wait_for_test_architecture() {
    local counter=0
    while true ; do

        if test $counter -eq 20 ; then
            echo
            echo "FATAL: Could not connect to test-architecture after 20 tries"
            exit 1
        fi

        if _run_health_check ; then
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
