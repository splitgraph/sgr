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

# At engine initialization, we also create the directory that's shared
# between the host and the engine (for object storage).

_init_engines() {
  ( SG_ENGINE=LOCAL sgr init ) \
    && ( SG_ENGINE=remote_engine sgr init ) \
    && mkdir -p /var/lib/splitgraph/objects \
    && mkdir -p /var/lib/splitgraph/objects_remote \
    && return 0

  return 1
}

_run_health_check() {
    pushd "$REPO_ROOT_DIR" \
        && python -c "$HEALTHCHECK" \
        && return 0

    return 1
}

_attempt_init() {
  _init_engines \
    && _run_health_check \
    && return 0
}

_wait_for_test_architecture() {
    local counter=0
    while true ; do

        if test $counter -eq 20 ; then
            echo
            echo "FATAL: Could not connect to test-architecture after 20 tries"
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
