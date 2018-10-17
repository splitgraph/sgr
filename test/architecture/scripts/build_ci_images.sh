#!/bin/bash

usage() {
    echo
    echo "./scripts/build_ci_images.sh"
    echo
    echo "Find services defined in this docker-compose beginning with ci_"
    echo "and build them. As long as they have an image: directive to tag"
    echo "them with splitgraphci/imgname, they will be tagged so that"
    echo "scripts/publish_ci_images.sh can detect and publish them."
    echo
}

test "$1" == "--help" && { usage ; exit 1 ; }

set -eo pipefail

_log_error() {
    echo "$@" 1>&2
}

fatal_error() {
    _log_error "Fatal:" "$@"
    exit 1
}

_log() {
    echo "$@"
}

DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

ARCH_DIR="$DIR"/../

cd "$ARCH_DIR" || {
    fatal_error "Failed to cd to architecture directory at $ARCH_DIR" ;
}

which docker-compose >/dev/null 2>&1|| {
    fatal_error "docker-compose is not installed or not in \$PATH";
}

get_ci_services() {
    docker-compose config --services \
        | grep '^ci_'
}

_log "Building CI services:"
get_ci_services

get_ci_services | xargs docker-compose build || { \
    fatal_error "Failed to build CI services."
}

_log "Success."
