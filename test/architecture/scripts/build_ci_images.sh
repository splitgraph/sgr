#!/bin/bash

usage() {
    echo
    echo "./scripts/build_ci_images.sh"
    echo
    echo "Build the services in the docker-compose.ci.yml file As long as they "
    echo "have an image: directive to tag them with splitgraphci/imgname, they "
    echo "will be tagged so that scripts/publish_ci_images.sh can detect and"
    echo "publish them."
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

docker-compose -f docker-compose.ci.yml build && exit 0

fatal_error "Build failed"
