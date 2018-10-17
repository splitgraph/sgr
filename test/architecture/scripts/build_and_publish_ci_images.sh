#!/bin/bash

DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)

usage() {
    echo
    echo "./scripts/build_and_publish_ci_images.sh"
    echo
    echo "1. Build images via scripts/build_ci_images.sh"
    echo
    echo

    "$DIR"/build_ci_images.sh --help

    echo
    echo
    echo "2. Publish images via scripts/publish_ci_images.sh"
    echo
    echo

    "$DIR"/publish_ci_images.sh --help

    echo
    echo
    echo
}

test "$1" == "--help" && { usage ; exit 1 ; }

"$DIR"/build_ci_images.sh && "$DIR"/publish_ci_images.sh
