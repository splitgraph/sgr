#!/bin/bash -ex

# CI script for compiling Sphinx docs and other assets into a bundle used to build
# the splitgraph.com docs section (API and sgr). The bundle is available on the
# releases page for the splitgraph.com CI job to pick up.

# Doesn't actually build the HTML docs and isn't useful by itself (contains files
# in Sphinx-internal fjson format).

OUTPUT=${OUTPUT-sgr-docs-bin}
CI_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
REPO_ROOT_DIR="${CI_DIR}/.."
TARGET_DIR="${REPO_ROOT_DIR}"/dist/"$OUTPUT"

rm "$TARGET_DIR" -rf
mkdir -p "$TARGET_DIR"

echo "Generating Sphinx documentation in JSON format..."
cd "$REPO_ROOT_DIR"/docs
make json
mv _build/json "$TARGET_DIR"

echo "Generating Markdown sgr reference"
python generate_reference.py sgr "$TARGET_DIR"/sgr

echo "Generating configuration reference"
python generate_reference.py config "$TARGET_DIR"/0100_config-flag-reference.mdx

# Temporarily disabled: these take way too much time and aren't used by the website.
# echo "Building Asciinema casts"
# TARGET_DIR=$TARGET_DIR "$CI_DIR"/rebuild_asciicasts.sh

echo "Archiving the bundle $OUTPUT.tar.gz"
cd "$TARGET_DIR"/..
tar -czf "$OUTPUT".tar.gz "$OUTPUT"

echo "All done."
