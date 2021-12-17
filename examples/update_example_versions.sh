#!/bin/bash -e

# Update poetry.lock files in all examples that use Poetry.

while IFS= read -r -d '' file
do
    path=$(dirname "$file")
    echo "Updating $path..."
    pushd "$path"
    poetry update --lock
    popd
done < <(find . -name "poetry.lock" -print0)
