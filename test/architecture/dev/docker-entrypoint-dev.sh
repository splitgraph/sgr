#!/usr/bin/env bash

main() {
    ( install_latest_sgr && clear_pycache_files ) || return 1
    return 0
}

install_latest_sgr() {
    mkdir -p /src/venvs \
        && poetry config settings.virtualenvs.create true \
        && poetry config settings.virtualenvs.path /src/venvs \
        && cd /src/splitgraph \
        && poetry install -E ingestion --develop splitgraph
}

# Because we are bind-mounting the root of the repository with /src/splitgraph
# in the container, _all_ the files are shared. This includes __pycache__ and
# .pyc files, but these files contain system-specific paths (i.e. full paths
# of modules on the host machine) that do not match docker paths. Therefore, in
# order to e.g. run tests in the docker container, we first need to clear all the
# .pyc and __pycache__ files/directories. Obviously, this will also clear them
# on the host. But since this is development, that is okay. It just means you
# cannot run the test suite in docker and on the host in parallel. No surprise.
clear_pycache_files() {
    python3 -c "import pathlib; [p.unlink() for p in pathlib.Path('.').rglob('*.py[co]')]" \
        && python3 -c "import pathlib; [p.rmdir() for p in pathlib.Path('.').rglob('__pycache__')]" \
        && return 0

    return 1
}

poetry() {
    "$HOME"/.poetry/bin/poetry "$@"
}

main "$@" || exit 1

if test "$1" = "shell" ; then
    exec "/bin/bash"
elif test "$1" = "test" ; then
    exec "$HOME"/.poetry/bin/poetry run pytest -c /sgconfig/pytest.dev.ini "$@"
else
    exec "$HOME"/.poetry/bin/poetry run sgr "$@"
fi
