FROM ../base+build

deps:
    COPY pyproject.toml /deps/splitgraph/pyproject.toml
    COPY poetry.lock /deps/splitgraph/poetry.lock
    SAVE ARTIFACT deps /deps

src:
    COPY splitgraph /src/splitgraph
    COPY bin /src
    SAVE ARTIFACT src /src

venv:
    COPY +deps/* /deps
    WORKDIR /deps/splitgraph
    RUN poetry install --no-root
