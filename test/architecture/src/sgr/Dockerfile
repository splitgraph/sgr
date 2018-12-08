FROM python:3.6

RUN pip install poetry

# ADD pyproject.toml poetry.lock setup.cfg splitgraph.spec /splitgraph/

ADD test/architecture/src/sgr/entrypoint.sh /run/entrypoint.sh

# ADD splitgraph /splitgraph/splitgraph

VOLUME /src

# VOLUME /splitgraph/README.md
# VOLUME /splitgraph/LICENSE.md
# VOLUME /splitgraph/splitgraph
# VOLUME /splitgraph/pyproject.toml
# VOLUME /splitgraph/poetry.lock
# VOLUME /splitgraph/setup.cfg
# VOLUME /splitgraph/splitgraph.spec

VOLUME /venv/

WORKDIR /src/

RUN poetry config settings.virtualenvs.create true
RUN poetry config settings.virtualenvs.path "/venv"

# ENTRYPOINT ["/bin/bash"]

ENTRYPOINT ["/run/entrypoint.sh"]

# RUN poetry config settings.virtualenvs.create true
# RUN poetry config settings.virtualenvs.path "/venv"
# RUN poetry install

# ENTRYPOINT ["poetry", "run", "sgr"]
