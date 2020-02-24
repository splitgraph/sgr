ARG DOCKER_REPO
ARG DOCKER_TAG
FROM ${DOCKER_REPO}/engine:${DOCKER_TAG}

RUN echo "port = 5431" >> /etc/postgresql/postgresql.conf
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
