##
# pgorigin
##
FROM ubuntu:18.04

RUN apt-get update -qq && apt-get install -y postgresql-10 postgresql-server-dev-10 && apt-get clean

ENV ORIGIN_USER docker
ENV ORIGIN_PASS docker
ENV ORIGIN_PG_DB origindb

# Open Postgres ports
RUN mkdir -p /etc/postgresql/10/main
RUN echo "host    all             all             0.0.0.0/0               md5" >> /etc/postgresql/10/main/pg_hba.conf
RUN echo "listen_addresses = '*'" >> /etc/postgresql/10/main/postgresql.conf
RUN echo "port = 5432" >> /etc/postgresql/10/main/postgresql.conf

EXPOSE 5432

ADD start.sh /start.sh
RUN chmod a+x /start.sh

VOLUME /src

CMD ["/start.sh"]

