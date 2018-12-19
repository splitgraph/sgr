FROM splitgraph/driver

RUN echo "port = 5431" >> /etc/postgresql/postgresql.conf
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
