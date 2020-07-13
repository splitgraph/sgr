##
# mongoorigin
##
FROM mongo:3.6.5-jessie

ENV ORIGIN_USER docker
ENV ORIGIN_PASS docker
ENV ORIGIN_MONGO_DB origindb

ADD start.sh /start.sh
RUN chmod a+x /start.sh

VOLUME /src

CMD ["/start.sh"]

