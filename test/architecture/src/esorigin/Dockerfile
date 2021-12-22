##
# esorigin
# Build seed data into our image for tests; appropriated from:
# https://stackoverflow.com/questions/35526532/how-to-add-an-elasticsearch-index-during-docker-build
##
FROM elasticsearch:7.16.1

RUN mkdir /data && \
    chown -R elasticsearch:elasticsearch /data && \
    echo 'path.data: /data' >> config/elasticsearch.yml && \
    echo 'discovery.type: "single-node"' >> config/elasticsearch.yml && \
    echo "xpack.security.enabled: false" >> config/elasticsearch.yml && \
    echo 'cluster.routing.allocation.disk.watermark.flood_stage: "99%"' >> config/elasticsearch.yml && \
    echo 'cluster.routing.allocation.disk.watermark.high: "99%"' >> config/elasticsearch.yml

ADD https://raw.githubusercontent.com/vishnubob/wait-for-it/e1f115e4ca285c3c24e847c4dd4be955e0ed51c2/wait-for-it.sh /utils/wait-for-it.sh

COPY accounts.json /accounts.json
COPY init-data.sh /init-data.sh
RUN chmod a+x /init-data.sh
RUN /usr/local/bin/docker-entrypoint.sh elasticsearch -p /tmp/epid & /bin/bash /utils/wait-for-it.sh -t 0 localhost:9200 -- /init-data.sh; kill $(cat /tmp/epid) && wait $(cat /tmp/epid); exit 0;
