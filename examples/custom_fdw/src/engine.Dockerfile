FROM splitgraph/engine:stable

RUN mkdir /hn_fdw
COPY ./hn_fdw /hn_fdw/hn_fdw
COPY .sgconfig /.sgconfig
ENV PYTHONPATH $PYTHONPATH:/hn_fdw
