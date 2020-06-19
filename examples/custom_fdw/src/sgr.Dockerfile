FROM python:3.8.2-slim

RUN pip3 install splitgraph

# Copy the Python code into the container and add it to the PYTHONPATH manually
RUN mkdir /hn_fdw
COPY ./hn_fdw /hn_fdw/hn_fdw
COPY .sgconfig /.sgconfig
ENV PYTHONPATH $PYTHONPATH:/hn_fdw

ENTRYPOINT ["/bin/bash"]
