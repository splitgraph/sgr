#!/bin/bash

for i in `seq 10` ; do
  SG_CONFIG_FILE=test/resources/.sgconfig python -c "import test.splitgraph.conftest as c; c.healthcheck()"
  result=$?
  if [ $result -eq 0 ] ; then
    echo "Test architecture ready, let's roll!" >&2
    exit 0
  fi
  sleep 1
done
echo "Timeout bringing up the architecture" >&2
exit 1
