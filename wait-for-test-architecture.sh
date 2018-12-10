#!/bin/bash

for i in `seq 10` ; do
  # Make sure the test drivers are up and install the audit triggers.
  SG_CONFIG_FILE=test/resources/.sgconfig sgr init
  SG_CONFIG_FILE=test/resources/.sgconfig SG_DRIVER_PORT=5431 sgr init
  # Also make sure the 3 upstream databases that we're ingesting data from are up.
  SG_CONFIG_FILE=test/resources/.sgconfig python -c "import test.splitgraph.conftest as c; c.healthcheck()"
  result=$?
  if [ $result -eq 0 ] ; then
    echo "Test architecture ready, let's roll!" >&2
    exit 0
  fi
  sleep 5
done
echo "Timeout bringing up the architecture" >&2
exit 1
