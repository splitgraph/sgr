#!/bin/bash

curl -XPUT --header 'Content-Type: application/json' http://localhost:9200/account/_bulk \
--data-binary @/accounts.json
