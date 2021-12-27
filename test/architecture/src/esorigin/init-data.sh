#!/bin/bash

curl -XPUT localhost:9200/account?pretty -H 'Content-Type: application/json' -d'
{
 "mappings": {
    "properties": {
      "account_number": {"type": "integer"},
      "balance": {"type": "integer"},
      "firstname": {"type": "keyword"},
      "lastname": {"type": "keyword"},
      "age": {"type": "integer"},
      "gender": {"type": "keyword"},
      "address": {"type": "keyword"},
      "employer": {"type": "keyword"},
      "email": {"type": "keyword"},
      "city": {"type": "keyword"},
      "state": {"type": "keyword"}
   }
  }
}
'

curl -XPUT http://localhost:9200/account/_bulk -H 'Content-Type: application/json' \
--data-binary @/accounts.json
