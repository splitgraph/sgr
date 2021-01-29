#!/bin/bash -ex

CI_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
REPO_ROOT_DIR="${CI_DIR}/.."
ASCIINEMA_CASTS=${ASCIINEMA_CASTS-"bloom-filter import-from-csv import-from-mongo dbt pg-replication postgrest push-to-other-engine splitgraph-cloud splitfiles us-election"}

# Asciicasts for which we need to log into the splitgraph registry to pull datasets
LOGIN_REQUIRED_CASTS="bloom-filter splitgraph-cloud us-election"

test -z "$TARGET_DIR" && { echo "Fatal Error: No TARGET_DIR set" ; exit 1 ; }
test -z "$SG_DEMO_KEY" && { echo "Fatal Error: No SG_DEMO_KEY set" ; exit 1 ; }
test -z "$SG_DEMO_SECRET" && { echo "Fatal Error: No SG_DEMO_SECRET set" ; exit 1 ; }


# Generate a throwaway sg config file to be used for making the demos in order to not log into
# every demo separately.
# We use the Mongo config as a template because it has the Mongo FDW handler configured.
# If there are weird config issues, maybe this is the culprit.
ASCIINEMA_CONFIG="$REPO_ROOT_DIR"/examples/asciinema.sgconfig
cp "$REPO_ROOT_DIR"/examples/import-from-mongo/.sgconfig "$ASCIINEMA_CONFIG"
SG_CONFIG_FILE=$ASCIINEMA_CONFIG sgr --verbosity DEBUG cloud login-api --api-key "$SG_DEMO_KEY" --api-secret "$SG_DEMO_SECRET"

# TODO the us-election asciicast requires scipy for the last part, consider replacing
# dbt required by the dbt example
pip install scipy dbt==0.18.0

cd "$REPO_ROOT_DIR"/examples
for dir in $ASCIINEMA_CASTS; do
  # shellcheck disable=SC2046
  ./rebuild_asciinema.py "$dir" \
    --skip-gif \
    --extra-gif-args "-S0.5 -s1 -t splitgraph" \
    --gif-stylesheet ./asciinema-player.css \
    $( echo "$LOGIN_REQUIRED_CASTS" | grep -F -q "$dir" && echo "--config $ASCIINEMA_CONFIG" ) \
    --output-path "$TARGET_DIR"/asciinema/
done

rm "$ASCIINEMA_CONFIG"
