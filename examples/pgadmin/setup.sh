#!/bin/bash -e

DC="docker-compose --project-name splitgraph_example"

# Remove old example containers
$DC down -v

# Start the containers
$DC up -d

# Initialize the Splitgraph engine
sgr init

# Get pgadmin to load server configuration and credentials for the engine
# Inspired by https://github.com/MaartenSmeets/db_perftest

echo "Initializing pgAdmin and adding the engine to it..."
$DC exec pgadmin sh -c "mkdir -m 700 /var/lib/pgadmin/storage/pgadmin4_pgadmin.org && \\
  cp /tmp/pgpassfile /var/lib/pgadmin/storage/pgadmin4_pgadmin.org && \\
  chmod 600 /var/lib/pgadmin/storage/pgadmin4_pgadmin.org/pgpassfile && \\
  python /pgadmin4/setup.py --load-servers /tmp/servers.json"

echo "pgAdmin initialized."
echo "Go to http://localhost:5050 and log in with"
echo "  email: pgadmin4@pgadmin.org"
echo "  password: password"
