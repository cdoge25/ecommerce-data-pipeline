#!/bin/bash
# MinIO Connection
airflow connections add 'minio' \
    --conn-type 'aws' \
    --conn-login 'minio' \
    --conn-password 'minio123' \
    --conn-extra '{"endpoint_url": "http://host.docker.internal:9000"}'

# Postgres Connection
airflow connections add 'postgres' \
    --conn-type 'postgres' \
    --conn-host 'host.docker.internal' \
    --conn-schema 'postgres' \
    --conn-login 'postgres' \
    --conn-password 'postgres' \
    --conn-port '5432'

# Slack Connection
airflow connections add 'slack' \
    --conn-type 'slack' \
    --conn-extra "{"token": \"${SLACK_TOKEN}\"}"

echo "Connections added successfully."