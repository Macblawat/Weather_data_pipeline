#!/bin/bash
set -e

airflow db init

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

airflow scheduler &

exec airflow webserver --port 8080