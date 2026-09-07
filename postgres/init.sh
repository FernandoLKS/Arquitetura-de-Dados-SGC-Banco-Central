#!/bin/bash

set -e

echo "Creating Airflow database..."

psql \
    -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "postgres" \
    -v airflow_user="$AIRFLOW_DB_USER" \
    -v airflow_password="$AIRFLOW_DB_PASSWORD" \
    -v airflow_db="$AIRFLOW_DB" \
    <<-EOSQL

    CREATE ROLE :"airflow_user"
        LOGIN
        PASSWORD :'airflow_password';

    CREATE DATABASE :"airflow_db"
        OWNER :"airflow_user";

EOSQL

echo "Airflow database created."