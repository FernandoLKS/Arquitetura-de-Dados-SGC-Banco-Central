FROM apache/airflow:3.0.6

USER root

RUN apt-get update \
    && apt-get install -y docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow