FROM apache/airflow:2.11.0

# Install Java required by Apache Spark
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Install Spark and the Airflow Spark provider
USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==2.11.0" \
    "apache-airflow-providers-apache-spark==5.3.3" \
    "pyspark==4.2.0"