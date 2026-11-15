from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


# ============================================================
# Configuration
# ============================================================

PROJECT_DIR = "/opt/olympic_project"

SPARK_CONF = {
    "spark.sql.shuffle.partitions": "4",
}


# ============================================================
# Airflow DAG
# ============================================================

with DAG(
    dag_id="olympic_batch_data_lake",
    description="Olympic data pipeline: Bronze, Silver, Gold",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["olympics", "batch", "spark"],
) as dag:

    # Step 1. Load CSV files into the Bronze layer
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=f"{PROJECT_DIR}/batch/landing_to_bronze.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        name="OlympicLandingToBronze",
    )

    # Step 2. Clean Bronze data and create the Silver layer
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=f"{PROJECT_DIR}/batch/bronze_to_silver.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        name="OlympicBronzeToSilver",
    )

    # Step 3. Aggregate Silver data into the Gold layer
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=f"{PROJECT_DIR}/batch/silver_to_gold.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        name="OlympicSilverToGold",
    )

    # Define task execution order
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
