
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
JAR_PATH = PROJECT_ROOT / "mysql-connector-j-8.0.32.jar"

if not JAR_PATH.is_file():
    raise FileNotFoundError(f"JDBC driver not found: {JAR_PATH}")

MYSQL_USER = os.getenv("MYSQL_USER", "olympic")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is not set")

MYSQL_URL = (
    "jdbc:mysql://localhost:3307/olympic"
    "?useUnicode=true&characterEncoding=UTF-8"
    "&serverTimezone=UTC&allowPublicKeyRetrieval=true&useSSL=false"
)

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "athlete_event_results"

SPARK_KAFKA_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)


def main():
    spark = (
        SparkSession.builder
        .appName("MySQL to Kafka")
        .config("spark.jars", str(JAR_PATH))
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .getOrCreate()
    )

    try:
        df = (
            spark.read.format("jdbc")
            .option("url", MYSQL_URL)
            .option("dbtable", "athlete_event_results")
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASSWORD)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )

        kafka_df = df.select(
            F.to_json(F.struct(*df.columns)).alias("value")
        )

        (
            kafka_df.write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVER)
            .option("topic", KAFKA_TOPIC)
            .save()
        )

        print(f"Successfully sent {df.count()} records to Kafka topic: {KAFKA_TOPIC}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
