
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)


# ============================================================
# Configuration
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
JAR_PATH = PROJECT_ROOT / "mysql-connector-j-8.0.32.jar"

MYSQL_USER = os.getenv("MYSQL_USER", "olympic")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is not set")

MYSQL_URL = (
    "jdbc:mysql://localhost:3307/olympic"
    "?useUnicode=true&characterEncoding=UTF-8"
    "&serverTimezone=UTC&allowPublicKeyRetrieval=true&useSSL=false"
)

MYSQL_OUTPUT_TABLE = "athlete_enriched_agg"

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "athlete_event_results"
KAFKA_OUTPUT_TOPIC = "athlete_enriched_agg"

SPARK_KAFKA_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
)

CHECKPOINT_DIR = str(
    PROJECT_ROOT / "checkpoints" / "olympic_streaming"
)


def main():

    if not JAR_PATH.is_file():
        raise FileNotFoundError(
            f"MySQL JDBC driver not found: {JAR_PATH}"
        )

    spark = (
        SparkSession.builder
        .appName("OlympicStructuredStreaming")
        .master("local[*]")
        .config("spark.jars", str(JAR_PATH))
        .config("spark.jars.packages", SPARK_KAFKA_PACKAGE)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:

        # Step 6. Read and Parse Kafka Streaming Data

        event_schema = StructType([
            StructField("athlete_id", IntegerType(), True),
            StructField("sport", StringType(), True),
            StructField("event", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("country_noc", StringType(), True),
        ])

        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_SERVER)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )

        events_stream = (
            kafka_stream
            .select(
                F.from_json(
                    F.col("value").cast("string"),
                    event_schema,
                ).alias("data")
            )
            .select("data.*")
            .filter(F.col("athlete_id").isNotNull())
        )

        print("\nParsed streaming schema:")
        events_stream.printSchema()

        # Step 7. Read and Clean Athlete Data from MySQL

        athletes_df = (
            spark.read
            .format("jdbc")
            .option("url", MYSQL_URL)
            .option("dbtable", "athlete_bio")
            .option("user", MYSQL_USER)
            .option("password", MYSQL_PASSWORD)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )

        athletes_clean = (
            athletes_df
            .withColumn(
                "height",
                F.expr("try_cast(trim(height) AS DOUBLE)")
            )
            .withColumn(
                "weight",
                F.expr("try_cast(trim(weight) AS DOUBLE)")
            )
            .filter(
                F.col("height").isNotNull()
                & F.col("weight").isNotNull()
                & ~F.isnan("height")
                & ~F.isnan("weight")
                & F.col("height").between(1, 300)
                & F.col("weight").between(1, 300)
            )
            .select(
                "athlete_id",
                "sex",
                "height",
                "weight",
            )
        )

        print("\nCleaned athlete data:")
        athletes_clean.show(5, truncate=False)

        # Join Kafka Events with MySQL Athlete Data

        enriched_stream = events_stream.join(
            athletes_clean,
            on="athlete_id",
            how="inner",
        )

        print("\nEnriched streaming schema:")
        enriched_stream.printSchema()

        # Step 8. Aggregate Athlete Statistics

        aggregated_stream = (
            enriched_stream
            .groupBy(
                "sport",
                "medal",
                "sex",
                "country_noc",
            )
            .agg(
                F.avg("height").alias("avg_height"),
                F.avg("weight").alias("avg_weight"),
            )
        )

        print("\nAggregated streaming schema:")
        aggregated_stream.printSchema()

        # Step 9 and 10. Save Results to Kafka and MySQL

        def save_aggregates(batch_df, batch_id):

            result_df = (
                batch_df
                .withColumn("timestamp", F.current_timestamp())
                .persist()
            )

            try:

                row_count = result_df.count()

                print(f"\nProcessing batch: {batch_id}")
                print(f"Aggregated rows: {row_count}")

                if row_count == 0:
                    return

                print("\nAggregated results:")
                result_df.show(10, truncate=False)

                # Save aggregated results to Kafka

                kafka_output_df = result_df.select(
                    F.to_json(
                        F.struct(
                            *[
                                F.col(column)
                                for column in result_df.columns
                            ]
                        )
                    ).alias("value")
                )

                (
                    kafka_output_df.write
                    .format("kafka")
                    .option(
                        "kafka.bootstrap.servers",
                        KAFKA_SERVER,
                    )
                    .option("topic", KAFKA_OUTPUT_TOPIC)
                    .save()
                )

                print(
                    f"Batch {batch_id} saved to Kafka: "
                    f"{KAFKA_OUTPUT_TOPIC}"
                )

                # Save latest aggregated snapshot to MySQL

                (
                    result_df.write
                    .format("jdbc")
                    .option("url", MYSQL_URL)
                    .option("dbtable", MYSQL_OUTPUT_TABLE)
                    .option("user", MYSQL_USER)
                    .option("password", MYSQL_PASSWORD)
                    .option("driver", "com.mysql.cj.jdbc.Driver")
                    .mode("overwrite")
                    .save()
                )

                print(
                    f"Batch {batch_id} saved to MySQL: "
                    f"olympic.{MYSQL_OUTPUT_TABLE}"
                )

            finally:
                result_df.unpersist()

        # Start Structured Streaming

        query = (
            aggregated_stream.writeStream
            .foreachBatch(save_aggregates)
            .outputMode("complete")
            .option("checkpointLocation", CHECKPOINT_DIR)
            .trigger(availableNow=True)
            .start()
        )

        query.awaitTermination()

        print("\nStreaming pipeline completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
