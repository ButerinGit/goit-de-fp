
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = PROJECT_ROOT / "bronze"
SILVER_DIR = PROJECT_ROOT / "silver"

WEIGHT_RANGE = (
    r"^\s*(\d+(?:\.\d+)?)\s*[-–—]\s*"
    r"(\d+(?:\.\d+)?)\s*$"
)


def main():
    spark = (
        SparkSession.builder
        .appName("OlympicBronzeToSilver")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        bio_df = spark.read.parquet(
            str(BRONZE_DIR / "athlete_bio")
        )

        events_df = spark.read.parquet(
            str(BRONZE_DIR / "athlete_event_results")
        )

        # Clean athlete biography
        raw_weight = F.trim(F.col("weight"))

        # Convert decimal comma to decimal point: 77,5 -> 77.5
        weight_clean = F.regexp_replace(
            raw_weight,
            r"^(\d+),(\d+)$",
            "$1.$2"
        )

        lower_weight = F.regexp_extract(
            weight_clean, WEIGHT_RANGE, 1
        ).cast("double")

        upper_weight = F.regexp_extract(
            weight_clean, WEIGHT_RANGE, 2
        ).cast("double")

        weight_value = (
            F.when(
                weight_clean.rlike(WEIGHT_RANGE),
                (lower_weight + upper_weight) / 2
            )
            .when(
                weight_clean.rlike(r"^\d+(?:\.\d+)?$"),
                weight_clean.cast("double")
            )
            .otherwise(F.lit(None).cast("double"))
        )

        bio_silver = (
            bio_df
            .withColumn(
                "athlete_id",
                F.trim(F.col("athlete_id")).cast("int")
            )
            .withColumn("sex", F.trim(F.col("sex")))
            .withColumn(
                "height",
                F.trim(F.col("height")).cast("double")
            )
            .withColumn("weight", weight_value)
            .dropDuplicates()
        )

        # Clean athlete event results
        events_silver = (
            events_df
            .withColumn(
                "athlete_id",
                F.trim(F.col("athlete_id")).cast("int")
            )
            .withColumn("sport", F.trim(F.col("sport")))
            .withColumn("event", F.trim(F.col("event")))
            .withColumn(
                "country_noc",
                F.trim(F.col("country_noc"))
            )
            .withColumn(
                "medal",
                F.when(
                    F.lower(F.trim(F.col("medal"))).isin(
                        "", "null", "nan"
                    ),
                    F.lit(None).cast("string")
                ).otherwise(F.trim(F.col("medal")))
            )
            .dropDuplicates()
        )

        # Save Silver tables
        for table, df in [
            ("athlete_bio", bio_silver),
            ("athlete_event_results", events_silver),
        ]:
            output_path = SILVER_DIR / table

            df.write.mode("overwrite").parquet(
                str(output_path)
            )

            print(f"\nSilver table: {table}")
            print(f"Total rows: {df.count()}")
            df.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
