from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# Configuration
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_DIR = PROJECT_ROOT / "silver"
GOLD_DIR = PROJECT_ROOT / "gold"


def main():
    # Initialize SparkSession
    spark = (
        SparkSession.builder
        .appName("OlympicSilverToGold")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Step 1. Read Silver datasets
        bio_df = spark.read.parquet(
            str(SILVER_DIR / "athlete_bio")
        )

        events_df = spark.read.parquet(
            str(SILVER_DIR / "athlete_event_results")
        )

        # Step 2. Filter invalid athlete measurements
        athletes_clean = (
            bio_df
            .filter(
                F.col("athlete_id").isNotNull()
                & F.col("height").isNotNull()
                & F.col("weight").isNotNull()
                & ~F.isnan("height")
                & ~F.isnan("weight")
                & (F.col("height") > 0)
                & (F.col("height") < float("inf"))
                & F.col("weight").between(1, 300)
            )
            .select("athlete_id", "sex", "height", "weight")
        )

        # Step 3. Join event results with athlete data
        enriched_df = (
            events_df
            .filter(F.col("athlete_id").isNotNull())
            .join(
                athletes_clean,
                on="athlete_id",
                how="inner",
            )
        )

        # Step 4. Calculate aggregated statistics
        gold_df = (
            enriched_df
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
            .withColumn(
                "timestamp",
                F.current_timestamp(),
            )
        )

        # Step 5. Save the Gold dataset as Parquet
        output_path = GOLD_DIR / "avg_stats"

        (
            gold_df.write
            .mode("overwrite")
            .parquet(str(output_path))
        )

        # Step 6. Verify the saved Gold dataset
        result_df = spark.read.parquet(str(output_path))

        print("\nGold table: avg_stats")
        print(f"Total groups: {result_df.count()}")

        result_df.printSchema()
        result_df.show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()