import shutil
from pathlib import Path

from pyspark.sql import SparkSession


# ============================================================
# Configuration
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SOURCE_DIR = PROJECT_ROOT / "data"
LANDING_DIR = PROJECT_ROOT / "landing"
BRONZE_DIR = PROJECT_ROOT / "bronze"

TABLES = [
    "athlete_bio",
    "athlete_event_results",
]


def main():
    # Initialize SparkSession
    spark = (
        SparkSession.builder
        .appName("OlympicLandingToBronze")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Step 1. Copy source CSV files to the Landing layer
        LANDING_DIR.mkdir(parents=True, exist_ok=True)

        for table in TABLES:
            source_path = SOURCE_DIR / f"{table}.csv"
            landing_path = LANDING_DIR / f"{table}.csv"

            if not source_path.is_file():
                raise FileNotFoundError(source_path)

            shutil.copyfile(source_path, landing_path)

            # Step 2. Read the CSV file using Spark
            df = (
                spark.read
                .option("header", True)
                .csv(str(landing_path))
            )

            # Step 3. Save raw data as Parquet
            output_path = BRONZE_DIR / table

            (
                df.write
                .mode("overwrite")
                .parquet(str(output_path))
            )

            print(f"\nBronze table: {table}")
            print(f"Total rows: {df.count()}")
            df.show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
