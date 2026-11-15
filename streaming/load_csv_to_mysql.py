
from pathlib import Path
import getpass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
JAR_PATH = PROJECT_ROOT / "mysql-connector-j-8.0.32.jar"

MYSQL_URL = (
    "jdbc:mysql://localhost:3307/olympic"
    "?useUnicode=true&characterEncoding=UTF-8"
    "&serverTimezone=UTC&allowPublicKeyRetrieval=true&useSSL=false"
)

MYSQL_USER = "olympic"


def main():
    if not JAR_PATH.is_file():
        raise FileNotFoundError(f"JDBC driver not found: {JAR_PATH}")

    password = getpass.getpass("MySQL password: ")

    spark = (
        SparkSession.builder
        .appName("OlympicCSVToMySQL")
        .master("local[*]")
        .config("spark.jars", str(JAR_PATH))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        files = {
            "athlete_bio": DATA_DIR / "athlete_bio.csv",
            "athlete_event_results": DATA_DIR / "athlete_event_results.csv",
        }

        for table_name, file_path in files.items():
            if not file_path.is_file():
                raise FileNotFoundError(file_path)

            print(f"\nReading: {file_path.name}")

            df = (
                spark.read
                .option("header", "true")
                .option("multiLine", "true")
                .option("encoding", "UTF-8")
                .option("inferSchema", "false")
                .csv(str(file_path))
            )

            if "athlete_id" not in df.columns:
                raise ValueError(
                    f"athlete_id column missing in {file_path.name}"
                )

            df = df.withColumn(
                "athlete_id",
                F.trim(F.col("athlete_id")).cast("int")
            )

            row_count = df.count()

            print(f"Rows loaded from CSV: {row_count}")
            print(f"Writing MySQL table: {table_name}")

            (
                df.coalesce(2)
                .write
                .format("jdbc")
                .option("url", MYSQL_URL)
                .option("dbtable", table_name)
                .option("user", MYSQL_USER)
                .option("password", password)
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("batchsize", "1000")
                .mode("overwrite")
                .save()
            )

            print(f"Successfully saved: {table_name}")

        print("\nBoth CSV files were loaded into MySQL.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
