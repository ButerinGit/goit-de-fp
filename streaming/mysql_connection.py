
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
JAR_PATH = PROJECT_ROOT / "mysql-connector-j-8.0.32.jar"

if not JAR_PATH.is_file():
    raise FileNotFoundError(f"JDBC driver not found: {JAR_PATH}")


MYSQL_USER = os.getenv("MYSQL_USER", "olympic")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD environment variable is not set")

jdbc_url = (
    "jdbc:mysql://localhost:3307/olympic"
    "?useUnicode=true&characterEncoding=UTF-8"
    "&serverTimezone=UTC&allowPublicKeyRetrieval=true&useSSL=false"
)

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("OlympicMySQLConnection")
    .config("spark.jars", str(JAR_PATH))
    .getOrCreate()
)

try:
    athletes_df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "athlete_bio")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .load()
    )

    print("ATHLETE BIO:")
    athletes_df.printSchema()

    athletes_clean = (
        athletes_df
        .withColumn("height", F.expr("try_cast(trim(height) AS DOUBLE)"))
        .withColumn("weight", F.expr("try_cast(trim(weight) AS DOUBLE)"))
        .filter(
            F.col("height").isNotNull()
            & F.col("weight").isNotNull()
            & ~F.isnan("height")
            & ~F.isnan("weight")
            & (F.col("height") > 0)
            & (F.col("weight") > 0)
            & (F.col("height") < float("inf"))
            & (F.col("weight") < float("inf"))
        )
    )

    print("CLEANED ATHLETE BIO:")
    athletes_clean.select(
        "athlete_id", "sex", "height", "weight", "country_noc"
    ).show(10, truncate=False)

    event_results_df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "athlete_event_results")
        .option("user", MYSQL_USER)
        .option("password", MYSQL_PASSWORD)
        .load()
    )

    print("ATHLETE EVENT RESULTS:")
    event_results_df.printSchema()

    event_results_df.select(
        "athlete_id", "sport", "event", "medal", "country_noc"
    ).show(5, truncate=40)

finally:
    spark.stop()
