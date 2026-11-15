# Olympic Data Engineering Pipelines

A university final project that processes Olympic athlete and event data using two complementary approaches: a Kafka-based streaming pipeline and an Apache Spark batch data lake orchestrated with Apache Airflow.

The project demonstrates ingestion, data-quality checks, stream–static joins, aggregation, Parquet storage and workflow orchestration. It is a learning project, not a production deployment.

## Architecture

### Part 1 — Streaming pipeline

```text
University MySQL
  ├── athlete_event_results ──> Spark JDBC producer ──> Kafka: athlete_event_results
  └── athlete_bio ───────────────────────────────────────┐
                                                        │
Kafka: athlete_event_results ──> Spark Structured Streaming
                                                        │
                                   Parse JSON → clean bio → join on athlete_id
                                                        │
                           Group by sport, medal, sex and country_noc
                                                        │
                                           foreachBatch (complete snapshot)
                                                ├──> Kafka: athlete_enriched_agg
                                                └──> Local MySQL: olympic_results.athlete_enriched_agg
```

The producer loads historical events into Kafka. The Spark Structured Streaming job processes the messages available at execution time using `availableNow=True`; it is **not** configured as an always-running live service.

### Part 2 — Batch data lake

```text
Olympic CSV files
       |
       v
Landing (original CSV copies)
       |
       v
Bronze (raw Parquet)
       |
       v
Silver (cleaned Parquet)
       |
       v
Gold (aggregated Parquet: avg_stats)
```

Apache Airflow coordinates the three batch jobs in this order:

```text
landing_to_bronze >> bronze_to_silver >> silver_to_gold
```

All three tasks use `SparkSubmitOperator`.

## Technology stack

- **Python 3.12** and **PySpark 4.2.0** for data processing
- **Apache Spark Structured Streaming** and **Apache Kafka** for streaming ingestion
- **MySQL 8** for source data access and the local streaming result table
- **Apache Parquet** for the Bronze, Silver and Gold data lake layers
- **Apache Airflow 2.11.0** with **Spark Provider 5.3.3** for batch orchestration
- **Docker / Docker Compose** for local services
- **Java 17** for Spark execution

## Project structure

```text
final_project/
├── streaming/
│   ├── mysql_connection.py      # JDBC connection and data-cleaning exploration
│   ├── create_topics.py          # Create the two Kafka topics
│   ├── mysql_to_kafka.py         # Publish MySQL events to Kafka
│   ├── streaming_pipeline.py     # Stream–static join, aggregation and output sinks
│   ├── verify_kafka.py           # Read sample messages for verification
│   ├── check_weight_ranges.py    # Inspect ranges in the original athlete CSV
│   └── reset_topics.py           # Destructive Kafka topic reset (use with care)
├── batch/
│   ├── landing_to_bronze.py
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
├── airflow/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── dags/
│       └── project_solution.py
├── data/                         # Local source CSV files; excluded from Git
├── landing/                      # Generated; excluded from Git
├── bronze/                       # Generated; excluded from Git
├── silver/                       # Generated; excluded from Git
├── gold/                         # Generated; excluded from Git
├── evidence/                     # Local screenshots; excluded from Git
├── .gitignore
└── README.md
```

The MySQL JDBC driver JAR and virtual environment are also excluded from version control.

## Data sources and preparation

The two input CSV datasets used for the batch pipeline are:

- `athlete_bio.csv`
- `athlete_event_results.csv`

They were obtained from the course dataset links:

- <https://ftp.goit.study/neoversity/athlete_bio.csv>
- <https://ftp.goit.study/neoversity/athlete_event_results.csv>

Place them in `final_project/data/` before running the batch scripts. **The current `landing_to_bronze.py` copies existing local files; it does not download them automatically.**

For the streaming pipeline, the source is a separately hosted university MySQL database containing the `athlete_bio` and `athlete_event_results` tables. Access requires valid university credentials. That database is read-only from this project's perspective.

### Data-quality rules

- **Bronze:** preserve the CSV values in Parquet without intentional cleaning.
- **Silver:** trim selected text fields, cast numeric identifiers and measurements, standardise missing medal values and remove duplicate full rows after normalisation.
- **Weight ranges:** convert values such as `70-74` kg to their midpoint (`72.0` kg). These are **estimated**, not measured, weights.
- **Gold:** keep athletes with a valid identifier, positive finite height and weight between 1 and 300 kg; join the event and biography datasets; compute mean height and weight by `sport`, `medal`, `sex` and `country_noc`.
- **Streaming:** cast height and weight from MySQL to numeric types, exclude invalid measurements and use the same 300 kg upper limit for weight. Unlike the batch CSV pipeline, streaming does **not** reconstruct source weight ranges.

The 300 kg threshold is a project-level plausibility rule for excluding visibly corrupted weights. It is not a universal physiological standard. The streaming and batch outputs therefore need not contain identical numbers of groups.

## Running the project locally

### Prerequisites

Install Docker Desktop, Java 17 and Python 3.12. In the project directory, create an isolated Python environment and install the packages used by the local scripts:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install pyspark==4.2.0 kafka-python
```

Obtain a MySQL Connector/J driver compatible with MySQL 8 and place it at the project root under the filename expected by the code: `mysql-connector-j-8.0.32.jar`.

**External services are required:**

1. A Kafka broker reachable from the host at `localhost:9092`.
2. A MySQL 8 instance reachable from the host at `localhost:3307`, with a database called `olympic_results` and credentials for writing results.
3. University MySQL credentials and network access to the source database for Part 1.

The current Airflow Compose file starts **only Airflow**. It does not provision Kafka, MySQL or the university database. These services must be set up separately. All credentials must remain outside the Git repository.

### Part 1: Streaming

Run the commands from the `final_project` directory with the Python environment activated:

```bash
python streaming/create_topics.py
python streaming/mysql_to_kafka.py
python streaming/streaming_pipeline.py
python streaming/verify_kafka.py
```

The producer and processing scripts prompt for MySQL credentials rather than embedding passwords in the source files. The verification script is currently configured to inspect the output topic `athlete_enriched_agg`.

**Important:** `mysql_to_kafka.py` republishes the full source table each time it runs. The streaming job starts from the earliest available Kafka offsets without a durable checkpoint, and `foreachBatch` publishes the latest aggregate snapshot to Kafka while overwriting the local MySQL output table. Repeated runs can therefore produce duplicate output-topic messages. `reset_topics.py` deletes Kafka topics and their messages; use it only when an intentional reset is required.

### Part 2: Batch

Run the three Spark scripts individually:

```bash
python batch/landing_to_bronze.py
python batch/bronze_to_silver.py
python batch/silver_to_gold.py
```

These create the `landing/`, `bronze/`, `silver/` and `gold/avg_stats/` directories inside `final_project`.

### Part 2: Airflow orchestration

The Airflow image uses the `airflow/Dockerfile` to install Java, PySpark and the Airflow Spark Provider. From the project root, build the image and start the provided Compose configuration:

```bash
docker build -t olympic-final-airflow:local ./airflow
docker compose -f airflow/docker-compose.yml up -d
```

Open <http://localhost:8082> and sign in using the credentials generated by Airflow's local `standalone` setup. Trigger the DAG **`olympic_batch_data_lake`** manually; it has no automatic schedule.

The Compose file mounts the project at `/opt/olympic_project` so that Spark tasks inside the Airflow container can access the same project files and output folders.

To stop the Airflow service:

```bash
docker compose -f airflow/docker-compose.yml down
```

## Observed results

The following numbers were recorded during successful local runs and Airflow execution. They are **observations from this run**, not fixed expected outputs for every dataset version or cleaning rule.

| Check | Observed result |
|---|---:|
| Bronze athlete biographies | 155,861 rows |
| Bronze event results | 316,834 rows |
| Silver athlete biographies | 155,861 rows |
| Silver event results | 315,626 rows |
| Gold aggregate groups | 10,916 |
| Streaming aggregate groups after plausibility filtering | 10,879 |
| Airflow DAG tasks completed successfully | 3 / 3 |

The Gold dataset contains `sport`, `medal`, `sex`, `country_noc`, `avg_height`, `avg_weight` and `timestamp`. The streaming result has the same aggregate fields and a calculation timestamp.

## Limitations and next steps

This is an educational implementation. In particular:

- The local infrastructure is not yet entirely self-contained in one Compose file; Kafka and MySQL are configured separately.
- The source university MySQL database is restricted and is not published with this repository.
- Streaming uses an available-now trigger and does not implement a long-running ingestion service.
- Kafka and MySQL outputs are not written as a single atomic transaction; retries and repeat runs need careful handling.
- The MySQL aggregate table is overwritten with the latest snapshot rather than updated using a transactional upsert strategy.
- Additional automated tests, data validation metrics and durable streaming checkpoint management would be appropriate for production use.

## Security

Do not commit passwords, `.env` files, virtual environments, screenshots containing credentials, database volumes or generated data. Review `git status` and `git diff --cached` before each push. Rotate any credentials that have been exposed in screenshots or shared outside a secure channel.

## Project context

Created as an MSc Data Engineering final project to demonstrate streaming and batch processing with Spark, Kafka, MySQL, Parquet, Docker and Airflow.

## Execution Evidence

### Part 1 — Streaming Pipeline

The streaming pipeline processed Olympic event records using
Apache Kafka and Spark Structured Streaming.

The resulting aggregates were written to Kafka and MySQL.

**Kafka output — sample aggregated messages:**

![Kafka output](evidence/streaming_kafka_output.png)

**MySQL aggregation statistics:**

- Total groups: 10,879
- Minimum average weight: 31 kg
- Maximum average weight: 150 kg

![MySQL statistics](evidence/streaming_mysql_stats.png)

**Sample MySQL records:**

![MySQL output](evidence/streaming_mysql_output.png)

### Part 2 — Batch Data Lake

The batch pipeline was orchestrated using Apache Airflow.
All three SparkSubmitOperator tasks completed successfully.

![Successful Airflow DAG](evidence/airflow_dag_success.png)

**Bronze layer — raw data ingestion:**

![Bronze logs](evidence/airflow_bronze_logs.png)

**Silver layer — data cleaning:**

![Silver logs](evidence/airflow_silver_logs.png)

**Gold layer — 10,916 aggregated groups:**

![Gold logs](evidence/airflow_gold_logs.png)