
import json
from kafka import KafkaConsumer


# Kafka connection settings
KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "athlete_enriched_agg"
MAX_MESSAGES = 10

# Column title, JSON field name, and display width
COLUMNS = [
    ("Sport", "sport", 22),
    ("Medal", "medal", 10),
    ("Sex", "sex", 10),
    ("Country", "country_noc", 12),
    ("Avg height", "avg_height", 12),
    ("Avg weight", "avg_weight", 12),
    ("Timestamp", "timestamp", 22),
]


def format_row(data):
    """Format one Kafka record as a fixed-width table row."""
    cells = []

    for _, field, width in COLUMNS:
        value = data.get(field)

        if value is None:
            text = "NULL"
        else:
            text = str(value)

        cells.append(f"{text[:width]:<{width}}")

    return " | ".join(cells)


def verify_kafka():
    """Read and display up to ten messages from Kafka."""

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=10000,
    )

    records = []

    try:
        for message in consumer:
            records.append(
                json.loads(message.value.decode("utf-8"))
            )

            if len(records) >= MAX_MESSAGES:
                break

    finally:
        consumer.close()

    print(f"\nKafka topic: {KAFKA_TOPIC}")

    if not records:
        print("No messages found in Kafka.")
        return

    header = " | ".join(
        f"{title:<{width}}"
        for title, _, width in COLUMNS
    )

    print(header)
    print("-" * len(header))

    for record in records:
        print(format_row(record))

    print(f"\nTotal messages verified: {len(records)}")


if __name__ == "__main__":
    verify_kafka()
