
import time

from kafka.admin import KafkaAdminClient


# Configuration
KAFKA_SERVER = "localhost:9092"

PROJECT_TOPICS = [
    "athlete_event_results",
    "athlete_enriched_agg",
]


def reset_topics():
    # Confirm deletion of test Kafka messages
    confirmation = input(
        "Delete project Kafka topics and their messages? "
        "Type RESET: "
    )

    if confirmation != "RESET":
        print("Operation cancelled.")
        return

    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_SERVER,
        client_id="olympic_topic_reset",
    )

    try:
        # Find only the existing project topics
        existing_topics = set(admin_client.list_topics())

        topics_to_delete = [
            topic for topic in PROJECT_TOPICS
            if topic in existing_topics
        ]

        if not topics_to_delete:
            print("No project topics found.")
            return

        # Delete project topics and their test messages
        admin_client.delete_topics(
            topics=topics_to_delete,
            timeout_ms=30000,
        )

        # Wait until topic deletion is visible
        deadline = time.monotonic() + 45

        while time.monotonic() < deadline:
            remaining = (
                set(admin_client.list_topics())
                & set(topics_to_delete)
            )

            if not remaining:
                print("Project Kafka topics deleted successfully.")
                return

            time.sleep(1)

        raise TimeoutError(
            "Kafka topic deletion is not yet complete."
        )

    finally:
        admin_client.close()


if __name__ == "__main__":
    reset_topics()
