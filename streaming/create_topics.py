from kafka.admin import KafkaAdminClient, NewTopic


# Step 3. Configure the Kafka connection
KAFKA_SERVER = "localhost:9092"

TOPIC_NAMES = [
    "athlete_event_results",
    "athlete_enriched_agg",
]


def create_topics():
    # Connect to the local Kafka broker
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_SERVER,
        client_id="olympic_final_project",
    )

    try:
        # List existing Kafka topics
        existing_topics = set(admin_client.list_topics())

        # Define only topics that do not yet exist
        new_topics = [
            NewTopic(
                name=topic_name,
                num_partitions=1,
                replication_factor=1,
            )
            for topic_name in TOPIC_NAMES
            if topic_name not in existing_topics
        ]

        # Step 3. Create missing Kafka topics
        if new_topics:
            admin_client.create_topics(
                new_topics=new_topics,
                validate_only=False,
            )
            print("Topics created successfully.")
        else:
            print("All topics already exist.")

        # Verify the Kafka topics
        print("\nProject Kafka topics:")

        available_topics = set(admin_client.list_topics())

        for topic in TOPIC_NAMES:
            if topic in available_topics:
                print(f"OK: {topic}")
            else:
                print(f"MISSING: {topic}")

    finally:
        admin_client.close()


if __name__ == "__main__":
    create_topics()