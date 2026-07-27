"""
Development bridge: reads events from Kafka and inserts them into
Snowflake raw tables. In production, Snowpipe Streaming would replace this.

Usage:
    python scripts/kafka_to_snowflake.py
"""

import json
import os
import sys
from datetime import datetime, timezone

import snowflake.connector
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "voltsense.charger.sessions"
CONSUMER_GROUP = "snowflake-loader"
BATCH_SIZE = 50


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="VOLTSENSE",
        warehouse="VOLTSENSE_WH",
    )


def flush_batch(cursor, rows):
    """Insert a batch of rows into the raw table."""
    if not rows:
        return

    for content, metadata in rows:
        cursor.execute(
            "INSERT INTO VOLTSENSE.RAW.RAW_CHARGER_SESSIONS (RECORD_CONTENT, RECORD_METADATA) "
            "SELECT PARSE_JSON(%s), PARSE_JSON(%s)",
            (content, metadata),
        )

def main():
    # Connect to Snowflake
    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    print("Connected\n")

    # Set up Kafka consumer
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    print(f"Subscribed to: {TOPIC}")
    print(f"Consumer group: {CONSUMER_GROUP}")
    print(f"Batch size: {BATCH_SIZE}")
    print("Waiting for messages (Ctrl+C to stop)\n")

    batch = []
    total_inserted = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No new messages - flush anything pending
                if batch:
                    flush_batch(cursor, batch)
                    total_inserted += len(batch)
                    print(f"Flushed {len(batch)} rows (total: {total_inserted})")
                    batch = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {msg.error()}")
                continue

            # Build the record content and metadata
            content = msg.value().decode("utf-8")
            metadata = json.dumps({
                "CreateTime": datetime.now(timezone.utc).isoformat(),
                "offset": msg.offset(),
                "partition": msg.partition(),
                "topic": msg.topic(),
            })

            batch.append((content, metadata))

            # Flush when batch is full
            if len(batch) >= BATCH_SIZE:
                flush_batch(cursor, batch)
                total_inserted += len(batch)
                print(f"Flushed {len(batch)} rows (total: {total_inserted})")
                batch = []

    except KeyboardInterrupt:
        # Flush remaining rows
        if batch:
            flush_batch(cursor, batch)
            total_inserted += len(batch)
        print(f"\nStopped. Total rows inserted: {total_inserted}")

    finally:
        consumer.close()
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()