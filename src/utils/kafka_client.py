import os
import json
import time
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
GROUP_ID = os.getenv("KAFKA_GROUP_ID")
raw_topic = os.getenv("KAFKA_RAW_TOPIC")
rich_topic = os.getenv("KAFKA_RICH_TOPIC")
producer = None


def _retry_until_available(factory, client_name, retries=30, delay=2):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            return factory()
        except NoBrokersAvailable as error:
            last_error = error
            print(
                f"{client_name} not ready yet, retrying "
                f"({attempt}/{retries}) in {delay}s..."
            )
            time.sleep(delay)

    raise last_error


def create_raw_consumer():
    return _retry_until_available(
        lambda: KafkaConsumer(
            raw_topic,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=GROUP_ID,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
        ),
        "Kafka consumer",
    )

def create_producer():
    return _retry_until_available(
        lambda: KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        ),
        "Kafka producer",
    )

def send_rich_event(event):
    global producer
    if producer is None:
        producer = create_producer()

    producer.send(
        topic=rich_topic,
        value=event
    )
    producer.flush()
