"""Publish one deterministic simulated league season for HDFS/Airflow CI."""
from __future__ import annotations

import os

from kafka import KafkaProducer

from league_table import generate_csv
from producer import TOPIC, make_event


def main() -> None:
    season = os.getenv("CI_SIMULATED_SEASON", "2025-2026")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv("KAFKA_TOPIC", TOPIC)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )
    try:
        csv_text = generate_csv(season)
        record = producer.send(topic, key=season.encode("ascii"), value=make_event(season, csv_text))
        metadata = record.get(timeout=30)
        print(
            f"Đã gửi dữ liệu mô phỏng mùa {season} vào Kafka topic {topic}; "
            f"partition={metadata.partition}, offset={metadata.offset}",
            flush=True,
        )
    finally:
        producer.close(timeout=30)


if __name__ == "__main__":
    main()
