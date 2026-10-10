"""Periodically fetch a full-season CSV and publish it as one Kafka event."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

from league_table import next_season, parse_and_validate_csv, validate_season


TOPIC = "league_table_simulated"
DEFAULT_CHECKPOINT = Path(__file__).resolve().parent / "state" / "producer_checkpoint.json"


def parse_interval(value: str) -> float:
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([sm])", value)
    if not match:
        raise ValueError("interval must be a positive value followed by s or m, e.g. 30s or 2m")
    seconds = float(match.group(1)) * (60 if match.group(2) == "m" else 1)
    if seconds <= 0:
        raise ValueError("interval must be positive")
    return seconds


def read_checkpoint(path: Path) -> str | None:
    if not path.exists():
        return None
    data = json.loads(path.read_text(encoding="utf-8"))
    season = data["last_published_season"]
    validate_season(season)
    return season


def save_checkpoint(path: Path, season: str) -> None:
    validate_season(season)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump({"last_published_season": season}, handle)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def latest_kafka_season(consumer, topic: str) -> str | None:
    """Read the last record from the required single-partition topic."""
    from kafka import TopicPartition

    partitions = consumer.partitions_for_topic(topic)
    if partitions != {0}:
        raise RuntimeError(f"Topic {topic} must exist with exactly one partition; got {partitions}")
    partition = TopicPartition(topic, 0)
    consumer.assign([partition])
    end = consumer.end_offsets([partition])[partition]
    if end == 0:
        return None
    consumer.seek(partition, end - 1)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        records = consumer.poll(timeout_ms=1000, max_records=1)
        for record in records.get(partition, []):
            if record.offset == end - 1:
                event = json.loads(record.value.decode("utf-8"))
                season = event["season"]
                validate_season(season)
                return season
    raise RuntimeError("Could not read the topic's last event; refusing to advance season")


def determine_next_season(start_season: str, local: str | None, remote: str | None) -> str:
    validate_season(start_season)
    if local and remote and validate_season(local) > validate_season(remote):
        raise RuntimeError("Producer checkpoint is ahead of Kafka; inspect broker data before resuming")
    if local and not remote:
        raise RuntimeError("Kafka topic is empty but producer checkpoint exists; inspect broker data")
    latest = max((value for value in (local, remote) if value), key=validate_season, default=None)
    if latest is None:
        return start_season
    return max(start_season, next_season(latest), key=validate_season)


def fetch_csv(api_url: str, season: str) -> str:
    separator = "&" if "?" in api_url else "?"
    url = api_url + separator + urlencode({"season": season})
    with urlopen(url, timeout=20) as response:
        csv_text = response.read().decode("utf-8-sig")
    parse_and_validate_csv(csv_text)
    return csv_text


def make_event(season: str, csv_text: str) -> bytes:
    validate_season(season)
    parse_and_validate_csv(csv_text)
    payload = {
        "season": season,
        "csv": csv_text,
        "sha256": hashlib.sha256(csv_text.encode("utf-8")).hexdigest(),
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def publish_one(producer, api_url: str, topic: str, season: str, checkpoint: Path) -> None:
    csv_text = fetch_csv(api_url, season)
    result = producer.send(topic, key=season.encode("ascii"), value=make_event(season, csv_text))
    metadata = result.get(timeout=30)
    if metadata.partition != 0:
        raise RuntimeError("Event was not written to the expected single partition")
    save_checkpoint(checkpoint, season)
    print(f"Published {season}: {len(csv_text.encode('utf-8'))} bytes, offset {metadata.offset}", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default=os.getenv("LEAGUE_API_URL", "http://localhost:8000/league-table.csv"))
    parser.add_argument("--bootstrap-server", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", TOPIC))
    parser.add_argument("--start-season", default=os.getenv("START_SEASON", "2025-2026"))
    parser.add_argument("--interval", default=os.getenv("POLL_INTERVAL", "60s"))
    parser.add_argument("--checkpoint-file", type=Path, default=Path(os.getenv("PRODUCER_CHECKPOINT", str(DEFAULT_CHECKPOINT))))
    parser.add_argument("--count", type=int, default=0, help="Number of seasons to publish; 0 means indefinitely")
    args = parser.parse_args(argv)
    try:
        validate_season(args.start_season)
        interval = parse_interval(args.interval)
        if args.count < 0:
            raise ValueError("count cannot be negative")
    except ValueError as exc:
        parser.error(str(exc))

    from kafka import KafkaConsumer, KafkaProducer

    consumer = KafkaConsumer(bootstrap_servers=args.bootstrap_server, enable_auto_commit=False)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )
    try:
        remote = latest_kafka_season(consumer, args.topic)
        local = read_checkpoint(args.checkpoint_file)
        season = determine_next_season(args.start_season, local, remote)
        published = 0
        while args.count == 0 or published < args.count:
            started = time.monotonic()
            try:
                publish_one(producer, args.api_url, args.topic, season, args.checkpoint_file)
            except Exception as exc:
                print(f"Publish {season} failed; retrying the same season: {exc}", file=sys.stderr, flush=True)
                time.sleep(interval)
                continue
            published += 1
            season = next_season(season)
            if args.count == 0 or published < args.count:
                time.sleep(max(0, interval - (time.monotonic() - started)))
    finally:
        producer.close(timeout=30)
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
