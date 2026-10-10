import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from api import league_table_csv
from league_table import CSV_COLUMNS, generate_csv, parse_and_validate_csv
from producer import (
    determine_next_season, make_event, parse_interval, publish_one, read_checkpoint,
)
from streaming_spark import FACT_SCHEMA, fact_rows


class SimulationTests(unittest.TestCase):
    def test_two_full_seasons_are_deterministic_and_consistent(self):
        first = generate_csv("2025-2026")
        self.assertEqual(first, generate_csv("2025-2026"))
        second = generate_csv("2026-2027")
        self.assertNotEqual(first, second)
        for content in (first, second):
            rows = parse_and_validate_csv(content)
            self.assertEqual(list(rows[0]), CSV_COLUMNS)
            self.assertEqual(sum(int(row["MP"]) for row in rows), 760)
            self.assertEqual(sum(int(row["W"]) for row in rows), sum(int(row["L"]) for row in rows))

    def test_api_response_and_fact_schema(self):
        response = league_table_csv("2025-2026")
        self.assertEqual(response.status_code, 200)
        rows = parse_and_validate_csv(response.body.decode("utf-8"))
        facts = fact_rows(rows)
        self.assertEqual(len(facts), 20)
        self.assertEqual(len(FACT_SCHEMA.fields), 12)  # season is the Parquet partition
        self.assertEqual({row[0] for row in facts}, {
            "ARS", "AVL", "BOU", "BRE", "BHA", "CHE", "CRY", "EVE", "FUL", "IPS",
            "LEI", "LIV", "MCI", "MUN", "NEW", "NFO", "SOU", "TOT", "WHU", "WOL",
        })

    def test_tampered_points_are_rejected(self):
        rows = list(csv.DictReader(io.StringIO(generate_csv("2025-2026"))))
        rows[0]["Pts"] = "999"
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
        with self.assertRaises(ValueError):
            parse_and_validate_csv(output.getvalue())


class ProducerTests(unittest.TestCase):
    def test_intervals_and_recovery(self):
        self.assertEqual(parse_interval("30s"), 30)
        self.assertEqual(parse_interval("2m"), 120)
        self.assertEqual(determine_next_season("2025-2026", None, None), "2025-2026")
        self.assertEqual(
            determine_next_season("2025-2026", "2025-2026", "2026-2027"),
            "2027-2028",
        )
        with self.assertRaises(RuntimeError):
            determine_next_season("2025-2026", "2026-2027", "2025-2026")

    def test_checkpoint_advances_only_after_kafka_ack(self):
        content = generate_csv("2025-2026")

        class Future:
            def __init__(self, fails=False):
                self.fails = fails

            def get(self, timeout):
                if self.fails:
                    raise RuntimeError("broker unavailable")
                return type("Metadata", (), {"partition": 0, "offset": 0})()

        class FakeProducer:
            def __init__(self):
                self.fails = True
                self.messages = []

            def send(self, topic, key, value):
                self.messages.append((topic, key, value))
                return Future(self.fails)

        with tempfile.TemporaryDirectory() as directory:
            checkpoint = Path(directory) / "state.json"
            producer = FakeProducer()
            with patch("producer.fetch_csv", return_value=content):
                with self.assertRaises(RuntimeError):
                    publish_one(producer, "http://example.test", "league_table_simulated", "2025-2026", checkpoint)
                self.assertIsNone(read_checkpoint(checkpoint))
                producer.fails = False
                publish_one(producer, "http://example.test", "league_table_simulated", "2025-2026", checkpoint)
            self.assertEqual(read_checkpoint(checkpoint), "2025-2026")
            event = json.loads(producer.messages[-1][2])
            self.assertEqual(event["season"], "2025-2026")
            self.assertEqual(event["csv"], content)
            self.assertEqual(len(event["sha256"]), 64)


if __name__ == "__main__":
    unittest.main()
