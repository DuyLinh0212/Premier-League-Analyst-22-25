"""Persist Kafka prediction outcomes and replay provenance in Neon PostgreSQL."""
import hashlib
from analytics import neon_store


def schema():
    neon_store.schema()


def publish_predictions(items, run_id=None):
    if not items:
        return
    reports = []
    for event, prediction in items:
        event_id = event["event_id"]
        report_kind = "match_prediction_v2" if event.get("schema_version") == 2 else "match_prediction"
        payload = {
            "event_id": event_id,
            "model_version": prediction.get("model_version"),
            "request": event,
            "result": prediction,
        }
        reports.append((report_kind, payload, {"event_id": event_id}))
    stable_batch = hashlib.sha256("".join(sorted(event["event_id"] for event, _ in items)).encode()).hexdigest()[:24]
    neon_store.publish_reports(
        run_id or "kafka_batch_" + stable_batch,
        "kafka-match-events",
        reports,
        {"source": "Kafka", "event_count": len(reports),
         "event_ids": sorted(event["event_id"] for event, _ in items)},
    )
