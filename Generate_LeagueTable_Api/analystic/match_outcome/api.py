"""FastAPI for portable inference, examples and the unchanged KNN report."""
import csv
import hashlib
import json
import os
import uuid
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict
from kafka import KafkaProducer
from .inference import predict
from .schema import feature_schema, validate_stats
from analytics import store
from analytics.api import router, payload, save_user_analysis

ROOT = Path(__file__).resolve().parents[3]
OUTPUT = Path(os.getenv("MATCH_MODEL_DIR", str(Path(__file__).parent / "outputs")))
KNN = Path(os.getenv("KNN_OUTPUT_DIR", str(Path(__file__).parent.parent / "knn" / "outputs")))

@asynccontextmanager
async def lifespan(app):
    app.state.artifact = None
    app.state.published_version = None
    app.state.producer = None
    yield
    if app.state.producer:
        app.state.producer.close(timeout=5)

app = FastAPI(title="Football statistics", lifespan=lifespan)
app.include_router(router)

@app.exception_handler(Exception)
async def unavailable(request, exception):
    from fastapi.responses import JSONResponse
    import logging
    logging.getLogger(__name__).exception('Serving operation failed', exc_info=exception)
    return JSONResponse(status_code=503, content={'detail': 'Database hoặc dịch vụ phân tích chưa sẵn sàng. Hãy kiểm tra kết nối và chạy lại.'})

def refresh():
    artifact, metadata = store.report('model')
    app.state.artifact = artifact
    app.state.published_version = metadata['run_id']
    return app.state.artifact

def published_dir():
    refresh()
    version = getattr(app.state, "published_version", None)
    return OUTPUT / "versions" / version if version else OUTPUT

class PredictionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    home_team: str
    away_team: str
    home_stats: dict
    away_stats: dict

def matches():
    return payload('matches')

@app.get("/health")
def health():
    return {"status": "ok", "model_version": refresh()["model_version"]}

@app.get("/features")
def features():
    return feature_schema(refresh())

@app.get("/model/metrics")
def model_metrics():
    return payload('model_metrics')

@app.get("/matches")
def examples():
    refresh()
    return [{k: v for k, v in r.items() if k in {"match_id", "match_date", "season", "home_team", "away_team", "home_name", "away_name", "home_stats", "away_stats"}} for r in matches()]

@app.get("/clubs")
def clubs():
    return payload('clubs')

@app.post("/predict")
def forecast(request: PredictionRequest):
    artifact = refresh()
    club_ids = {c["id"]: c["name"] for c in clubs()}
    if request.home_team == request.away_team or request.home_team not in club_ids or request.away_team not in club_ids:
        raise HTTPException(422, "Chọn hai CLB khác nhau trong danh sách")
    try:
        event = {**request.model_dump(), "home_stats": validate_stats(request.home_stats), "away_stats": validate_stats(request.away_stats),
                 "event_id": uuid.uuid4().hex, "kind": "scenario", "schema_version": 1, "source": "manual", "is_simulated": True,
                 "home_name": club_ids[request.home_team], "away_name": club_ids[request.away_team],
                 "model_version": artifact["model_version"]}
        result = predict(event, artifact)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    try:
        if app.state.producer is None:
            app.state.producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
                acks="all", retries=3, max_block_ms=5000, request_timeout_ms=5000,
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False, allow_nan=False).encode())
        app.state.producer.send("match_statistics", key=event["event_id"].encode(), value=event).get(timeout=10)
    except Exception as exc:
        raise HTTPException(503, "Kafka chưa sẵn sàng. Hãy thử lại sau.") from exc
    saved = save_user_analysis('match_prediction', event, result, True)
    return {**saved, "prediction_id": event["event_id"], "streaming": "queued"}

def knn_rows(name):
    return payload('knn_predictions' if name == 'knn_predictions.csv' else 'knn_neighbors')

@app.get("/knn/predictions")
def knn_predictions():
    return knn_rows("knn_predictions.csv")

@app.get("/knn/neighbors/{club_id}")
def knn_neighbors(club_id: str):
    return [r for r in knn_rows("knn_neighbors.csv") if r["query_club_id"] == club_id]
