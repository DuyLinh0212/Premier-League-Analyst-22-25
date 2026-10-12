"""FastAPI for portable inference, examples and the unchanged KNN report."""
import csv
import hashlib
import json
import os
import uuid
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from typing import Literal
from datetime import date, timedelta
from kafka import KafkaProducer
from .inference import predict
from .schema import feature_schema, validate_stats, METRICS
from analytics.api import router, payload, save_user_analysis, get_report

ROOT = Path(__file__).resolve().parents[3]
OUTPUT = Path(os.getenv("MATCH_MODEL_DIR", str(ROOT / "data/models/match_outcome")))
KNN = Path(os.getenv("KNN_OUTPUT_DIR", str(ROOT / "data/models/knn")))

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
    artifact, metadata = get_report('model')
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

class PredictionRequestV2(BaseModel):
    model_config = ConfigDict(extra='forbid')
    mode: Literal['pregame', 'live'] = 'pregame'
    home_team: str
    away_team: str
    prediction_date: date | None = None
    home_goals: int = Field(0, ge=0, le=30, strict=True)
    away_goals: int = Field(0, ge=0, le=30, strict=True)
    minute: int = Field(0, ge=0, le=90, strict=True)
    stoppage_minutes: int = Field(0, ge=0, le=30, strict=True)
    finished: bool = False
    home_stats: dict = Field(default_factory=dict)
    away_stats: dict = Field(default_factory=dict)

def send_prediction(event):
    try:
        if app.state.producer is None:
            brokers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
            if not brokers:
                return 'not_configured'
            app.state.producer = KafkaProducer(bootstrap_servers=brokers,
                acks='all', retries=1, max_block_ms=1000, request_timeout_ms=2000,
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False, allow_nan=False).encode())
        app.state.producer.send('match_statistics', key=event['event_id'].encode(), value=event).get(timeout=2)
        return 'queued'
    except Exception:
        import logging
        logging.getLogger(__name__).warning('Match prediction was saved, but optional Kafka delivery is unavailable', exc_info=True)
        app.state.producer = None
        return 'unavailable'

@app.get('/prediction/config')
def prediction_config():
    from .forecast_v2 import STYLE
    artifact, metadata = get_report('forecast_v2')
    return {'model_version':artifact['model_version'],'data_through':artifact['data_through'],
            'trained_through':artifact['trained_through'],
            'minimum_date':(date.fromisoformat(artifact['trained_through'])+timedelta(days=1)).isoformat(),
            'default_date':(date.fromisoformat(artifact['data_through'])+timedelta(days=1)).isoformat(),
            'metrics':[{'key':k,'label':METRICS[k][0],'unit':METRICS[k][1],'max':METRICS[k][2] or 1000} for k in STYLE],
            '_publication':metadata}

@app.get('/model/metrics/v2')
def prediction_metrics_v2():
    return payload('forecast_v2_metrics')

@app.post('/predict/v2')
def forecast_v2(request: PredictionRequestV2):
    from .forecast_v2 import forecast, validate_request
    artifact, metadata = get_report('forecast_v2')
    try:
        params=validate_request(request.model_dump(mode='json'))
        if params['prediction_date'] is None:
            params['prediction_date']=(date.fromisoformat(artifact['data_through'])+timedelta(days=1)).isoformat()
        result=forecast(params,artifact)
    except ValueError as exc:
        raise HTTPException(422,str(exc)) from exc
    names={r[s+'_team']:r.get(s+'_name',r[s+'_team']) for r in artifact['history'] for s in ('home','away')}
    event={**params,'event_id':uuid.uuid4().hex,'kind':'scenario','schema_version':2,'source':'manual' if params['mode']=='live' else 'as_of_history',
           'is_simulated':params['mode']=='live','model_version':artifact['model_version'],'dataset_fingerprint':artifact['dataset_fingerprint'],
           'home_name':names[params['home_team']],'away_name':names[params['away_team']]}
    saved=save_user_analysis('match_prediction_v2',event,result,params['mode']=='live')
    streaming = send_prediction(event)
    return {**saved,'prediction_id':event['event_id'],'streaming':streaming,'publication_run_id':metadata['run_id']}

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
    saved = save_user_analysis('match_prediction', event, result, True)
    streaming = send_prediction(event)
    return {**saved, "prediction_id": event["event_id"], "streaming": streaming}

@app.get("/knn/predictions")
def knn_predictions():
    return payload('knn_predictions')
