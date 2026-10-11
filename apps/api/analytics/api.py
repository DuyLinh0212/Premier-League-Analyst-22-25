"""Database-backed APIs for unified reports and saved user analyses."""
import json
import uuid
from datetime import date
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, ConfigDict
from typing import Literal
from . import codec, neon_store
from .players import valuation, similar

router = APIRouter()


def _db():
    return neon_store


def get_report(kind, filters=None, run_id=None):
    try:
        return _db().report(kind, filters, run_id)
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc


def payload(kind, filters=None, run_id=None):
    try:
        data, meta = get_report(kind, filters, run_id)
        return {**data, '_publication': meta} if isinstance(data, dict) else data
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get('/outlook')
def outlook(season: Literal['2022-2023', '2023-2024', '2024-2025'] = '2024-2025', window: int = Query(3, ge=1, le=3), run_id: str | None = None):
    return payload('outlook', {'season': season, 'window': window}, run_id)


@router.get('/phases')
def phases(run_id: str | None = None):
    return payload('phases', run_id=run_id)


@router.get('/transfers')
def transfers(season: str | None = None, club_id: str | None = None, run_id: str | None = None):
    report = payload('transfers', run_id=run_id)
    for name in ('net_spend_sql', 'roi_sql'):
        report[name] = [r for r in report[name] if (not season or r['season'] == season) and (not club_id or r['club_id'] == club_id)]
    return report


@router.get('/rfm/{method}')
def rfm(method: Literal['gmm', 'rules'], run_id: str | None = None):
    return payload('rfm_' + method, run_id=run_id)


@router.get('/players')
def players(season: str | None = None, position: str | None = None, search: str = '', run_id: str | None = None):
    return [p for p in payload('players', run_id=run_id) if (not season or p['season'] == season) and (not position or p['pos'] == position) and search.casefold() in p['name'].casefold()]


def save_user_analysis(kind, parameters, result, simulated=False):
    return neon_store.save_user_analysis(kind, parameters, result, simulated)


@router.get('/players/similar')
def recommendations(player_id: str, season: str | None = None, limit: int = Query(6, ge=1, le=30)):
    try:
        parameters = {'player_id': player_id, 'season': season, 'limit': limit}
        return save_user_analysis('player_similarity', parameters, similar(payload('players'), player_id, season, limit))
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


class ValuationRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    age: int = Field(ge=15, le=50)
    position: Literal['FW', 'MF', 'DF', 'GK']
    market_value_mil: float = Field(ge=0, le=1000, allow_inf_nan=False)
    goals_assists: int = Field(ge=0, le=150)
    minutes: int = Field(ge=0, le=10000)
    tier: Literal['tier1', 'tier2', 'tier3'] = 'tier2'


@router.post('/valuation')
def value_player(request: ValuationRequest):
    params = request.model_dump()
    return save_user_analysis('valuation', params, valuation(**params), True)


class SimulationRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    batch_size: int = Field(5, ge=1, le=100)
    season: str = Field('2025-2026', pattern=r'^20\d{2}-20\d{2}$')
    seed: int = Field(42, ge=0, le=2147483647)


@router.post('/transfers/simulate')
def simulate(request: SimulationRequest):
    from .simulator import generate_simulated_transfers
    from kafka import KafkaProducer
    import os
    if int(request.season[5:]) != int(request.season[:4]) + 1 or int(request.season[:4]) < 2025:
        raise HTTPException(422, 'Mùa mô phỏng phải bắt đầu từ 2025 và gồm hai năm liên tiếp')
    records = generate_simulated_transfers(**request.model_dump())
    for row in records:
        row.update({'event_id': uuid.uuid4().hex, 'schema_version': 1, 'is_simulated': True, 'source': 'Danh seeded simulation'})
    saved = save_user_analysis('transfer_simulation', request.model_dump(), {'records': records, 'is_simulated': True}, True)
    producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'], acks='all', retries=3,
                             value_serializer=lambda value: codec.encode(value).encode(), max_block_ms=5000)
    try:
        for row in records:
            producer.send(os.getenv('TRANSFER_TOPIC', 'fact_transfers_topic'), key=row['event_id'].encode(), value=row).get(timeout=10)
    finally:
        producer.close(timeout=5)
    return {**saved, 'streaming': 'queued'}


@router.get('/streaming/transfers')
def streamed_transfers(limit: int = Query(100, ge=1, le=1000)):
    return neon_store.latest_records('transfer_simulation', 'records', limit)


@router.get('/reports')
def history(day: date | None = Query(None, alias='date'), limit: int = Query(500, ge=1, le=1000)):
    return _db().runs(limit, day)


@router.get('/dashboard')
def dashboard(run_id: str | None = None):
    return payload('dashboard',run_id=run_id)


@router.get('/pipeline')
def pipeline(day: date | None = Query(None, alias='date'), limit: int = Query(1000, ge=1, le=2000)):
    return neon_store.pipeline(limit, day)


@router.get('/reports/{run_id}/{kind}')
def historical_report(run_id: str, kind: str):
    try:
        return [neon_store.report_by_run(run_id, kind)]
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get('/data/catalog')
def catalog():
    rows = neon_store.catalog()
    return [{**r, 'metadata': json.loads(r['metadata']) if isinstance(r.get('metadata'), str) else r.get('metadata', {})} for r in rows]


@router.get('/data/rows')
def data_rows(table: str, offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
    try:
        return neon_store.table_rows(table, offset=offset, limit=limit)
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
