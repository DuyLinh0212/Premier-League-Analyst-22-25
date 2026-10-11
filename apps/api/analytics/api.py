"""Database-backed APIs for unified reports and saved user analyses."""
import json
import uuid
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, ConfigDict
from typing import Literal
from . import store
from .players import valuation, similar

router = APIRouter()


def _db():
    if __import__('os').getenv('NEON_DATABASE_URL') or __import__('os').getenv('DATABASE_URL'):
        from . import neon_store
        return neon_store
    return store


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
    if _db() is not store:
        return _db().save_user_analysis(kind, parameters, result, simulated)
    store.schema()
    run_id = kind + '_' + uuid.uuid4().hex
    run_key = store.begin_run(run_id, None, parameters, simulated)
    try:
        store.save_report(run_key, kind, result, parameters)
        store.finish_run(run_key, 1)
    except Exception as exc:
        store.fail_run(run_key, exc)
        raise
    return {**result, 'run_id': run_id, 'persistence': 'saved'}


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
                             value_serializer=lambda value: store.encode(value).encode(), max_block_ms=5000)
    try:
        for row in records:
            producer.send(os.getenv('TRANSFER_TOPIC', 'fact_transfers_topic'), key=row['event_id'].encode(), value=row).get(timeout=10)
    finally:
        producer.close(timeout=5)
    return {**saved, 'streaming': 'queued'}


@router.get('/streaming/transfers')
def streamed_transfers(limit: int = Query(100, ge=1, le=1000)):
    if _db() is not store:
        try:
            report, _ = _db().report('transfer_simulation', {})
            return report.get('records', [])[:limit]
        except LookupError:
            return []
    rows = store.query('MATCH (e:PLEvent {namespace:$ns,kind:"transfer",status:"complete"}) RETURN e.payload_json AS payload ORDER BY e.completed_at DESC LIMIT $limit', {'ns': store.namespace(), 'limit': limit})
    return [json.loads(r['payload']) for r in rows]


@router.get('/reports')
def history(limit: int = Query(50, ge=1, le=200)):
    return _db().runs(limit)


@router.get('/dashboard')
def dashboard(run_id: str | None = None):
    return payload('dashboard',run_id=run_id)


@router.get('/pipeline')
def pipeline():
    db = _db()
    if db is store:
        return store.query('MATCH (j:PLJob {namespace:$ns}) RETURN j.run_id AS run_id,j.stage AS stage,j.status AS status,j.started_at AS started_at,j.completed_at AS completed_at,j.error AS error ORDER BY j.started_at DESC LIMIT 100',{'ns':store.namespace()})
    return db.pipeline(100)


@router.get('/reports/{run_id}/{kind}')
def historical_report(run_id: str, kind: str):
    db = _db()
    if db is not store:
        try:
            return [db.report_by_run(run_id, kind)]
        except LookupError as exc:
            raise HTTPException(404, str(exc)) from exc
    rows = store.query('MATCH (r:PLRun {key:$run,status:"complete"})-[:PRODUCED]->(p) WHERE p.kind=$kind RETURN p.payload_json AS payload,p.filters_json AS filters', {'run': store.namespace() + ':' + run_id, 'kind': kind})
    if not rows:
        raise HTTPException(404, 'Không có báo cáo đã lưu')
    return [{'payload': json.loads(r['payload']), 'filters': json.loads(r['filters'])} for r in rows]


@router.get('/data/catalog')
def catalog():
    db = _db()
    rows = db.catalog() if db is not store else store.catalog()
    return [{**r, 'metadata': json.loads(r['metadata']) if isinstance(r.get('metadata'), str) else r.get('metadata', {})} for r in rows]


@router.get('/data/rows')
def data_rows(table: str, offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=1000)):
    try:
        db = _db()
        return db.table_rows(table, offset=offset, limit=limit) if db is not store else store.table_rows(table, offset=offset, limit=limit)
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
