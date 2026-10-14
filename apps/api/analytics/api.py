"""Database-backed APIs for unified reports and saved user analyses."""
import json
from datetime import date
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, ConfigDict
from typing import Literal
from . import neon_store
from .players import valuation, similar

router = APIRouter()
RETIRED_REPORTS = {'dashboard', 'transfers', 'knn_neighbors', 'rfm_gmm', 'rfm_rules', 'transfer_simulation'}


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


@router.get('/standings')
def actual_standings(season: Literal['2022-2023', '2023-2024', '2024-2025'] = '2024-2025', run_id: str | None = None):
    return payload('actual_standings', {'season': season}, run_id)


@router.get('/phases')
def phases(run_id: str | None = None):
    return payload('phases', run_id=run_id)


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


@router.get('/reports')
def history(day: date | None = Query(None, alias='date'), limit: int = Query(500, ge=1, le=1000)):
    return _db().runs(limit, day)


@router.get('/match-results')
def match_results(run_id: str | None = None):
    return payload('match_results', run_id=run_id)


@router.get('/pipeline')
def pipeline(day: date | None = Query(None, alias='date'), limit: int = Query(1000, ge=1, le=2000)):
    return neon_store.pipeline(limit, day)


@router.get('/airflow/history')
def airflow_history(limit: int = Query(100, ge=1, le=1000)):
    from .airflow_history import history
    return history(limit)


@router.get('/reports/{run_id}/{kind}')
def historical_report(run_id: str, kind: str):
    if kind in RETIRED_REPORTS:
        raise HTTPException(404, 'Báo cáo này đã được gỡ khỏi sản phẩm')
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
    if table in RETIRED_REPORTS:
        raise HTTPException(404, 'Báo cáo này đã được gỡ khỏi sản phẩm')
    try:
        return neon_store.table_rows(table, offset=offset, limit=limit)
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
