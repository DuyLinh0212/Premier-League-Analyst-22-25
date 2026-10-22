from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from app.queries import (
    attack_defense,
    head_to_head,
    matchday_performance,
    player_contribution,
    player_radar,
    points_progression,
    spending_vs_achievement,
    squad_value,
    team_strength,
)
from app.query_utils import collect, sql_string
from src.pipeline.database import PostgresRuntime, provenance
from app.static_reports import REPORTS, ensure_report


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOGGER = logging.getLogger("plca.analytics")
runtime = PostgresRuntime()
spark = None


def seasons_available() -> list[str]:
    if not runtime.manifest:
        return []
    return [str(season) for season in runtime.manifest.get("seasons", [])]


def validate_season(season: str) -> str:
    if season not in seasons_available():
        raise HTTPException(status_code=400, detail="Mùa giải không có trong Silver hiện tại.")
    return season


def validate_club(season: str, club_id: str) -> str:
    assert spark is not None
    found = spark.sql(
        f"""
        SELECT COUNT(*) AS n FROM fact_team_season
        WHERE season = {sql_string(season)} AND club_id = {sql_string(club_id)}
        """
    ).first()["n"]
    if not found:
        raise HTTPException(status_code=400, detail="CLB không thuộc mùa giải đã chọn.")
    return club_id


def validate_comparison_seasons(seasons: list[str]) -> list[str]:
    selected = seasons or seasons_available()
    if len(selected) > 3 or len(set(selected)) != len(selected):
        raise HTTPException(status_code=400, detail="Có thể so sánh tối đa ba mùa khác nhau.")
    for season in selected:
        validate_season(season)
    return selected


def validate_club_in_seasons(club_id: str, seasons: list[str]) -> None:
    if not seasons:
        raise HTTPException(status_code=400, detail="Không có mùa để so sánh.")
    season_filter = ", ".join(sql_string(season) for season in seasons)
    exists = active_spark().sql(
        f"""
        SELECT COUNT(*) AS n
        FROM fact_team_season
        WHERE club_id = {sql_string(club_id)}
          AND season IN ({season_filter})
        """
    ).first()["n"]
    if not exists:
        raise HTTPException(status_code=400, detail="CLB không có dữ liệu trong các mùa đã chọn.")


@asynccontextmanager
async def lifespan(_: FastAPI):
    global spark
    spark = runtime.start()
    LOGGER.info("Đang khởi động API phân tích; truy vấn PostgreSQL và đọc báo cáo đã xuất.")
    yield
    runtime.stop()
    spark = None


app = FastAPI(
    title="Premier League Analytics",
    description="Dashboard tiếng Việt truy vấn PostgreSQL trên snapshot phân tích đã công bố.",
    version="1.0.0",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.middleware("http")
async def snapshot_request(request: Request, call_next):
    if request.url.path.startswith("/pipeline/") or request.url.path in ("/docs", "/openapi.json", "/redoc"):
        return await call_next(request)
    try:
        with runtime.request(request.query_params.get("run_id")):
            response = await call_next(request)
            response.headers["X-PLCA-Run-Id"] = runtime.manifest["run_id"]
            response.headers["X-PLCA-Source"] = "postgresql"
            return response
    except (OSError, RuntimeError) as error:
        LOGGER.error("Không phục vụ được PostgreSQL: %s", type(error).__name__)
        return JSONResponse(status_code=503, content={"status": "unavailable", "database": False,
                            "detail": "PostgreSQL chưa sẵn sàng hoặc chưa có snapshot đã công bố."})
    except Exception as error:
        import psycopg
        if isinstance(error, psycopg.Error):
            LOGGER.exception("Truy vấn PostgreSQL thất bại")
            return JSONResponse(status_code=503, content={"status": "unavailable", "database": False,
                                "detail": "Không truy vấn được PostgreSQL; xem log kỹ thuật."})
        raise


def active_spark() -> Any:
    if spark is None:
        raise HTTPException(status_code=503, detail="API chưa khởi động xong.")
    return spark


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "status": "ready" if spark is not None else "starting",
        "service": "plca-analytics-api",
        "database": True,
        **provenance(),
        "silver_dataset": runtime.manifest.get("dataset_id"),
    }


@app.get("/api/metadata")
def metadata(
    season: str = "2024-2025",
    position: str = "FW",
    min_minutes: int = Query(default=900, ge=0, le=5000),
) -> dict[str, Any]:
    validate_season(season)
    if position not in {"all", "DF", "FW", "GK", "MF"}:
        raise HTTPException(status_code=400, detail="Vị trí không hợp lệ.")
    current = active_spark()
    teams = collect(
        current.sql(
            f"""
            SELECT t.club_id, coalesce(c.club_name, t.club_id) AS club_name
            FROM fact_team_season t
            LEFT JOIN club_names c ON c.club_id = t.club_id
            WHERE t.season = {sql_string(season)}
            ORDER BY club_name
            """
        )
    )
    position_filter = "" if position == "all" else f"AND p.position = {sql_string(position)}"
    players = collect(
        current.sql(
            f"""
            SELECT
                concat(p.player_id, '|', p.club_id) AS selection_id,
                p.player_id,
                p.club_id,
                p.player_name,
                coalesce(c.club_name, p.club_id) AS club_name,
                p.position,
                CAST(coalesce(p.minutes, p.playing_minutes, p.gk_minutes) AS INT) AS minutes
            FROM fact_player_season p
            LEFT JOIN club_names c ON c.club_id = p.club_id
            WHERE p.season = {sql_string(season)}
              {position_filter}
              AND coalesce(p.minutes, p.playing_minutes, p.gk_minutes, 0) >= {int(min_minutes)}
            ORDER BY p.player_name, c.club_name
            """
        )
    )
    return {
        "seasons": seasons_available(),
        "season": season,
        "positions": [
            {"id": "DF", "label": "Hậu vệ"},
            {"id": "FW", "label": "Tiền đạo"},
            {"id": "GK", "label": "Thủ môn"},
            {"id": "MF", "label": "Tiền vệ"},
        ],
        "teams": teams,
        "players": players,
        "silver_dataset": runtime.manifest.get("dataset_id"),
        "meta": provenance(),
    }


@app.get("/api/player-radar")
def api_player_radar(
    season: str,
    position: str,
    players: list[str] = Query(..., min_length=2, max_length=4),
    min_minutes: int = Query(default=900, ge=0, le=5000),
) -> dict[str, Any]:
    validate_season(season)
    if position not in {"DF", "FW", "GK", "MF"}:
        raise HTTPException(status_code=400, detail="Vị trí không hợp lệ.")
    if len(set(players)) != len(players) or any("|" not in player for player in players):
        raise HTTPException(status_code=400, detail="Chọn từ 2 đến 4 cầu thủ khác nhau.")
    return player_radar.run(active_spark(), season, position, players, min_minutes)


@app.get("/api/player-contribution")
def api_player_contribution(season: str, club_id: str) -> dict[str, Any]:
    season = validate_season(season)
    club_id = validate_club(season, club_id)
    return player_contribution.run(active_spark(), season, club_id)


@app.get("/api/squad-value")
def api_squad_value(season: str, club_id: str | None = None) -> dict[str, Any]:
    season = validate_season(season)
    if club_id:
        validate_club(season, club_id)
    return squad_value.run(active_spark(), season, club_id)


@app.get("/api/matchday-performance")
def api_matchday_performance(season: str, club_id: str) -> dict[str, Any]:
    season = validate_season(season)
    club_id = validate_club(season, club_id)
    return matchday_performance.run(active_spark(), season, club_id)


@app.get("/api/points-progression")
def api_points_progression(
    club_id: str,
    seasons: list[str] = Query(default=[]),
) -> dict[str, Any]:
    selected_seasons = validate_comparison_seasons(seasons)
    validate_club_in_seasons(club_id, selected_seasons)
    return points_progression.run(active_spark(), selected_seasons, club_id)


@app.get("/api/head-to-head")
def api_head_to_head(
    club_id: str,
    seasons: list[str] = Query(default=[]),
) -> dict[str, Any]:
    selected_seasons = validate_comparison_seasons(seasons)
    validate_club_in_seasons(club_id, selected_seasons)
    return head_to_head.run_club_comparison(active_spark(), selected_seasons, club_id)


@app.get("/api/team-strength")
def api_team_strength(
    season: str,
    clubs: list[str] = Query(default=[]),
) -> dict[str, Any]:
    season = validate_season(season)
    if len(clubs) > 4 or len(set(clubs)) != len(clubs):
        raise HTTPException(status_code=400, detail="Có thể chọn tối đa bốn CLB khác nhau.")
    for club_id in clubs:
        validate_club(season, club_id)
    return team_strength.run(active_spark(), season, clubs)


@app.get("/api/reports/{slug}")
def api_static_report(
    slug: str,
    season: str,
) -> dict[str, Any]:
    season = validate_season(season)
    if slug not in REPORTS:
        raise HTTPException(status_code=404, detail="Không tìm thấy báo cáo tĩnh.")
    try:
        return ensure_report(
            active_spark(),
            season,
            slug,
            str(runtime.manifest.get("dataset_id", "unknown")),
        )
    except Exception as error:
        LOGGER.exception("Không tải được báo cáo %s mùa %s", slug, season)
        raise HTTPException(status_code=503, detail=f"Không tạo được biểu đồ: {error}") from error


@app.get("/reports/{season}/{filename}")
def report_image(season: str, filename: str):
    validate_season(season)
    safe_name = Path(filename).name
    path = runtime.reports_dir / season / safe_name
    if not path.is_file() or path.suffix.lower() != ".png":
        raise HTTPException(status_code=404, detail="Ảnh báo cáo chưa được tạo.")
    return FileResponse(path, media_type="image/png", filename=safe_name)
