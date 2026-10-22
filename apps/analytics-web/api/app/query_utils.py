from __future__ import annotations

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable


def sql_string(value: str) -> str:
    """Quote a value used in a Spark SQL string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def sql_in(values: Iterable[str]) -> str:
    return ", ".join(sql_string(value) for value in values)


def json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        if value.is_nan() or value.is_infinite():
            return None
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if hasattr(value, "item"):
        try:
            return json_value(value.item())
        except (TypeError, ValueError):
            pass
    return value


def collect(frame: Any) -> list[dict[str, Any]]:
    return [
        {key: json_value(value) for key, value in row.asDict(recursive=True).items()}
        for row in frame.collect()
    ]


def payload(
    data: Any,
    *,
    kind: str,
    sample_size: int,
    method: str,
    finding: str,
    limitations: str,
    **metadata: Any,
) -> dict[str, Any]:
    from src.pipeline.database import provenance
    return {
        "data": data,
        "insight": {
            "type": kind,
            "sample_size": int(sample_size),
            "method": method,
            "finding": finding,
            "limitations": limitations,
        },
        "meta": {**metadata, **provenance()},
    }


def round_results_cte(season: str, club_id: str) -> str:
    """Shared Spark SQL CTE joining Silver match rows to Transfermarkt rounds."""
    return f"""
        WITH games AS (
            SELECT
                CAST(game_id AS STRING) AS game_id,
                CAST(season AS STRING) AS tm_season,
                CAST(nullif(regexp_extract(round, '([0-9]+)', 1), '') AS INT) AS matchday
            FROM tm_games
            WHERE competition_id = 'GB1'
              AND CAST(season AS STRING) = substring({sql_string(season)}, 1, 4)
        ),
        selected AS (
            SELECT
                CAST(m.tm_game_id AS STRING) AS game_id,
                m.club_id,
                m.result,
                CAST(m.goals_for AS INT) AS goals_for,
                CAST(m.goals_against AS INT) AS goals_against,
                g.matchday
            FROM fact_team_match m
            INNER JOIN games g
                ON CAST(m.tm_game_id AS STRING) = g.game_id
            WHERE m.season = {sql_string(season)}
              AND m.club_id = {sql_string(club_id)}
              AND g.matchday BETWEEN 1 AND 38
        )
    """
