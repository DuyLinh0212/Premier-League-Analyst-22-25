from __future__ import annotations

from typing import Any

from app.query_utils import collect, payload, round_results_cte, sql_string


def run(spark: Any, season: str, club_id: str) -> dict[str, Any]:
    cte = round_results_cte(season, club_id)
    rows = collect(
        spark.sql(
            f"""
            {cte},
            round_totals AS (
                SELECT
                    matchday,
                    COUNT(*) AS matches,
                    SUM(CASE WHEN upper(result) = 'W' THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN upper(result) = 'D' THEN 1 ELSE 0 END) AS draws,
                    SUM(CASE WHEN upper(result) = 'L' THEN 1 ELSE 0 END) AS losses,
                    SUM(goals_for) AS goals_for,
                    SUM(goals_against) AS goals_against,
                    SUM(CASE WHEN upper(result) = 'W' THEN 3 WHEN upper(result) = 'D' THEN 1 ELSE 0 END) AS points
                FROM selected
                GROUP BY matchday
            )
            SELECT
                matchday,
                matches,
                wins,
                draws,
                losses,
                goals_for,
                goals_against,
                points,
                SUM(points) OVER (ORDER BY matchday ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_points
            FROM round_totals
            ORDER BY matchday
            """
        )
    )
    matches = sum(row["matches"] for row in rows)
    wins = sum(row["wins"] for row in rows)
    draws = sum(row["draws"] for row in rows)
    losses = sum(row["losses"] for row in rows)
    first = next((row for row in rows if row["goals_for"] > row["goals_against"]), None)
    finding = f"Record nối được {matches} trận: {wins} thắng, {draws} hòa, {losses} thua."
    if first:
        finding += f" Đội ghi nhiều bàn hơn ở vòng {first['matchday']}."
    return payload(
        rows,
        kind="mô tả",
        sample_size=matches,
        method="Nối tm_game_id với games.csv (competition_id GB1), lấy round và cộng điểm 3–1–0 theo vòng.",
        finding=finding,
        limitations=f"Có dữ liệu ở {len(rows)}/38 vòng. Lịch hoãn hoặc thiếu mã trận có thể làm một vòng có nhiều hay ít trận.",
        club_id=club_id,
    )
