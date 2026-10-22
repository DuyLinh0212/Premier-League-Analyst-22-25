from __future__ import annotations

from typing import Any

from app.query_utils import collect, payload, sql_string


def run(spark: Any, season: str) -> dict[str, Any]:
    rows = collect(
        spark.sql(
            f"""
            SELECT
                t.club_id,
                coalesce(c.club_name, t.club_id) AS club_name,
                t.matches_played,
                t.goals_for,
                t.goals_against,
                CASE WHEN t.matches_played > 0 THEN 1.0 * t.goals_for / t.matches_played END AS goals_for_per_match,
                CASE WHEN t.matches_played > 0 THEN 1.0 * t.goals_against / t.matches_played END AS goals_against_per_match
            FROM fact_team_season t
            LEFT JOIN club_names c ON c.club_id = t.club_id
            WHERE t.season = {sql_string(season)}
            ORDER BY goals_for_per_match DESC
            """
        )
    )
    strongest_attack = max(rows, key=lambda row: row.get("goals_for_per_match") or 0, default=None)
    strongest_defense = min(rows, key=lambda row: row.get("goals_against_per_match") or float("inf"), default=None)
    finding = "Chưa có dữ liệu đội bóng."
    if strongest_attack and strongest_defense:
        finding = (
            f"Tấn công tốt nhất theo bàn/trận: {strongest_attack['club_name']} "
            f"({strongest_attack['goals_for_per_match']:.2f}); phòng ngự tốt nhất theo "
            f"bàn thua/trận: {strongest_defense['club_name']} "
            f"({strongest_defense['goals_against_per_match']:.2f})."
        )
    return payload(
        rows,
        kind="mô tả",
        sample_size=len(rows),
        method="Trục X = bàn thắng/trận; trục Y = bàn thua/trận, tính từ fact_team_season.",
        finding=finding,
        limitations="Silver không có xG. Hai trục mô tả bàn thắng thực tế, không đo cơ hội hay giải thích nguyên nhân.",
    )
