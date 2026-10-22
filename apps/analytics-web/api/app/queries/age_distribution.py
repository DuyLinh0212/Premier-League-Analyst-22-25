from __future__ import annotations

from typing import Any

from app.query_utils import collect, payload, sql_string


def run(spark: Any, season: str) -> dict[str, Any]:
    rows = collect(
        spark.sql(
            f"""
            WITH players AS (
                SELECT
                    player_id,
                    first(player_name, true) AS player_name,
                    max(CAST(age AS DOUBLE)) AS age
                FROM fact_player_season
                WHERE season = {sql_string(season)}
                GROUP BY player_id
            ),
            banded AS (
                SELECT
                    CASE
                        WHEN age < 23 THEN '<23'
                        WHEN age >= 23 AND age <= 27 THEN '23–27'
                        WHEN age >= 28 AND age <= 32 THEN '28–32'
                        WHEN age > 32 THEN '>32'
                        ELSE 'Thiếu tuổi'
                    END AS age_band,
                    age
                FROM players
            )
            SELECT
                age_band,
                COUNT(*) AS player_count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percent_of_total,
                AVG(age) AS average_age
            FROM banded
            GROUP BY age_band
            ORDER BY CASE age_band
                WHEN '<23' THEN 1 WHEN '23–27' THEN 2 WHEN '28–32' THEN 3
                WHEN '>32' THEN 4 ELSE 5 END
            """
        )
    )
    known = [row for row in rows if row["age_band"] != "Thiếu tuổi"]
    largest = max(known, key=lambda row: row["player_count"]) if known else None
    missing = next((row["player_count"] for row in rows if row["age_band"] == "Thiếu tuổi"), 0)
    finding = (
        f"Nhóm đông nhất là {largest['age_band']} tuổi với {largest['player_count']} cầu thủ."
        if largest
        else "Chưa có tuổi cầu thủ để phân nhóm."
    )
    if missing:
        finding += f" Có {missing} cầu thủ thiếu tuổi."
    return payload(
        rows,
        kind="mô tả",
        sample_size=sum(row["player_count"] for row in rows),
        method="Đếm cầu thủ duy nhất theo tuổi trong fact_player_season, chia thành bốn khoảng không chồng lấn.",
        finding=finding,
        limitations="Tuổi được lấy từ Silver theo mùa; hồ sơ thiếu tuổi được trình bày riêng.",
    )
