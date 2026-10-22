from __future__ import annotations

from typing import Any

from app.query_utils import collect, payload, sql_string


def run(spark: Any, season: str, club_id: str) -> dict[str, Any]:
    rows = collect(
        spark.sql(
            f"""
            SELECT
                p.player_id,
                p.player_name,
                p.position,
                coalesce(c.club_name, p.club_id) AS club_name,
                CAST(p.minutes AS INT) AS minutes,
                CAST(p.goals AS INT) AS goals,
                CAST(p.assists AS INT) AS assists,
                CASE
                    WHEN p.goals IS NOT NULL AND p.assists IS NOT NULL
                    THEN CAST(p.goals + p.assists AS INT)
                END AS goal_contributions
            FROM fact_player_season p
            LEFT JOIN club_names c ON c.club_id = p.club_id
            WHERE p.season = {sql_string(season)}
              AND p.club_id = {sql_string(club_id)}
            ORDER BY p.minutes DESC NULLS LAST, p.player_name
            """
        )
    )
    used = next((row for row in rows if row.get("minutes") is not None), None)
    creator = next(
        (row for row in sorted(rows, key=lambda item: item.get("goal_contributions") or -1, reverse=True)
         if row.get("goal_contributions") is not None),
        None,
    )
    parts = []
    if used:
        parts.append(f"{used['player_name']} chơi nhiều phút nhất ({used['minutes']:,} phút)")
    if creator:
        parts.append(
            f"{creator['player_name']} có nhiều bàn + kiến tạo nhất "
            f"({creator['goal_contributions']})"
        )
    finding = "; ".join(parts) + "." if parts else "Không có dữ liệu cầu thủ cho lựa chọn này."
    return payload(
        rows,
        kind="mô tả",
        sample_size=len(rows),
        method="Báo cáo phút thi đấu và tổng bàn thắng + kiến tạo như hai chỉ số riêng.",
        finding=finding,
        limitations="Thời lượng ra sân và đóng góp bàn thắng trực tiếp không được gộp thành điểm quan trọng. Bảng Silver ghi nhận thống kê theo từng cầu thủ–CLB–mùa.",
        club_id=club_id,
        club_name=rows[0]["club_name"] if rows else club_id,
    )
