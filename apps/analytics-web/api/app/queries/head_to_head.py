from __future__ import annotations

from typing import Any

from app.query_utils import collect, payload, sql_string


def run(spark: Any, season: str) -> dict[str, Any]:
    match_count = spark.sql(
        f"""
        SELECT COUNT(DISTINCT tm_game_id) AS n
        FROM fact_team_match
        WHERE season = {sql_string(season)}
        """
    ).first()["n"]
    teams = collect(
        spark.sql(
            f"""
            SELECT t.club_id, coalesce(c.club_name, t.club_id) AS club_name
            FROM fact_team_season t
            LEFT JOIN club_names c ON c.club_id = t.club_id
            WHERE t.season = {sql_string(season)}
            ORDER BY club_name
            """
        )
    )
    pairs = collect(
        spark.sql(
            f"""
            SELECT
                m.club_id AS row_club_id,
                coalesce(c1.club_name, m.club_id) AS row_club,
                m.opponent_club_id AS column_club_id,
                coalesce(c2.club_name, m.opponent_club_id) AS column_club,
                SUM(CASE WHEN upper(m.result) = 'W' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN upper(m.result) = 'D' THEN 1 ELSE 0 END) AS draws,
                SUM(CASE WHEN upper(m.result) = 'L' THEN 1 ELSE 0 END) AS losses,
                COUNT(*) AS matches
            FROM fact_team_match m
            LEFT JOIN club_names c1 ON c1.club_id = m.club_id
            LEFT JOIN club_names c2 ON c2.club_id = m.opponent_club_id
            WHERE m.season = {sql_string(season)}
            GROUP BY m.club_id, c1.club_name, m.opponent_club_id, c2.club_name
            """
        )
    )
    wins = max(pairs, key=lambda row: row["wins"], default=None)
    losses = max(pairs, key=lambda row: row["losses"], default=None)
    finding = "Chưa có trận đối đầu trong mùa này."
    if wins and losses:
        finding = (
            f"Cặp có nhiều chiến thắng nhất theo một hướng: {wins['row_club']} thắng "
            f"{wins['column_club']} {wins['wins']} lần; cặp có nhiều thất bại nhất: "
            f"{losses['row_club']} thua {losses['column_club']} {losses['losses']} lần."
        )
    return payload(
        pairs,
        kind="mô tả",
        sample_size=match_count,
        method="Mỗi ô nhìn từ đội ở hàng sang đội ở cột, đếm thắng–hòa–thua từ fact_team_match; n là số trận duy nhất, mỗi trận xuất hiện ở hai hướng.",
        finding=finding,
        limitations="Ma trận là thống kê lịch sử trong mùa, không dự báo kết quả lần gặp tiếp theo.",
        teams=teams,
        rows=teams,
        columns=teams,
        directed_pair_records=sum(row["matches"] for row in pairs),
    )


def run_club_comparison(spark: Any, seasons: list[str], club_id: str) -> dict[str, Any]:
    season_filter = ", ".join(sql_string(season) for season in seasons)
    totals = spark.sql(
        f"""
        SELECT
            coalesce(first(c.club_name, true), {sql_string(club_id)}) AS club_name,
            SUM(CASE upper(m.result) WHEN 'W' THEN 1 ELSE 0 END) AS wins,
            SUM(CASE upper(m.result) WHEN 'D' THEN 1 ELSE 0 END) AS draws,
            SUM(CASE upper(m.result) WHEN 'L' THEN 1 ELSE 0 END) AS losses,
            COUNT(*) AS matches,
            COUNT(DISTINCT m.season) AS seasons_played
        FROM fact_team_match m
        LEFT JOIN club_names c ON c.club_id = m.club_id
        WHERE m.club_id = {sql_string(club_id)}
          AND m.season IN ({season_filter})
        """
    ).first()
    rows = collect(
        spark.sql(
            f"""
            SELECT
                m.opponent_club_id,
                coalesce(c.club_name, m.opponent_club_id) AS opponent_name,
                SUM(CASE upper(m.result) WHEN 'W' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE upper(m.result) WHEN 'D' THEN 1 ELSE 0 END) AS draws,
                SUM(CASE upper(m.result) WHEN 'L' THEN 1 ELSE 0 END) AS losses,
                COUNT(*) AS matches,
                COUNT(DISTINCT m.season) AS seasons_played
            FROM fact_team_match m
            LEFT JOIN club_names c ON c.club_id = m.opponent_club_id
            WHERE m.club_id = {sql_string(club_id)}
              AND m.season IN ({season_filter})
              AND m.opponent_club_id IS NOT NULL
            GROUP BY m.opponent_club_id, c.club_name
            ORDER BY wins DESC, losses ASC, opponent_name
            """
        )
    )
    club_name = totals["club_name"] if totals else club_id
    wins = int(totals["wins"] or 0) if totals else 0
    draws = int(totals["draws"] or 0) if totals else 0
    losses = int(totals["losses"] or 0) if totals else 0
    matches = int(totals["matches"] or 0) if totals else 0
    played_seasons = int(totals["seasons_played"] or 0) if totals else 0

    most_wins = max(rows, key=lambda row: (row["wins"], -row["losses"]), default=None)
    most_losses = max(rows, key=lambda row: (row["losses"], -row["wins"]), default=None)
    finding = f"{club_name} có {wins} thắng, {draws} hòa và {losses} thua trong {matches} trận thuộc {played_seasons} mùa."
    if most_wins and most_losses:
        finding += (
            f" Thắng nhiều nhất trước {most_wins['opponent_name']} ({most_wins['wins']} trận); "
            f"thua nhiều nhất trước {most_losses['opponent_name']} ({most_losses['losses']} trận)."
        )

    return payload(
        rows,
        kind="mô tả",
        sample_size=matches,
        method="Gộp số trận thắng–hòa–thua của CLB đang chọn với từng đối thủ trong các mùa đã chọn; mỗi trận được tính một lần từ góc nhìn CLB này.",
        finding=finding,
        limitations="Các đối thủ có thể chỉ gặp nhau trong một phần số mùa do lên/xuống hạng. Kết quả lịch sử không dự báo trận gặp tiếp theo.",
        club_id=club_id,
        club_name=club_name,
        seasons=seasons,
        seasons_played=played_seasons,
        wins=wins,
        draws=draws,
        losses=losses,
        opponents=len(rows),
    )
