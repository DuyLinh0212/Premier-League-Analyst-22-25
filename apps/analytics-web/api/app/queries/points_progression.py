from __future__ import annotations

from typing import Any

from app.query_utils import collect, payload, sql_string


def run(spark: Any, seasons: list[str], club_id: str) -> dict[str, Any]:
    if not seasons:
        return payload(
            [],
            kind="mô tả",
            sample_size=0,
            method="Cộng dồn 3 điểm cho trận thắng, 1 điểm cho hòa, 0 điểm cho thua theo vòng Transfermarkt.",
            finding="Không có mùa để so sánh.",
            limitations="Cần có ít nhất một mùa trong Silver.",
            club_id=club_id,
            season_totals=[],
        )

    season_list = ", ".join(sql_string(season) for season in seasons)
    transfermarkt_seasons = ", ".join(sql_string(season[:4]) for season in seasons)
    rows = collect(
        spark.sql(
            f"""
            WITH games AS (
                SELECT
                    CAST(game_id AS STRING) AS game_id,
                    CAST(season AS STRING) AS tm_season,
                    CAST(nullif(regexp_extract(round, '([0-9]+)', 1), '') AS INT) AS matchday
                FROM tm_games
                WHERE competition_id = 'GB1'
                  AND CAST(season AS STRING) IN ({transfermarkt_seasons})
            ),
            selected AS (
                SELECT
                    m.season,
                    m.club_id,
                    m.result,
                    g.matchday
                FROM fact_team_match m
                INNER JOIN games g
                    ON CAST(m.tm_game_id AS STRING) = g.game_id
                   AND substring(m.season, 1, 4) = g.tm_season
                WHERE m.season IN ({season_list})
                  AND m.club_id = {sql_string(club_id)}
                  AND g.matchday BETWEEN 1 AND 38
            ),
            round_points AS (
                SELECT
                    season,
                    matchday,
                    SUM(CASE upper(result) WHEN 'W' THEN 3 WHEN 'D' THEN 1 ELSE 0 END) AS earned_points,
                    COUNT(*) AS matches
                FROM selected
                GROUP BY season, matchday
            )
            SELECT
                season,
                matchday,
                earned_points,
                matches,
                SUM(earned_points) OVER (
                    PARTITION BY season
                    ORDER BY matchday ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_points
            FROM round_points
            ORDER BY season, matchday
            """
        )
    )

    silver_rows = collect(
        spark.sql(
            f"""
            SELECT season, club_name, points, matches_played
            FROM (
                SELECT
                    t.season,
                    coalesce(c.club_name, t.club_id) AS club_name,
                    t.points,
                    t.matches_played
                FROM fact_team_season t
                LEFT JOIN club_names c ON c.club_id = t.club_id
                WHERE t.season IN ({season_list})
                  AND t.club_id = {sql_string(club_id)}
            )
            ORDER BY season
            """
        )
    )

    matched_by_season: dict[str, dict[str, int]] = {}
    for row in rows:
        current = matched_by_season.setdefault(
            row["season"], {"matches": 0, "points_from_rounds": 0}
        )
        current["matches"] += int(row["matches"] or 0)
        current["points_from_rounds"] = int(row["cumulative_points"] or 0)

    silver_by_season = {row["season"]: row for row in silver_rows}
    totals = []
    for season in seasons:
        values = matched_by_season.get(season)
        silver = silver_by_season.get(season)
        if not values and not silver:
            continue
        points_from_rounds = values["points_from_rounds"] if values else 0
        matches = values["matches"] if values else 0
        silver_points = int(silver["points"]) if silver and silver["points"] is not None else None
        totals.append(
            {
                "season": season,
                "matches": matches,
                "points_from_rounds": points_from_rounds,
                "silver_points": silver_points,
                "difference": points_from_rounds - silver_points if silver_points is not None else None,
            }
        )

    club_name = silver_rows[-1]["club_name"] if silver_rows else club_id
    total_matches = sum(item["matches"] for item in totals)
    summaries = [
        f"{item['season']}: {item['points_from_rounds']} điểm / {item['matches']} trận"
        for item in totals
    ]
    finding = f"{club_name} — " + ("; ".join(summaries) if summaries else "chưa nối được dữ liệu vòng đấu.")
    discrepancies = [item for item in totals if item["difference"] not in (None, 0)]
    if discrepancies:
        finding += " Có mùa chưa khớp tổng điểm Silver."
    else:
        finding += " Điểm cuối mùa khớp với bảng Silver ở các mùa có đủ dữ liệu vòng."

    return payload(
        rows,
        kind="mô tả",
        sample_size=total_matches,
        method="Mỗi mùa được tính độc lập: cộng dồn 3 điểm cho thắng, 1 điểm cho hòa và 0 điểm cho thua theo vòng Transfermarkt.",
        finding=finding,
        limitations="Chỉ so sánh các mùa CLB có dữ liệu thi đấu. Vòng không nối được do thiếu mã trận sẽ vắng khỏi đường; không suy ra nguyên nhân thay đổi điểm số.",
        club_id=club_id,
        club_name=club_name,
        seasons=[item["season"] for item in totals],
        season_totals=totals,
    )
