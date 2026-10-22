from __future__ import annotations

from typing import Any

from app.query_utils import collect, payload, sql_in, sql_string


OUTFIELD = [
    ("goals90", "Bàn thắng / 90 phút"),
    ("assists90", "Kiến tạo / 90 phút"),
    ("shotsOnTarget90", "Sút trúng đích / 90 phút"),
    ("tacklesWon90", "Tắc bóng thắng / 90 phút"),
    ("interceptions90", "Đánh chặn / 90 phút"),
]
GOALKEEPER = [
    ("saves90", "Cứu thua / 90 phút"),
    ("savePct", "Tỷ lệ cứu thua"),
    ("cleanSheetPct", "Tỷ lệ giữ sạch lưới"),
    ("goalsAgainst90", "Bàn thua / 90 phút"),
    ("penaltySavePct", "Tỷ lệ cản penalty"),
]


def run(
    spark: Any,
    season: str,
    position: str,
    players: list[str],
    min_minutes: int = 900,
) -> dict[str, Any]:
    metrics = GOALKEEPER if position == "GK" else OUTFIELD
    cohort_count = spark.sql(
        f"""
        SELECT COUNT(*) AS n
        FROM fact_player_season
        WHERE season = {sql_string(season)}
          AND position = {sql_string(position)}
          AND coalesce(minutes, playing_minutes, gk_minutes, 0) >= {int(min_minutes)}
        """
    ).first()["n"]

    if not players:
        return payload(
            [],
            kind="công thức",
            sample_size=cohort_count,
            method=f"Percentile trong cùng mùa, cùng vị trí {position}, từ {min_minutes:,} phút.",
            finding="Chọn từ 2 đến 4 cầu thủ đủ điều kiện để vẽ radar.",
            limitations="Percentile mô tả vị trí tương đối trong cohort, không phải xác suất hay điểm đánh giá tổng hợp.",
            axes=[label for _, label in metrics],
            position=position,
            min_minutes=min_minutes,
        )

    if position == "GK":
        base_metrics = """
            CASE WHEN coalesce(p.minutes, p.playing_minutes, p.gk_minutes) > 0
                 THEN 90.0 * p.gk_saves / coalesce(p.minutes, p.playing_minutes, p.gk_minutes) END AS saves90,
            p.gk_save_pct AS savePct,
            p.gk_clean_sheet_pct AS cleanSheetPct,
            p.gk_goals_against_per90 AS goalsAgainst90,
            p.gk_penalty_save_pct AS penaltySavePct
        """
        percentile_expressions = [
            ("saves90", "saves90", "ASC"),
            ("savePct", "savePct", "ASC"),
            ("cleanSheetPct", "cleanSheetPct", "ASC"),
            ("goalsAgainst90", "goalsAgainst90", "DESC"),
            ("penaltySavePct", "penaltySavePct", "ASC"),
        ]
        raw_columns = "saves90, savePct, cleanSheetPct, goalsAgainst90, penaltySavePct"
    else:
        base_metrics = """
            CASE WHEN coalesce(p.minutes, p.playing_minutes) > 0 THEN 90.0 * p.goals / coalesce(p.minutes, p.playing_minutes) END AS goals90,
            CASE WHEN coalesce(p.minutes, p.playing_minutes) > 0 THEN 90.0 * p.assists / coalesce(p.minutes, p.playing_minutes) END AS assists90,
            CASE WHEN coalesce(p.minutes, p.playing_minutes) > 0 THEN 90.0 * p.shooting_shots_on_target / coalesce(p.minutes, p.playing_minutes) END AS shotsOnTarget90,
            CASE WHEN coalesce(p.minutes, p.playing_minutes) > 0 THEN 90.0 * p.misc_tackles_won / coalesce(p.minutes, p.playing_minutes) END AS tacklesWon90,
            CASE WHEN coalesce(p.minutes, p.playing_minutes) > 0 THEN 90.0 * p.misc_interceptions / coalesce(p.minutes, p.playing_minutes) END AS interceptions90
        """
        percentile_expressions = [(name, name, "ASC") for name, _ in OUTFIELD]
        raw_columns = "goals90, assists90, shotsOnTarget90, tacklesWon90, interceptions90"

    percentile_columns = ",\n".join(
        f"CASE WHEN {raw} IS NOT NULL THEN "
        f"100.0 * percent_rank() OVER (ORDER BY {raw} {direction} NULLS LAST) END AS pct_{alias}"
        for alias, raw, direction in percentile_expressions
    )
    rows = collect(
        spark.sql(
            f"""
            WITH base AS (
                SELECT
                    p.player_id,
                    p.club_id,
                    p.player_name,
                    p.position,
                    CAST(coalesce(p.minutes, p.playing_minutes, p.gk_minutes) AS INT) AS minutes,
                    {base_metrics}
                FROM fact_player_season p
                WHERE p.season = {sql_string(season)}
                  AND p.position = {sql_string(position)}
                  AND coalesce(p.minutes, p.playing_minutes, p.gk_minutes, 0) >= {int(min_minutes)}
            ),
            scored AS (
                SELECT
                    *,
                    {percentile_columns}
                FROM base
            )
            SELECT
                concat(s.player_id, '|', s.club_id) AS selection_id,
                s.player_id,
                s.club_id,
                s.player_name,
                coalesce(c.club_name, s.club_id) AS club_name,
                s.position,
                s.minutes,
                {raw_columns},
                {", ".join("pct_" + name for name, _ in metrics)}
            FROM scored s
            LEFT JOIN club_names c ON c.club_id = s.club_id
            WHERE concat(s.player_id, '|', s.club_id) IN ({sql_in(players)})
            ORDER BY s.player_name, s.club_id
            """
        )
    )
    leaders: list[str] = []
    for key, label in metrics:
        candidates = [item for item in rows if item.get("pct_" + key) is not None]
        if candidates:
            leader = max(candidates, key=lambda item: item["pct_" + key])
            leaders.append(f"{label}: {leader['player_name']} ({leader['pct_' + key]:.0f} percentile)")
    finding = (
        "Không có một điểm tổng hợp; các mốc cao nhất theo từng trục là "
        + "; ".join(leaders)
        + "."
        if leaders
        else "Không có giá trị hợp lệ cho các cầu thủ đã chọn trong cohort."
    )
    return payload(
        rows,
        kind="công thức",
        sample_size=cohort_count,
        method=f"Chuẩn hóa mỗi 90 phút rồi tính percentile 0–100 trong cùng mùa, cùng vị trí {position}, từ {min_minutes:,} phút.",
        finding=finding,
        limitations="Percentile mô tả vị trí tương đối trong cohort; chỉ số GK và cầu thủ sân ngoài dùng bộ trục riêng. Không diễn giải thành quan hệ nhân quả.",
        axes=[{"key": key, "label": label} for key, label in metrics],
        position=position,
        min_minutes=min_minutes,
        cohort_size=cohort_count,
    )
