from __future__ import annotations

from typing import Any

from app.query_utils import collect, payload, sql_string


def run(spark: Any, season: str, clubs: list[str]) -> dict[str, Any]:
    club_filter = f"AND club_id IN ({', '.join(sql_string(club) for club in clubs)})" if clubs else ""
    rows = collect(
        spark.sql(
            f"""
            WITH points_variation AS (
                SELECT
                    season,
                    club_id,
                    ROUND(stddev_samp(
                        CASE upper(result) WHEN 'W' THEN 3.0 WHEN 'D' THEN 1.0 ELSE 0.0 END
                    ), 12) AS points_sd_per_match
                FROM fact_team_match
                WHERE season = {sql_string(season)}
                GROUP BY season, club_id
            ),
            valuation AS (
                SELECT
                    player_id,
                    market_value_eur,
                    ROW_NUMBER() OVER (
                        PARTITION BY player_id ORDER BY valuation_date DESC
                    ) AS rn
                FROM (
                    SELECT
                        b.player_id,
                        v.market_value_eur,
                        v.valuation_date
                    FROM bridge_transfermarkt_player b
                    INNER JOIN fact_player_valuation v
                        ON v.tm_player_id = b.tm_player_id
                    WHERE v.valuation_date >= to_date(concat(substring({sql_string(season)}, 1, 4), '-07-01'))
                      AND v.valuation_date < to_date(concat(substring({sql_string(season)}, 6, 4), '-07-01'))
                )
            ),
            team_value AS (
                SELECT
                    p.club_id,
                    AVG(v.market_value_eur) AS average_market_value_eur,
                    COUNT(v.market_value_eur) AS valued_players
                FROM fact_player_season p
                LEFT JOIN valuation v ON v.player_id = p.player_id AND v.rn = 1
                WHERE p.season = {sql_string(season)}
                GROUP BY p.club_id
            ),
            base AS (
                SELECT
                    t.club_id,
                    coalesce(c.club_name, t.club_id) AS club_name,
                    t.matches_played,
                    t.goals_for,
                    t.goals_against,
                    t.points,
                    CASE WHEN t.matches_played > 0 THEN 1.0 * t.goals_for / t.matches_played END AS goals_for_per_match,
                    CASE WHEN t.matches_played > 0 THEN 1.0 * t.goals_against / t.matches_played END AS goals_against_per_match,
                    CASE WHEN t.matches_played > 0 THEN 1.0 * t.points / t.matches_played END AS points_per_match,
                    pv.points_sd_per_match,
                    tv.average_market_value_eur,
                    tv.valued_players
                FROM fact_team_season t
                LEFT JOIN club_names c ON c.club_id = t.club_id
                LEFT JOIN points_variation pv ON pv.season = t.season AND pv.club_id = t.club_id
                LEFT JOIN team_value tv ON tv.club_id = t.club_id
                WHERE t.season = {sql_string(season)}
            ),
            scored AS (
                SELECT
                    *,
                    COUNT(*) OVER () AS reference_clubs,
                    100.0 * percent_rank() OVER (ORDER BY goals_for_per_match ASC NULLS LAST) AS attack_pct,
                    100.0 * percent_rank() OVER (ORDER BY goals_against_per_match DESC NULLS LAST) AS defense_pct,
                    100.0 * percent_rank() OVER (ORDER BY points_per_match ASC NULLS LAST) AS results_pct,
                    100.0 * percent_rank() OVER (ORDER BY points_sd_per_match ASC NULLS LAST) AS consistency_pct,
                    CASE WHEN average_market_value_eur IS NOT NULL THEN
                        100.0 * percent_rank() OVER (ORDER BY average_market_value_eur ASC NULLS LAST)
                    END AS squad_quality_pct
                FROM base
            )
            SELECT * FROM scored
            WHERE 1 = 1 {club_filter}
            ORDER BY club_name
            """
        )
    )
    reference_size = int(rows[0].pop("reference_clubs")) if rows else 0
    axes = [
        {"key": "attack_pct", "label": "Tấn công"},
        {"key": "defense_pct", "label": "Phòng ngự"},
        {"key": "results_pct", "label": "Kết quả"},
        {"key": "consistency_pct", "label": "Ổn định"},
        {"key": "squad_quality_pct", "label": "Chất lượng đội hình"},
    ]
    missing = sum(1 for row in rows if row.get("squad_quality_pct") is None)
    finding = (
        f"Percentile được tính trên toàn bộ {reference_size} CLB trong mùa; "
        f"biểu đồ đang hiển thị {len(rows)} CLB đã chọn."
    )
    if missing:
        finding += f" Trục chất lượng đội hình thiếu định giá ở {missing} đội."
    return payload(
        rows,
        kind="công thức",
        sample_size=reference_size,
        method="Percentile 0–100 trong cùng mùa cho bàn thắng/trận, bàn thua/trận (đảo chiều), điểm/trận, độ lệch chuẩn điểm/trận (đảo chiều, làm tròn 12 chữ số để xử lý đồng hạng), giá trị thị trường TB/cầu thủ.",
        finding=finding,
        limitations="Không cộng năm trục thành thứ hạng tổng hợp. Giá trị thị trường chỉ tính trên cầu thủ có định giá ghép được trong mùa.",
        axes=axes,
        selected_count=len(rows),
    )
