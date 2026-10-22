from __future__ import annotations

from typing import Any

from app.query_utils import collect, payload, sql_string


VALUATION_CTE = """
    WITH season_valuations AS (
        SELECT
            b.player_id,
            v.market_value_eur,
            v.valuation_date,
            ROW_NUMBER() OVER (
                PARTITION BY b.player_id
                ORDER BY v.valuation_date DESC
            ) AS valuation_order
        FROM bridge_transfermarkt_player b
        INNER JOIN fact_player_valuation v
            ON v.tm_player_id = b.tm_player_id
        WHERE v.valuation_date >= to_date(concat(substring({season}, 1, 4), '-07-01'))
          AND v.valuation_date < to_date(concat(substring({season}, 6, 4), '-07-01'))
    )
"""


def run(spark: Any, season: str, club_id: str | None = None) -> dict[str, Any]:
    season_sql = sql_string(season)
    cte = VALUATION_CTE.format(season=season_sql)
    club_filter = f"AND p.club_id = {sql_string(club_id)}" if club_id else ""
    rows = collect(
        spark.sql(
            f"""
            {cte}
            SELECT
                p.player_id,
                p.player_name,
                p.position,
                p.club_id,
                coalesce(c.club_name, p.club_id) AS club_name,
                CAST(v.market_value_eur AS DOUBLE) AS market_value_eur,
                CAST(v.valuation_date AS STRING) AS valuation_date
            FROM fact_player_season p
            LEFT JOIN season_valuations v
                ON v.player_id = p.player_id AND v.valuation_order = 1
            LEFT JOIN club_names c ON c.club_id = p.club_id
            WHERE p.season = {season_sql}
              {club_filter}
            ORDER BY v.market_value_eur DESC NULLS LAST, p.player_name
            """
        )
    )
    position_summary = collect(
        spark.sql(
            f"""
            {cte}
            SELECT
                p.position,
                COUNT(*) AS player_rows,
                SUM(CASE WHEN v.market_value_eur IS NOT NULL THEN 1 ELSE 0 END) AS valued_rows,
                SUM(v.market_value_eur) AS total_value_eur,
                AVG(v.market_value_eur) AS average_value_eur
            FROM fact_player_season p
            LEFT JOIN season_valuations v
                ON v.player_id = p.player_id AND v.valuation_order = 1
            WHERE p.season = {season_sql}
              {club_filter}
            GROUP BY p.position
            ORDER BY p.position
            """
        )
    )
    valued = sum(1 for row in rows if row.get("market_value_eur") is not None)
    missing = len(rows) - valued
    return payload(
        rows,
        kind="mô tả",
        sample_size=len(rows),
        method="Nối player_id qua bridge Transfermarkt; lấy lần định giá gần nhất trong khoảng 1/7–30/6 của mùa.",
        finding=f"Có định giá cho {valued}/{len(rows)} dòng cầu thủ; {missing} dòng thiếu được giữ là NULL.",
        limitations="Phạm vi phụ thuộc độ phủ ghép danh tính và lịch cập nhật Transfermarkt. Giá trị định giá không phải phí chuyển nhượng hay giá trị đội hình đã kiểm toán.",
        club_id=club_id,
        valued_rows=valued,
        missing_rows=missing,
        positions=position_summary,
    )
