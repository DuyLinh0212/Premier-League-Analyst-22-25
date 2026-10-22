from __future__ import annotations

from typing import Any

from app.query_utils import collect, payload, sql_string


def run(spark: Any, season: str) -> dict[str, Any]:
    rows = collect(
        spark.sql(
            f"""
            WITH transfers AS (
                SELECT
                    to_club_id AS club_id,
                    COUNT(*) AS inbound_transfers,
                    SUM(CASE WHEN fee_status = 'reported_positive' AND fee_eur > 0 THEN fee_eur END) AS reported_spend_eur,
                    SUM(CASE WHEN fee_status = 'reported_positive' AND fee_eur > 0 THEN 1 ELSE 0 END) AS reported_fee_count,
                    SUM(CASE WHEN fee_status <> 'reported_positive' OR fee_eur IS NULL THEN 1 ELSE 0 END) AS excluded_fee_count
                FROM fact_transfer
                WHERE season = {sql_string(season)}
                  AND to_active_pl_season = true
                GROUP BY to_club_id
            ),
            base AS (
                SELECT
                    t.club_id,
                    coalesce(c.club_name, t.club_id) AS club_name,
                    x.inbound_transfers,
                    x.reported_fee_count,
                    x.excluded_fee_count,
                    CAST(x.reported_spend_eur AS DOUBLE) AS reported_spend_eur,
                    t.rank AS league_rank,
                    t.points,
                    t.goals_for,
                    t.goals_against
                FROM fact_team_season t
                LEFT JOIN transfers x ON x.club_id = t.club_id
                LEFT JOIN club_names c ON c.club_id = t.club_id
                WHERE t.season = {sql_string(season)}
            ),
            ranked AS (
                SELECT
                    *,
                    RANK() OVER (ORDER BY reported_spend_eur ASC NULLS LAST) AS spend_rank,
                    RANK() OVER (ORDER BY league_rank ASC) AS table_rank
                FROM base
                WHERE reported_spend_eur IS NOT NULL
            ),
            correlation AS (
                SELECT corr(spend_rank, table_rank) AS spearman_rho FROM ranked
            )
            SELECT
                b.*,
                r.spend_rank,
                r.table_rank,
                (SELECT spearman_rho FROM correlation) AS spearman_rho
            FROM base b
            LEFT JOIN ranked r ON r.club_id = b.club_id
            ORDER BY b.reported_spend_eur DESC NULLS LAST
            """
        )
    )
    eligible = [row for row in rows if row.get("reported_spend_eur") is not None]
    rho = next((row.get("spearman_rho") for row in rows if row.get("spearman_rho") is not None), None)
    finding = "Chưa đủ đội có phí mua vào được báo cáo để tính tương quan Spearman."
    if rho is not None:
        if rho < -0.25:
            direction = "Mức chi cao hơn có liên hệ với thứ hạng số nhỏ hơn (vị trí tốt hơn)"
        elif rho > 0.25:
            direction = "Mức chi cao hơn có liên hệ với thứ hạng số lớn hơn (vị trí thấp hơn)"
        else:
            direction = "Chưa thấy liên hệ thứ hạng rõ theo Spearman"
        finding = f"{direction}; rho = {rho:.2f} trên {len(eligible)} đội đủ dữ liệu."
    excluded = sum(row.get("excluded_fee_count") or 0 for row in rows)
    return payload(
        rows,
        kind="tương quan",
        sample_size=len(eligible),
        method="Tổng phí mua vào có fee_status=reported_positive; Spearman giữa thứ hạng chi và thứ hạng cuối mùa.",
        finding=finding,
        limitations=f"Chỉ gồm phí được báo cáo, loại {excluded} giao dịch thiếu/không xác nhận phí. Tương quan không chứng minh chi tiêu gây ra thay đổi thành tích.",
        spearman_rho=rho,
        eligible_clubs=len(eligible),
        clubs_without_reported_fees=len(rows) - len(eligible),
    )
