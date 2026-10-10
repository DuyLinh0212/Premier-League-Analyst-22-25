#!/usr/bin/env python3
"""KNN similarity and Top-4 estimation for a historical league table.

Spark SQL builds the team-season features. Spark MLlib StandardScaler is used
before pairwise KNN distances are calculated. The latest selected season is the
query set; earlier selected seasons are the historical reference set.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession, Window, functions as F


DEFAULT_INPUT = "hdfs://localhost:9000/bigdata/processed/facts/fact_team_season"
DEFAULT_SEASONS = ("2022-2023", "2023-2024", "2024-2025")
DEFAULT_CHECKPOINT = (
    "hdfs://localhost:9000/bigdata/checkpoints/analytics/knn_league_table"
)
DEFAULT_OUTPUT_DIR = str(Path(__file__).resolve().parent / "outputs")
SIMULATED_START_YEAR = 2025
FEATURE_COLUMNS = (
    "points_per_mp",
    "goal_diff_per_match",
    "goals_for_per_match",
    "goals_against_per_match",
    "wins_per_match",
)
GROUP_NAMES = {
    "champion": "Nhóm vô địch",
    "champions_league": "Nhóm dự Champions League",
    "mid_table": "Nhóm giữa bảng",
    "relegation": "Nhóm đua trụ hạng",
}
GROUP_COLORS = {
    "champion": "#16a34a",
    "champions_league": "#2563eb",
    "mid_table": "#f59e0b",
    "relegation": "#dc2626",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Spark SQL + StandardScaler + KNN for historical league tables"
    )
    parser.add_argument("--input-path", default=DEFAULT_INPUT)
    parser.add_argument(
        "--seasons",
        default=",".join(DEFAULT_SEASONS),
        help="Comma-separated historical seasons before 2025",
    )
    parser.add_argument(
        "--query-season",
        default=None,
        help="Season to estimate; defaults to the latest selected season",
    )
    parser.add_argument(
        "--neighbors",
        type=int,
        default=5,
        help="Number of nearest historical teams per query team (default: 5)",
    )
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--checkpoint-location", default=DEFAULT_CHECKPOINT)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even when the HDFS input fingerprint matches the latest checkpoint",
    )
    return parser.parse_args()


def parse_seasons(raw: str) -> list[str]:
    seasons = [item.strip() for item in raw.split(",") if item.strip()]
    if not seasons:
        raise ValueError("--seasons must contain at least one season")
    for season in seasons:
        try:
            start_year = int(season[:4])
            end_year = int(season[5:9])
        except (ValueError, IndexError):
            raise ValueError(f"Invalid season: {season!r}; expected YYYY-YYYY") from None
        if len(season) != 9 or season[4] != "-" or end_year != start_year + 1:
            raise ValueError(f"Invalid season: {season!r}; expected YYYY-YYYY")
        if start_year >= SIMULATED_START_YEAR:
            raise ValueError(
                f"Season {season} is simulated by this project; use a historical season before 2025"
            )
    return seasons


def group_expression(column: str) -> F.Column:
    return (
        F.when(F.col(column) == 1, F.lit("champion"))
        .when(F.col(column) <= 4, F.lit("champions_league"))
        .when(F.col(column) <= 10, F.lit("mid_table"))
        .otherwise(F.lit("relegation"))
    )


def build_features(spark: SparkSession, input_path: str, seasons: list[str]) -> DataFrame:
    source = spark.read.parquet(input_path)
    source.createOrReplaceTempView("fact_team_season")
    season_values = ", ".join(f"'{season}'" for season in seasons)

    # This query deliberately derives rates from the count columns instead of
    # using rank/points as KNN features, avoiding direct target leakage.
    query = f"""
        SELECT
            CAST(club_id AS STRING) AS club_id,
            CAST(club_name AS STRING) AS club_name,
            CAST(season AS STRING) AS season,
            CAST(rank AS INT) AS rank,
            CAST(matches_played AS INT) AS matches_played,
            CAST(wins AS INT) AS wins,
            CAST(draws AS INT) AS draws,
            CAST(losses AS INT) AS losses,
            CAST(goals_for AS INT) AS goals_for,
            CAST(goals_against AS INT) AS goals_against,
            CAST(goal_diff AS INT) AS goal_diff,
            CAST(points AS INT) AS points,
            CASE WHEN matches_played > 0 THEN points * 1.0 / matches_played ELSE 0.0 END
                AS points_per_mp,
            CASE WHEN matches_played > 0 THEN goal_diff * 1.0 / matches_played ELSE 0.0 END
                AS goal_diff_per_match,
            CASE WHEN matches_played > 0 THEN goals_for * 1.0 / matches_played ELSE 0.0 END
                AS goals_for_per_match,
            CASE WHEN matches_played > 0 THEN goals_against * 1.0 / matches_played ELSE 0.0 END
                AS goals_against_per_match,
            CASE WHEN matches_played > 0 THEN wins * 1.0 / matches_played ELSE 0.0 END
                AS wins_per_match
        FROM fact_team_season
        WHERE season IN ({season_values})
          AND matches_played > 0
    """
    features = spark.sql(query).withColumn("actual_group", group_expression("rank")).cache()
    counts = {row["season"]: row["count"] for row in features.groupBy("season").count().collect()}
    missing = [season for season in seasons if counts.get(season, 0) == 0]
    if missing:
        raise RuntimeError(
            "No rows found for season(s): " + ", ".join(missing) + f" under {input_path}"
        )
    wrong_counts = {season: count for season, count in counts.items() if count != 20}
    if wrong_counts:
        raise RuntimeError(
            "Expected 20 teams per historical season, got: "
            + ", ".join(f"{season}={count}" for season, count in sorted(wrong_counts.items()))
        )
    return features


def fingerprint(features: DataFrame) -> str:
    cols = [
        "season",
        "club_id",
        "club_name",
        "rank",
        "matches_played",
        "wins",
        "draws",
        "losses",
        "goals_for",
        "goals_against",
        "goal_diff",
        "points",
    ]
    payload = "\n".join(features.select(*cols).orderBy("season", "rank", "club_id").toJSON().collect())
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def fit_scaled(features: DataFrame) -> DataFrame:
    assembler = VectorAssembler(
        inputCols=list(FEATURE_COLUMNS),
        outputCol="raw_features",
        handleInvalid="error",
    )
    assembled = assembler.transform(features)
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True,
    )
    return scaler.fit(assembled).transform(assembled).withColumn(
        "scaled_array", vector_to_array("scaled_features")
    )


def run_knn(
    scaled: DataFrame, query_season: str, neighbor_count: int
) -> tuple[DataFrame, DataFrame]:
    if neighbor_count < 1:
        raise ValueError("--neighbors must be at least 1")
    query = scaled.filter(F.col("season") == query_season).select(
        F.col("season").alias("query_season"),
        F.col("club_id").alias("query_club_id"),
        F.col("club_name").alias("query_club_name"),
        F.col("rank").alias("actual_rank"),
        F.col("points").alias("actual_points"),
        F.col("points_per_mp").alias("actual_points_per_mp"),
        F.col("actual_group").alias("actual_group"),
        F.col("scaled_array").alias("query_vector"),
    )
    reference = scaled.filter(F.col("season") != query_season).select(
        F.col("season").alias("neighbor_season"),
        F.col("club_id").alias("neighbor_club_id"),
        F.col("club_name").alias("neighbor_club_name"),
        F.col("rank").alias("neighbor_rank"),
        F.col("points").alias("neighbor_points"),
        F.col("points_per_mp").alias("neighbor_points_per_mp"),
        F.col("actual_group").alias("neighbor_group"),
        F.col("scaled_array").alias("neighbor_vector"),
    )
    if query.limit(1).count() == 0:
        raise RuntimeError(f"Query season {query_season} has no rows")
    if reference.limit(1).count() == 0:
        raise RuntimeError("At least one earlier reference season is required for KNN")

    squared_distance = F.lit(0.0)
    for index in range(len(FEATURE_COLUMNS)):
        squared_distance = squared_distance + F.pow(
            F.col("query_vector")[index] - F.col("neighbor_vector")[index], 2
        )
    pairs = query.crossJoin(reference).withColumn(
        "distance", F.sqrt(squared_distance)
    )
    nearest_window = Window.partitionBy("query_club_id").orderBy(
        F.col("distance").asc(),
        F.col("neighbor_season").asc(),
        F.col("neighbor_club_id").asc(),
    )
    neighbors = (
        pairs.withColumn("neighbor_order", F.row_number().over(nearest_window))
        .filter(F.col("neighbor_order") <= neighbor_count)
        .drop("query_vector", "neighbor_vector")
        .cache()
    )

    summary = neighbors.groupBy(
        "query_season",
        "query_club_id",
        "query_club_name",
        "actual_rank",
        "actual_points",
        "actual_points_per_mp",
        "actual_group",
    ).agg(
        F.avg("distance").alias("average_neighbor_distance"),
        F.avg("neighbor_rank").alias("predicted_rank"),
        F.avg("neighbor_points").alias("predicted_points"),
        F.avg("neighbor_points_per_mp").alias("predicted_points_per_mp"),
        F.avg(F.when(F.col("neighbor_rank") <= 4, 1.0).otherwise(0.0)).alias(
            "top4_probability"
        ),
    )
    group_counts = neighbors.groupBy("query_club_id", "neighbor_group").count()
    group_window = Window.partitionBy("query_club_id").orderBy(
        F.col("count").desc(), F.col("neighbor_group").asc()
    )
    majority_group = (
        group_counts.withColumn("group_order", F.row_number().over(group_window))
        .filter(F.col("group_order") == 1)
        .select("query_club_id", F.col("neighbor_group").alias("predicted_group"))
    )
    predictions = summary.join(majority_group, "query_club_id", "left").withColumn(
        "predicted_group_name",
        F.create_map(
            F.lit("champion"), F.lit(GROUP_NAMES["champion"]),
            F.lit("champions_league"), F.lit(GROUP_NAMES["champions_league"]),
            F.lit("mid_table"), F.lit(GROUP_NAMES["mid_table"]),
            F.lit("relegation"), F.lit(GROUP_NAMES["relegation"]),
        )[F.col("predicted_group")],
    )
    return predictions, neighbors


def clean_json(value: Any) -> Any:
    if isinstance(value, Decimal):
        return clean_json(float(value))
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 6)
    if isinstance(value, dict):
        return {str(key): clean_json(item) for key, item in value.items()}
    return value


def records_from_df(data: DataFrame, columns: list[str], order_by: list[str]) -> list[dict[str, Any]]:
    records = []
    for row in data.select(*columns).orderBy(*order_by).collect():
        records.append(clean_json(row.asDict(recursive=True)))
    return records


def write_csv(records: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not records:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)


def hdfs_fs(spark: SparkSession, uri: str):
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        path.toUri(), spark.sparkContext._jsc.hadoopConfiguration()
    )
    return fs, path


def read_hdfs_text(spark: SparkSession, uri: str) -> str | None:
    fs, path = hdfs_fs(spark, uri)
    if not fs.exists(path):
        return None
    stream = fs.open(path)
    buffer = spark._jvm.java.io.ByteArrayOutputStream()
    try:
        spark._jvm.org.apache.hadoop.io.IOUtils.copyBytes(
            stream, buffer, spark.sparkContext._jsc.hadoopConfiguration(), False
        )
        return bytes(buffer.toByteArray()).decode("utf-8")
    finally:
        stream.close()
        buffer.close()


def write_hdfs_text(spark: SparkSession, uri: str, text: str, append: bool = False) -> None:
    fs, path = hdfs_fs(spark, uri)
    parent = path.getParent()
    if parent is not None:
        fs.mkdirs(parent)
    target_exists = fs.exists(path)
    temp = None
    if append and target_exists:
        stream = fs.append(path)
    else:
        temp = spark._jvm.org.apache.hadoop.fs.Path(f"{uri}.tmp-{uuid.uuid4().hex}")
        stream = fs.create(temp, True)
    try:
        stream.write(bytearray(text.encode("utf-8")))
        stream.hflush()
    finally:
        stream.close()
    # A first append has no target yet, so it also needs the atomic rename.
    # Subsequent appends write directly to the existing history file.
    if temp is not None:
        if fs.exists(path):
            fs.delete(path, False)
        if not fs.rename(temp, path):
            raise RuntimeError(f"Could not atomically write HDFS checkpoint: {uri}")


def checkpoint_state(spark: SparkSession, root: str) -> dict[str, Any] | None:
    raw = read_hdfs_text(spark, root.rstrip("/") + "/latest.json")
    if not raw:
        return None
    return json.loads(raw)


def render_html(
    predictions: list[dict[str, Any]],
    neighbors: list[dict[str, Any]],
    query_season: str,
    path: Path,
) -> None:
    predictions_json = json.dumps(predictions, ensure_ascii=False).replace("</", "<\\/")
    neighbors_json = json.dumps(neighbors, ensure_ascii=False).replace("</", "<\\/")
    names_json = json.dumps(GROUP_NAMES, ensure_ascii=False)
    colors_json = json.dumps(GROUP_COLORS)
    options = "".join(
        f'<option value="{row["query_club_id"]}">{row["query_club_name"]}</option>'
        for row in predictions
    )
    html = f"""<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  <title>KNN league table — {query_season}</title>
  <style>
    :root {{ font-family:Inter,Segoe UI,Arial,sans-serif; color:#14233d; }}
    body {{ margin:0; background:#f5f7fb; }}
    .page {{ max-width:1180px; margin:auto; padding:28px 20px 46px; }}
    h1 {{ margin:0 0 6px; font-size:clamp(24px,3vw,38px); }}
    .muted {{ color:#61708a; }}
    .panel {{ background:#fff; border:1px solid #e3e8f0; border-radius:16px; box-shadow:0 8px 24px #16243d12; padding:18px; margin:16px 0; }}
    .controls {{ display:flex; flex-wrap:wrap; align-items:center; gap:12px; }}
    select {{ border:1px solid #ccd5e3; border-radius:9px; padding:9px 11px; background:white; min-width:250px; }}
    .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:10px; }}
    .card {{ background:#f8fafc; border:1px solid #e5eaf1; border-radius:12px; padding:13px; }}
    .card strong {{ display:block; font-size:24px; margin-top:5px; }}
    .bar-row {{ display:grid; grid-template-columns:180px 1fr 70px; gap:10px; align-items:center; margin:9px 0; font-size:13px; }}
    .bar-track {{ height:15px; background:#e9edf4; border-radius:10px; overflow:hidden; }}
    .bar {{ height:100%; border-radius:10px; }}
    table {{ width:100%; border-collapse:collapse; font-size:13px; min-width:780px; }}
    th,td {{ padding:9px 10px; border-bottom:1px solid #edf0f5; text-align:left; white-space:nowrap; }}
    th {{ color:#526079; background:#f8fafc; }}
    .table-wrap {{ overflow:auto; }}
    .pill {{ color:white; border-radius:999px; padding:4px 8px; font-size:12px; }}
    .note {{ font-size:12px; color:#68768d; margin-top:12px; }}
  </style>
</head>
<body>
  <main class="page">
    <h1>KNN tìm đội bóng tương tự</h1>
    <p class="muted">Mùa truy vấn: <strong>{query_season}</strong>. Feature đã được chuẩn hóa bằng StandardScaler; KNN so sánh với các mùa lịch sử trước đó.</p>
    <section class="panel controls"><label for="team">Chọn đội hiện tại</label><select id="team">{options}</select></section>
    <section class="panel"><div id="cards" class="cards"></div></section>
    <section class="panel"><h2>Xác suất Top 4 theo KNN</h2><div id="bars"></div><p class="note">Xác suất = tỷ lệ đội láng giềng nằm trong Top 4.</p></section>
    <section class="panel table-wrap"><h2>Các đội/mùa gần nhất</h2><table><thead><tr><th>#</th><th>Mùa</th><th>Đội tương tự</th><th>Khoảng cách</th><th>Hạng</th><th>Điểm/Trận</th><th>Nhóm</th></tr></thead><tbody id="neighborRows"></tbody></table></section>
    <p class="note">Các nhóm được suy ra từ hạng lịch sử: hạng 1, Top 4, hạng 5–10 và hạng 11–20. KNN dùng {len(neighbors) // max(len(predictions), 1)} láng giềng cho mỗi đội.</p>
  </main>
  <script>
    const PREDICTIONS = {predictions_json};
    const NEIGHBORS = {neighbors_json};
    const NAMES = {names_json};
    const COLORS = {colors_json};
    const team = document.getElementById('team');
    const cards = document.getElementById('cards');
    const bars = document.getElementById('bars');
    const neighborRows = document.getElementById('neighborRows');
    function esc(v) {{ return String(v).replace(/[&<>\"']/g,c=>({{'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}}[c])); }}
    function label(group) {{ return NAMES[group] || group; }}
    function render() {{
      const selected = PREDICTIONS.find(p => p.query_club_id === team.value) || PREDICTIONS[0];
      cards.innerHTML = [
        ['Hạng thực tế', selected.actual_rank],
        ['Hạng dự đoán', Number(selected.predicted_rank).toFixed(1)],
        ['Xác suất Top 4', (Number(selected.top4_probability)*100).toFixed(0)+'%'],
        ['Nhóm dự đoán', label(selected.predicted_group)]
      ].map(x => `<div class="card"><span>${{x[0]}}</span><strong>${{esc(x[1])}}</strong></div>`).join('');
      const sorted = PREDICTIONS.slice().sort((a,b) => Number(b.top4_probability)-Number(a.top4_probability));
      bars.innerHTML = sorted.map(p => `<div class="bar-row"><span>${{esc(p.query_club_name)}}</span><div class="bar-track"><div class="bar" style="width:${{Number(p.top4_probability)*100}}%;background:${{COLORS[p.predicted_group]}}"></div></div><strong>${{(Number(p.top4_probability)*100).toFixed(0)}}%</strong></div>`).join('');
      const near = NEIGHBORS.filter(n => n.query_club_id === selected.query_club_id).sort((a,b)=>a.neighbor_order-b.neighbor_order);
      neighborRows.innerHTML = near.map(n => `<tr><td>${{n.neighbor_order}}</td><td>${{esc(n.neighbor_season)}}</td><td>${{esc(n.neighbor_club_name)}}</td><td>${{Number(n.distance).toFixed(3)}}</td><td>${{n.neighbor_rank}}</td><td>${{Number(n.neighbor_points_per_mp).toFixed(2)}}</td><td><span class="pill" style="background:${{COLORS[n.neighbor_group]}}">${{esc(label(n.neighbor_group))}}</span></td></tr>`).join('');
    }}
    team.addEventListener('change', render); render();
  </script>
</body>
</html>
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")


def main() -> int:
    args = parse_args()
    seasons = parse_seasons(args.seasons)
    query_season = args.query_season or sorted(seasons)[-1]
    if query_season not in seasons:
        raise ValueError(f"--query-season {query_season} is not in --seasons")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    html_path = output_dir / "knn_league_table.html"
    predictions_csv = output_dir / "knn_predictions.csv"
    neighbors_csv = output_dir / "knn_neighbors.csv"

    spark = (
        SparkSession.builder.appName("HistoricalLeagueTableKNN")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        features = build_features(spark, args.input_path, seasons)
        source_fingerprint = fingerprint(features)
        checkpoint = checkpoint_state(spark, args.checkpoint_location)
        checkpoint_key = {
            "input_path": args.input_path,
            "seasons": seasons,
            "query_season": query_season,
            "neighbors": args.neighbors,
            "input_fingerprint": source_fingerprint,
        }
        outputs_exist = html_path.exists() and predictions_csv.exists() and neighbors_csv.exists()
        if not args.force and checkpoint and checkpoint.get("config") == checkpoint_key and outputs_exist:
            print("HDFS input has not changed; KNN outputs are already up to date.")
            print(f"Last run: {checkpoint.get('run_at_utc', 'unknown')}")
            return 0

        scaled = fit_scaled(features).cache()
        predictions, neighbors = run_knn(scaled, query_season, args.neighbors)
        prediction_columns = [
            "query_season",
            "query_club_id",
            "query_club_name",
            "actual_rank",
            "actual_points",
            "actual_points_per_mp",
            "actual_group",
            "predicted_rank",
            "predicted_points",
            "predicted_points_per_mp",
            "top4_probability",
            "average_neighbor_distance",
            "predicted_group",
            "predicted_group_name",
        ]
        neighbor_columns = [
            "query_season",
            "query_club_id",
            "query_club_name",
            "actual_rank",
            "neighbor_order",
            "neighbor_season",
            "neighbor_club_id",
            "neighbor_club_name",
            "neighbor_rank",
            "neighbor_points",
            "neighbor_points_per_mp",
            "neighbor_group",
            "distance",
        ]
        prediction_records = records_from_df(predictions, prediction_columns, ["actual_rank"])
        neighbor_records = records_from_df(
            neighbors, neighbor_columns, ["query_club_id", "neighbor_order"]
        )
        write_csv(prediction_records, predictions_csv)
        write_csv(neighbor_records, neighbors_csv)
        render_html(prediction_records, neighbor_records, query_season, html_path)

        now = datetime.now(timezone.utc).isoformat()
        history_entry = {
            "run_at_utc": now,
            "config": checkpoint_key,
            "rows": len(prediction_records),
            "neighbor_rows": len(neighbor_records),
            "outputs": [str(predictions_csv.resolve()), str(neighbors_csv.resolve()), str(html_path.resolve())],
        }
        write_hdfs_text(
            spark,
            args.checkpoint_location.rstrip("/") + "/history.jsonl",
            json.dumps(history_entry, ensure_ascii=False) + "\n",
            append=True,
        )
        write_hdfs_text(
            spark,
            args.checkpoint_location.rstrip("/") + "/latest.json",
            json.dumps(history_entry, ensure_ascii=False, indent=2),
        )
        print(f"KNN query season: {query_season}")
        print(f"Reference seasons: {', '.join(s for s in seasons if s != query_season)}")
        print(f"Query teams: {len(prediction_records)}; neighbor rows: {len(neighbor_records)}")
        print(f"Input fingerprint: {source_fingerprint}")
        print(f"Predictions CSV: {predictions_csv.resolve()}")
        print(f"Neighbors CSV: {neighbors_csv.resolve()}")
        print(f"Interactive HTML: {html_path.resolve()}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
