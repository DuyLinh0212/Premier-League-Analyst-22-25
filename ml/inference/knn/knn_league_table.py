#!/usr/bin/env python3
"""Spark KNN rank estimates and chronological metrics for football teams.

Build four club-season features; fit MinMax scaling on historical references and
evaluate held-out completed seasons without temporal leakage.
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

from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame, SparkSession, Window, functions as F


DEFAULT_INPUT = "hdfs://localhost:9000/bigdata/processed/facts/fact_team_season"
DEFAULT_CHECKPOINT = (
    "hdfs://localhost:9000/bigdata/checkpoints/analytics/knn_league_table"
)
DEFAULT_OUTPUT_DIR = str(Path(__file__).resolve().parents[3] / "data/models/knn")
FACT_COLUMNS = (
    "club_id",
    "club_name",
    "season",
    "rank",
    "matches_played",
    "wins",
    "draws",
    "losses",
    "goals_for",
    "goals_against",
    "goal_diff",
    "points",
)
FEATURE_COLUMNS = (
    "points_per_mp",
    "goal_diff_per_match",
    "recent_form",
    "goals_for_per_match",
)
MATCH_COLUMNS = ("club_id", "season", "match_id", "match_date", "result")
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
    description="Spark SQL + MinMaxScaler + KNN for league-table estimates"
    )
    parser.add_argument("--input-path", default=DEFAULT_INPUT)
    parser.add_argument(
        "--match-input-path",
        default="hdfs://localhost:9000/bigdata/processed/facts/fact_team_match",
        help="Match-level Silver Parquet used to derive recent form",
    )
    parser.add_argument(
        "--seasons",
        default=None,
        help="Comma-separated seasons; defaults to every available season",
    )
    parser.add_argument(
        "--additional-input-path",
        action="append",
        default=[],
        help="Additional season-partitioned Parquet data; committed Kafka seasons are included",
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


def parse_seasons(raw: str | None) -> list[str]:
    if raw is None:
        raise ValueError("No seasons supplied")
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
    return sorted(set(seasons))


def _committed_season_partitions(spark: SparkSession, root: str) -> list[str]:
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(root)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return []
    partitions = []
    for status in fs.listStatus(path):
        if not status.isDirectory():
            continue
        name = status.getPath().getName()
        season = name.removeprefix("season=")
        try:
            parse_seasons(season)
        except ValueError:
            continue
        marker = jvm.org.apache.hadoop.fs.Path(status.getPath(), "_source_sha256")
        if fs.exists(marker):
            partitions.append(status.getPath().toString())
    return sorted(partitions)


def _load_fact_tables(
    spark: SparkSession, input_path: str, additional_input_paths: list[str]
) -> DataFrame:
    frames: list[DataFrame] = []
    base = spark.read.parquet(input_path)
    missing = sorted(set(FACT_COLUMNS) - set(base.columns))
    if missing:
        raise RuntimeError(f"KNN input {input_path} is missing columns: {', '.join(missing)}")
    base = base.select(*FACT_COLUMNS)
    if base.groupBy("season", "club_id").count().where("count > 1").limit(1).count():
        raise RuntimeError(f"KNN input {input_path} contains duplicate club-season rows")
    frames.append(base.withColumn("__source_priority", F.lit(0)))

    for priority, path in enumerate(additional_input_paths, start=1):
        partitions = _committed_season_partitions(spark, path)
        if not partitions:
            print(f"Không tìm thấy phân vùng mùa giải đã công bố tại {path}; bỏ qua dữ liệu bổ sung tùy chọn.", flush=True)
            continue
        extra = spark.read.option("basePath", path).parquet(*partitions)
        missing = sorted(set(FACT_COLUMNS) - set(extra.columns))
        if missing:
            raise RuntimeError(f"KNN input {path} is missing columns: {', '.join(missing)}")
        extra = extra.select(*FACT_COLUMNS)
        if extra.groupBy("season", "club_id").count().where("count > 1").limit(1).count():
            raise RuntimeError(f"KNN input {path} contains duplicate club-season rows")
        frames.append(extra.withColumn("__source_priority", F.lit(priority)))

    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)

    # If a season is present in both Silver and Kafka, use the committed Kafka
    # snapshot for that season and keep one row per club-season key.
    source_window = Window.partitionBy("season", "club_id").orderBy(
        F.col("__source_priority").desc()
    )
    return (
        combined.withColumn("__source_rank", F.row_number().over(source_window))
        .filter(F.col("__source_rank") == 1)
        .drop("__source_priority", "__source_rank")
    )


def build_features(
    spark: SparkSession,
    input_path: str,
    seasons: list[str] | None,
    additional_input_paths: list[str] | None = None,
    match_input_path: str | None = None,
) -> tuple[DataFrame, list[str]]:
    source = _load_fact_tables(spark, input_path, additional_input_paths or [])
    if seasons is None:
        available = [
            row["season"]
            for row in source.select(F.col("season").cast("string").alias("season"))
            .where(F.col("season").isNotNull())
            .distinct()
            .collect()
        ]
        if not available:
            raise RuntimeError("No season values found in the KNN input tables")
        seasons = parse_seasons(",".join(available))
    source.createOrReplaceTempView("fact_team_season")
    season_values = ", ".join(f"'{season}'" for season in seasons)

    # This query deliberately derives rates from the count columns instead of
    # using rank/points as KNN features, avoiding direct target leakage.
    matches = spark.read.parquet(match_input_path) if match_input_path else None
    if matches is None:
        raise RuntimeError("--match-input-path is required to calculate the recent-form feature")
    missing_match = sorted(set(MATCH_COLUMNS) - set(matches.columns))
    if missing_match:
        raise RuntimeError(f"Match input {match_input_path} is missing columns: {', '.join(missing_match)}")
    matches.select(*MATCH_COLUMNS).createOrReplaceTempView("fact_team_match_history")
    query = f"""
        WITH ordered_matches AS (
            SELECT
                CAST(club_id AS STRING) AS club_id,
                CAST(season AS STRING) AS season,
                CASE UPPER(TRIM(CAST(result AS STRING)))
                    WHEN 'W' THEN 3.0 WHEN 'D' THEN 1.0 WHEN 'L' THEN 0.0
                END AS match_points,
                ROW_NUMBER() OVER (
                    PARTITION BY season, club_id
                    ORDER BY match_date DESC, match_id DESC
                ) AS recent_order
            FROM fact_team_match_history
            WHERE season IN ({season_values})
        ),
        recent AS (
            SELECT season, club_id, AVG(match_points) AS recent_form
            FROM ordered_matches
            WHERE recent_order <= 5 AND match_points IS NOT NULL
            GROUP BY season, club_id
        )
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
            COALESCE(recent.recent_form,
                CASE WHEN matches_played > 0 THEN points * 1.0 / matches_played ELSE 0.0 END
            ) AS recent_form,
            CASE WHEN matches_played > 0 THEN goals_for * 1.0 / matches_played ELSE 0.0 END
                AS goals_for_per_match
        FROM fact_team_season
        LEFT JOIN recent USING (season, club_id)
        WHERE season IN ({season_values})
          AND matches_played > 0
    """
    features = spark.sql(query).cache()
    if features.where(F.col("recent_form").isNull()).limit(1).count():
        raise RuntimeError("Recent form could not be calculated for one or more club-seasons")
    counts = {row["season"]: row["count"] for row in features.groupBy("season").count().collect()}
    missing = [season for season in seasons if counts.get(season, 0) == 0]
    if missing:
        raise RuntimeError(
            "No rows found for season(s): " + ", ".join(missing) + f" under {input_path}"
        )
    wrong_counts = {season: count for season, count in counts.items() if count != 20}
    if wrong_counts:
        raise RuntimeError(
            "Expected 20 teams per season, got: "
            + ", ".join(f"{season}={count}" for season, count in sorted(wrong_counts.items()))
        )
    return features, seasons


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
        *FEATURE_COLUMNS,
    ]
    payload = "\n".join(features.select(*cols).orderBy("season", "rank", "club_id").toJSON().collect())
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def fit_scaled_fold(reference: DataFrame, query: DataFrame) -> DataFrame:
    """Scale a held-out season using reference seasons only (no fold leakage)."""
    assembler = VectorAssembler(
        inputCols=list(FEATURE_COLUMNS),
        outputCol="raw_features",
        handleInvalid="error",
    )
    reference_vector = assembler.transform(reference)
    query_vector = assembler.transform(query)
    scaler = MinMaxScaler(inputCol="raw_features", outputCol="scaled_features", min=0.0, max=1.0).fit(reference_vector)
    return (
        scaler.transform(reference_vector)
        .unionByName(scaler.transform(query_vector))
        .withColumn("scaled_array", vector_to_array("scaled_features"))
    )


def _rank_correlation(actual: list[float], predicted: list[float]) -> float | None:
    if len(actual) < 2:
        return None
    actual_mean = sum(actual) / len(actual)
    predicted_mean = sum(predicted) / len(predicted)
    actual_ss = sum((value - actual_mean) ** 2 for value in actual)
    predicted_ss = sum((value - predicted_mean) ** 2 for value in predicted)
    denominator = math.sqrt(actual_ss * predicted_ss)
    if denominator == 0:
        return None
    return sum((a - actual_mean) * (p - predicted_mean) for a, p in zip(actual, predicted)) / denominator


def evaluate_backtest(features: DataFrame, seasons: list[str], query_season: str, neighbor_count: int) -> dict[str, Any]:
    """Chronological leave-future-seasons-out evaluation for completed seasons."""
    fold_reports = []
    all_actual: list[float] = []
    all_estimated: list[float] = []
    all_points: list[tuple[float, float]] = []
    all_top4: list[tuple[bool, bool]] = []
    for season in seasons:
        if season == query_season:
            continue
        reference_seasons = [value for value in seasons if value < season]
        if not reference_seasons:
            continue
        reference = features.filter(F.col("season").isin(reference_seasons))
        held_out = features.filter(F.col("season") == season)
        scaled = fit_scaled_fold(reference, held_out)
        predictions = run_knn(scaled, season, neighbor_count)
        rows = [row.asDict(recursive=True) for row in predictions.collect()]
        rows.sort(key=lambda row: (float(row["predicted_rank"]), row["query_club_id"]))
        actual_positions = [int(row["actual_rank"]) for row in rows]
        predicted_positions = [index + 1 for index in range(len(rows))]
        rank_errors = [predicted - actual for actual, predicted in zip(actual_positions, predicted_positions)]
        point_pairs = [(float(row["actual_points"]), float(row["predicted_points"])) for row in rows]
        actual_top4 = {row["query_club_id"] for row in rows if int(row["actual_rank"]) <= 4}
        predicted_top4 = {row["query_club_id"] for row in rows[:4]}
        tp = len(actual_top4 & predicted_top4)
        tn = len(set(row["query_club_id"] for row in rows) - actual_top4 - predicted_top4)
        count = len(rows)
        estimated_top4 = {row["query_club_id"] for row in rows[:4]}
        for index, row in enumerate(rows):
            all_actual.append(float(row["actual_rank"]))
            all_estimated.append(float(predicted_positions[index]))
            all_points.append((float(row["actual_points"]), float(row["predicted_points"])))
            all_top4.append((row["query_club_id"] in actual_top4, row["query_club_id"] in estimated_top4))
        fold_reports.append({
            "season": season,
            "reference_seasons": reference_seasons,
            "samples": count,
            "rank_mae": sum(abs(error) for error in rank_errors) / max(1, count),
            "rank_rmse": math.sqrt(sum(error * error for error in rank_errors) / max(1, count)),
            "rank_spearman": _rank_correlation(actual_positions, predicted_positions),
            "points_mae": sum(abs(actual - predicted) for actual, predicted in point_pairs) / max(1, count),
            "points_rmse": math.sqrt(sum((actual - predicted) ** 2 for actual, predicted in point_pairs) / max(1, count)),
            "top4_accuracy": (tp + tn) / max(1, count),
            "top4_precision": tp / 4,
            "top4_recall": tp / 4,
            "top4_f1": tp / 4,
        })
    if not fold_reports:
        raise RuntimeError("Không đủ mùa hoàn tất trước mùa ước tính để đánh giá hồi cứu KNN")
    total = sum(item["samples"] for item in fold_reports)
    actual_top = sum(actual for actual, _ in all_top4)
    predicted_top = sum(predicted for _, predicted in all_top4)
    tp = sum(actual and predicted for actual, predicted in all_top4)
    tn = sum(not actual and not predicted for actual, predicted in all_top4)
    rank_errors = [predicted - actual for actual, predicted in zip(all_actual, all_estimated)]
    return {
        "model": "KNN bảng xếp hạng",
        "features": list(FEATURE_COLUMNS),
        "scaling": "MinMaxScaler [0, 1], khớp trên các mùa tham chiếu của từng fold",
        "neighbors": neighbor_count,
        "evaluation_scope": "Hồi cứu theo thứ tự mùa giải; mỗi mùa được dự đoán chỉ từ các mùa trước đó.",
        "limitation": "Đặc trưng là thống kê tổng kết mùa của chính mùa được giữ lại; đây là kiểm định hồi cứu, không phải dự báo trước mùa.",
        "folds": fold_reports,
        "overall": {
            "samples": total,
            "seasons": [item["season"] for item in fold_reports],
            "rank_mae": sum(abs(error) for error in rank_errors) / max(1, total),
            "rank_rmse": math.sqrt(sum(error * error for error in rank_errors) / max(1, total)),
            "rank_spearman": _rank_correlation(all_actual, all_estimated),
            "points_mae": sum(abs(actual - predicted) for actual, predicted in all_points) / max(1, total),
            "points_rmse": math.sqrt(sum((actual - predicted) ** 2 for actual, predicted in all_points) / max(1, total)),
            "top4_accuracy": (tp + tn) / max(1, total),
            "top4_precision": tp / max(1, predicted_top),
            "top4_recall": tp / max(1, actual_top),
            "top4_f1": (2 * tp / max(1, predicted_top + actual_top)),
        },
    }


def run_knn(
    scaled: DataFrame, query_season: str, neighbor_count: int
) -> DataFrame:
    if neighbor_count < 1:
        raise ValueError("--neighbors must be at least 1")
    query = scaled.filter(F.col("season") == query_season).select(
        F.col("season").alias("query_season"),
        F.col("club_id").alias("query_club_id"),
        F.col("club_name").alias("query_club_name"),
        F.col("rank").alias("actual_rank"),
        F.col("points").alias("actual_points"),
        F.col("scaled_array").alias("query_vector"),
    )
    reference = scaled.filter(F.col("season") != query_season).select(
        F.col("season").alias("neighbor_season"),
        F.col("club_id").alias("neighbor_club_id"),
        F.col("club_name").alias("neighbor_club_name"),
        F.col("rank").alias("neighbor_rank"),
        F.col("points").alias("neighbor_points"),
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
    ).agg(
        F.first("actual_rank", ignorenulls=True).alias("actual_rank"),
        F.first("actual_points", ignorenulls=True).alias("actual_points"),
        F.avg("neighbor_rank").alias("predicted_rank"),
        F.avg("neighbor_points").alias("predicted_points"),
        F.avg(F.when(F.col("neighbor_rank") <= 4, 1.0).otherwise(0.0)).alias(
            "top4_probability"
        ),
    )
    return summary


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



def main() -> int:
    args = parse_args()
    requested_seasons = parse_seasons(args.seasons) if args.seasons else None

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    predictions_csv = output_dir / "knn_predictions.csv"
    metrics_json = output_dir / "knn_metrics.json"
    for retired_output in ("knn_neighbors.csv", "knn_league_table.html"):
        (output_dir / retired_output).unlink(missing_ok=True)

    spark = (
        SparkSession.builder.appName("LeagueTableKNN")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        features, seasons = build_features(
            spark,
            args.input_path,
            requested_seasons,
            args.additional_input_path,
        )
        query_season = args.query_season or seasons[-1]
        if query_season not in seasons:
            raise ValueError(f"--query-season {query_season} is not in --seasons")
        source_fingerprint = fingerprint(features)
        checkpoint = checkpoint_state(spark, args.checkpoint_location)
        checkpoint_key = {
            "input_path": args.input_path,
            "additional_input_paths": args.additional_input_path,
            "seasons": seasons,
            "query_season": query_season,
            "neighbors": args.neighbors,
            "input_fingerprint": source_fingerprint,
            "output_contract": "minmax-four-features-with-ml-metrics-v4",
        }
        outputs_exist = predictions_csv.exists() and metrics_json.exists()
        if not args.force and checkpoint and checkpoint.get("config") == checkpoint_key and outputs_exist:
            print("Dữ liệu HDFS không thay đổi; kết quả KNN hiện tại vẫn còn mới.", flush=True)
            print(f"Thời điểm tính gần nhất: {checkpoint.get('run_at_utc', 'không rõ')}", flush=True)
            return 0

        training = features.filter(F.col("season") < query_season)
        query = features.filter(F.col("season") == query_season)
        if training.limit(1).count() == 0:
            raise RuntimeError("Ít nhất một mùa hoàn tất trước mùa cần ước tính là bắt buộc")
        scaled = fit_scaled_fold(training, query).cache()
        predictions = run_knn(scaled, query_season, args.neighbors)
        metrics = evaluate_backtest(features, seasons, query_season, args.neighbors)
        prediction_columns = [
            "query_season",
            "query_club_id",
            "query_club_name",
            "actual_rank",
            "actual_points",
            "predicted_rank",
            "predicted_points",
            "top4_probability",
        ]
        prediction_records = records_from_df(predictions, prediction_columns, ["query_club_id"])
        write_csv(prediction_records, predictions_csv)
        metrics_json.write_text(
            json.dumps(metrics, ensure_ascii=False, indent=2, allow_nan=False),
            encoding="utf-8",
        )
        now = datetime.now(timezone.utc).isoformat()
        history_entry = {
            "run_at_utc": now,
            "config": checkpoint_key,
            "rows": len(prediction_records),
            "outputs": [str(predictions_csv.resolve()), str(metrics_json.resolve())],
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
        print(f"Mùa được ước tính: {query_season}", flush=True)
        print(f"Các mùa dùng làm tham chiếu: {', '.join(s for s in seasons if s != query_season)}", flush=True)
        print(f"Số câu lạc bộ được ước tính: {len(prediction_records)}", flush=True)
        print(
            "Đánh giá KNN hồi cứu: "
            f"Top 4 đúng {metrics['overall']['top4_accuracy']:.1%}; "
            f"MAE thứ hạng {metrics['overall']['rank_mae']:.2f}; "
            f"RMSE thứ hạng {metrics['overall']['rank_rmse']:.2f}; "
            f"mẫu {metrics['overall']['samples']}.",
            flush=True,
        )
        print(f"Tệp metric KNN: {metrics_json.resolve()}", flush=True)
        print(f"Mã kiểm tra dữ liệu đầu vào: {source_fingerprint}", flush=True)
        print(f"Tệp kết quả KNN: {predictions_csv.resolve()}", flush=True)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
