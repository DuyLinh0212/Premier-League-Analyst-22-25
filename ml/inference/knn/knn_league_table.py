#!/usr/bin/env python3
"""KNN rank and points estimates for historical and Kafka-ingested tables.

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
    "goals_for_per_match",
    "goals_against_per_match",
    "wins_per_match",
)
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
    description="Spark SQL + StandardScaler + KNN for league tables"
    )
    parser.add_argument("--input-path", default=DEFAULT_INPUT)
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
            print(f"No committed season partitions found under {path}; skipping optional input.")
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
    features = spark.sql(query).cache()
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
) -> DataFrame:
    if neighbor_count < 1:
        raise ValueError("--neighbors must be at least 1")
    query = scaled.filter(F.col("season") == query_season).select(
        F.col("season").alias("query_season"),
        F.col("club_id").alias("query_club_id"),
        F.col("club_name").alias("query_club_name"),
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
            "output_contract": "predictions-only-v1",
        }
        outputs_exist = predictions_csv.exists()
        if not args.force and checkpoint and checkpoint.get("config") == checkpoint_key and outputs_exist:
            print("HDFS input has not changed; KNN outputs are already up to date.")
            print(f"Last run: {checkpoint.get('run_at_utc', 'unknown')}")
            return 0

        scaled = fit_scaled(features).cache()
        predictions = run_knn(scaled, query_season, args.neighbors)
        prediction_columns = [
            "query_season",
            "query_club_id",
            "query_club_name",
            "predicted_rank",
            "predicted_points",
            "top4_probability",
        ]
        prediction_records = records_from_df(predictions, prediction_columns, ["query_club_id"])
        write_csv(prediction_records, predictions_csv)

        now = datetime.now(timezone.utc).isoformat()
        history_entry = {
            "run_at_utc": now,
            "config": checkpoint_key,
            "rows": len(prediction_records),
            "outputs": [str(predictions_csv.resolve())],
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
        print(f"Query teams: {len(prediction_records)}")
        print(f"Input fingerprint: {source_fingerprint}")
        print(f"Predictions CSV: {predictions_csv.resolve()}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
