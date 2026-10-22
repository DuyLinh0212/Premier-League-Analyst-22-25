"""Retryable commands invoked by the real Airflow scheduler, one stage per process."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import hashlib
import json
import logging
import os
import shutil
from pathlib import Path
import subprocess
import sys
import time

from src.ingest.runtime import atomic_json, configure_logging, process_lock
from .config import ROOT, Config
from .context import Context, now

LOG = logging.getLogger(__name__)


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def preflight(ctx):
    ctx.hdfs.require_ready(min_datanodes=3)
    from .database import connect, initialize
    with connect(ctx.config) as db:
        db.execute("SELECT 1")
    initialize(ctx.config)
    files = [p for p in ctx.config.source.rglob("*") if p.is_file()]
    if len(files) != ctx.config["expected_source_files"]:
        raise RuntimeError(f"Cần {ctx.config['expected_source_files']} file nguồn; hiện có {len(files)}")
    raw_bytes = sum(path.stat().st_size for path in files)
    free_bytes = shutil.disk_usage(ctx.config.state).free
    if free_bytes < max(2*1024**3, raw_bytes*4):
        raise RuntimeError("Filesystem Linux không đủ dung lượng cho báo cáo và log")
    disk = ctx.hdfs.hdfs("dfs", "-df", ctx.config.hdfs_root, capture=True)
    available = int(disk.strip().splitlines()[-1].split()[3])
    if available < raw_bytes*6:
        raise RuntimeError("HDFS không đủ dung lượng cho Bronze, Silver và replication")
    ctx.completed("preflight", {"source_files": len(files), "source_bytes": raw_bytes,
                               "linux_free_bytes": free_bytes, "hdfs_available_bytes": available})


def register_raw_snapshot(ctx):
    ctx.hdfs.require_ready(min_datanodes=3)
    sources = sorted(p for p in ctx.config.source.rglob("*") if p.is_file())
    entries = []
    for source in sources:
        if source.is_symlink() or any(p.is_symlink() for p in source.parents):
            raise ValueError("Không chấp nhận symlink trong dữ liệu nguồn")
        entries.append({"path": source.relative_to(ctx.config.source).as_posix(), "size": source.stat().st_size,
                        "sha256": digest(source)})
    if len(entries) != ctx.config["expected_source_files"]:
        raise RuntimeError("Danh sách file nguồn không khớp cấu hình")
    source_manifest = ctx.local / "source_manifest.json"
    if source_manifest.exists() and json.loads(source_manifest.read_text()) != entries:
        raise RuntimeError("Nguồn đã thay đổi; tạo run_id mới để giữ snapshot bất biến")
    atomic_json(source_manifest, entries)
    client = ctx.hdfs
    def exists(path):
        return subprocess.run([client.binary, "dfs", "-test", "-e", path], env=client.environment,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    if not exists(ctx.bronze):
        staging = ctx.bronze + ".staging"
        if exists(staging):
            client.hdfs("dfs", "-rm", "-r", staging)
        client.hdfs("dfs", "-mkdir", "-p", staging)
        LOG.info("Đang nạp %d file thô lên Bronze: %s", len(entries), ctx.bronze)
        client.hdfs("dfs", "-put", *map(str, sorted(ctx.config.source.iterdir())), staging)
        # Detect changes during upload before any snapshot is published.
        if any(source.stat().st_size != entry["size"] or digest(source) != entry["sha256"]
               for source, entry in zip(sources, entries, strict=True)):
            raise RuntimeError("Nguồn thay đổi trong khi upload; không công bố Bronze")
        client.hdfs("dfs", "-mv", staging, ctx.bronze)
    spark = ctx.session("verify-bronze")
    try:
        # Hash HDFS streams in bounded buffers, including the large appearances file.
        # binaryFile materializes whole files and can exhaust a local Spark executor.
        jvm = spark._jvm
        root = jvm.org.apache.hadoop.fs.Path(ctx.bronze)
        fs = root.getFileSystem(spark._jsc.hadoopConfiguration())
        iterator = fs.listFiles(root, True)
        observed = {}
        while iterator.hasNext():
            status = iterator.next()
            path = status.getPath()
            stream = fs.open(path)
            try:
                checksum = jvm.org.apache.commons.codec.digest.DigestUtils.sha256Hex(stream)
            finally:
                stream.close()
            observed[str(path).split(ctx.bronze + "/", 1)[1]] = (status.getLen(), checksum)
        expected = {entry["path"]: (entry["size"], entry["sha256"]) for entry in entries}
        if observed != expected:
            raise RuntimeError("Checksum Bronze trên HDFS không khớp nguồn")
    finally:
        spark.stop()
    ctx.completed("register_raw_snapshot", {"files": len(entries), "checksum_verified": True, "bronze": ctx.bronze})


def normalize(ctx):
    ctx.require("register_raw_snapshot")
    from src.normalize.normalize import run_normalization
    spark = ctx.session("normalize")
    try:
        path = spark._jvm.org.apache.hadoop.fs.Path(ctx.normalized + "/_metadata/normalization_manifest")
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        if not fs.exists(path):
            result = run_normalization(argparse.Namespace(bronze=ctx.bronze, silver=ctx.normalized,
                                                         max_files=None, dry_run=False, overwrite=False), spark)
        else:
            result = {"resumed": True}
        from .curated.core import source_manifest
        items = source_manifest(ctx, spark)
        if len(items) != ctx.config["expected_source_files"]:
            raise RuntimeError("Thiếu file Parquet sau chuẩn hóa")
        if any(not i.get("column_mapping") for i in items if i["source"].endswith(".csv")):
            raise RuntimeError("Thiếu bản đồ tên cột; snapshot chuẩn hóa không tương thích")
        ctx.completed("normalize", {**result, "file_count": len(items)})
    finally:
        spark.stop()


def build_identity(ctx):
    ctx.require("normalize")
    from .curated.identity import build_identity as build
    build(ctx)


def build_silver(ctx):
    from .curated.silver import build_silver as build
    build(ctx)
    # Additional lookup required for all matchday/points analyses.
    from .curated.core import raw_tm
    from pyspark.sql import functions as F
    spark = ctx.session("games-lookup")
    try:
        games = raw_tm(ctx, spark, "games").filter("competition_id='GB1' AND season IN ('2022','2023','2024')")
        games = games.select("game_id", "competition_id", "season", "round")
        ctx.table(games, "tm_games", "lookups", keys=["game_id"], sources=["games"])
    finally:
        spark.stop()
    ctx.completed("build_silver", {"table_count": len(ctx.manifest["tables"])})


def validate_silver(ctx):
    from .curated.silver import validate_silver as validate
    validate(ctx)
    if len(ctx.manifest["tables"]) != 22:
        raise RuntimeError("Cần 21 bảng Silver và tm_games")
    from pyspark.sql import functions as F
    from functools import reduce
    spark = ctx.session("validate-all-tables")
    quality = []
    try:
        for key, entry in ctx.manifest["tables"].items():
            frame = spark.read.parquet(entry["path"])
            null_keys = frame.filter(reduce(lambda a, b: a | b, (F.col(k).isNull() for k in entry["keys"]))).count() if entry["keys"] else 0
            duplicates = frame.groupBy(*entry["keys"]).count().filter("count>1").count() if entry["keys"] else 0
            row_count = frame.count()
            passed = null_keys == 0 and duplicates == 0 and row_count == entry["row_count"]
            quality.append({"table": key, "rows": row_count, "null_keys": null_keys, "duplicate_keys": duplicates,
                            "schema": frame.schema.jsonValue(), "passed": passed})
        if any(not item["passed"] for item in quality):
            raise RuntimeError("Khóa hoặc số dòng không hợp lệ ở một bảng Silver")
        atomic_json(ctx.local / "silver_tables_quality.json", quality)
    finally:
        spark.stop()


def register_views(ctx, spark):
    for key, entry in ctx.manifest["tables"].items():
        spark.read.parquet(entry["path"]).createOrReplaceTempView(key.split("/")[-1])
    spark.sql("SELECT club_id,min(club_name) AS club_name FROM dim_club GROUP BY club_id").createOrReplaceTempView("club_names")


def api_imports():
    api = str(ROOT / "apps/analytics-web/api")
    if api not in sys.path:
        sys.path.insert(0, api)


def clean(value):
    from decimal import Decimal
    import math
    if isinstance(value, dict):
        return {key: clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value) if value.is_finite() else None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def analyze(ctx):
    ctx.require("validate_silver")
    api_imports()
    from app.queries import (age_distribution, attack_defense, head_to_head, matchday_performance,
                             player_contribution, player_radar, points_progression, spending_vs_achievement,
                             squad_value, team_strength)
    spark = ctx.session("ten-analyses")
    directory = ctx.local / "analysis"
    directory.mkdir(exist_ok=True)
    index = []
    def save(slug, scope, params, value):
        identity = slug + "__" + scope
        filename = "analysis/" + identity + ".json"
        value = clean(value)
        value["meta"] = {**value.get("meta", {}), "run_id": ctx.run_id, "source": "spark-hdfs", "parameters": params}
        atomic_json(ctx.local / filename, value)
        index.append({"id": identity, "slug": slug, "scope": scope, "params": params, "file": filename})
        rows = value["data"]
        # Explicit envelope is valid even when a cohort is empty or every metric is NULL.
        frame = spark.createDataFrame([(identity, slug, json.dumps(params, ensure_ascii=False), json.dumps(value, ensure_ascii=False))],
                                      "analysis_id string, analysis string, parameters_json string, payload_json string")
        frame.coalesce(1).write.mode("overwrite").parquet(ctx.gold_staging + "/data/" + identity)
        if rows:
            from pyspark.sql import functions as F
            table = spark.read.json(spark.sparkContext.parallelize([json.dumps(row, ensure_ascii=False) for row in rows], 1))
            table = table.withColumn("run_id", F.lit(ctx.run_id)).withColumn("analysis_id", F.lit(identity))
            table.coalesce(1).write.mode("overwrite").parquet(ctx.gold_staging + "/tables/" + identity)
        LOG.info("Đã phân tích %s, phạm vi %s: %d dòng.", slug, scope, len(rows))
    try:
        register_views(ctx, spark)
        from app.query_utils import collect, sql_string
        for season in ctx.config["seasons"]:
            params = {"season": season}
            for slug, module in [("age-distribution", age_distribution), ("attack-defense", attack_defense),
                                 ("head-to-head-matrix", head_to_head), ("spending-vs-achievement", spending_vs_achievement)]:
                save(slug, season, params, module.run(spark, season))
            save("squad-value", season, params, squad_value.run(spark, season))
            save("team-strength", season, {**params, "clubs": []}, team_strength.run(spark, season, []))
            teams = collect(spark.sql(f"SELECT club_id FROM fact_team_season WHERE season={sql_string(season)} ORDER BY club_id"))
            for team in teams:
                club = team["club_id"]
                for slug, module in [("player-contribution", player_contribution), ("matchday-performance", matchday_performance)]:
                    save(slug, season + "_" + club, {**params, "club_id": club}, module.run(spark, season, club))
            for position in ("DF", "FW", "GK", "MF"):
                rows = collect(spark.sql(f"""SELECT concat(player_id,'|',club_id) AS selection_id FROM fact_player_season
                    WHERE season={sql_string(season)} AND position={sql_string(position)}
                    AND coalesce(minutes,playing_minutes,gk_minutes,0)>={ctx.config['minimum_minutes']}
                    ORDER BY player_id,club_id"""))
                players = [r["selection_id"] for r in rows]
                save("player-radar", season + "_" + position, {**params, "position": position,
                     "players": players, "min_minutes": ctx.config["minimum_minutes"]},
                     player_radar.run(spark, season, position, players, ctx.config["minimum_minutes"]))
        clubs = collect(spark.sql("SELECT DISTINCT club_id FROM fact_team_season ORDER BY club_id"))
        for club in clubs:
            params = {"seasons": ctx.config["seasons"], "club_id": club["club_id"]}
            save("points-progression", "all_" + club["club_id"], params,
                 points_progression.run(spark, params["seasons"], params["club_id"]))
            save("head-to-head", "all_" + club["club_id"], params,
                 head_to_head.run_club_comparison(spark, params["seasons"], params["club_id"]))
        atomic_json(directory / "index.json", index)
        ctx.completed("analyze", {"results": len(index), "analysis_types": 10})
    finally:
        spark.stop()


def export_reports(ctx):
    ctx.require("analyze")
    from .reports import export
    export(ctx)
    ctx.completed("export_reports")


def archive_results(ctx):
    ctx.require("export_reports")
    ctx.hdfs.hdfs("dfs", "-mkdir", "-p", ctx.gold_staging + "/artifacts")
    for name in ("analysis", "reports", "excel", "diagrams", "tables"):
        ctx.hdfs.hdfs("dfs", "-put", "-f", str(ctx.local / name), ctx.gold_staging + "/artifacts")
    ctx.hdfs.hdfs("dfs", "-put", "-f", str(ctx.manifest_path), str(ctx.local / "source_manifest.json"), ctx.gold_staging + "/artifacts")
    ctx.completed("archive_results", {"gold": ctx.gold})


def load_postgres(ctx):
    ctx.require("archive_results")
    from .database import load_snapshot
    spark = ctx.session("load-postgresql")
    try:
        load_snapshot(ctx, spark)
    finally:
        spark.stop()
    ctx.completed("load_postgres")


def comparable(value, context=""):
    if isinstance(value, dict):
        ignored = {"run_id", "source", "parameters", "dataset_id"} if context == "meta" else set()
        if context == "insight":
            # Narrative examples may select different members of an exactly tied group.
            # All data, cohort counts, aggregate metrics and calculation methods are checked.
            ignored.add("finding")
        return {key.lower(): comparable(v, key) for key, v in value.items() if key not in ignored}
    if isinstance(value, list):
        # Unordered ties in Spark GROUP BY are not business ordering guarantees.
        return sorted((comparable(v, context) for v in value), key=lambda v: json.dumps(v, sort_keys=True, default=str))
    return clean(value)


def assert_equivalent(left, right, path="root"):
    import math
    if isinstance(left, dict) and isinstance(right, dict):
        if left.keys() != right.keys():
            raise AssertionError(f"{path}: khác danh sách cột")
        for key in left:
            assert_equivalent(left[key], right[key], path + "." + key)
    elif isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            raise AssertionError(f"{path}: khác số dòng")
        for i, (a, b) in enumerate(zip(left, right, strict=True)):
            assert_equivalent(a, b, f"{path}[{i}]")
    elif isinstance(left, (int, float)) and isinstance(right, (int, float)):
        if not math.isclose(left, right, rel_tol=1e-7, abs_tol=1e-7):
            raise AssertionError(f"{path}: {left} != {right}")
    elif left != right:
        raise AssertionError(f"{path}: {left!r} != {right!r}")


def validate_postgres(ctx):
    ctx.require("load_postgres")
    api_imports()
    from importlib import import_module
    from .database import PostgresRuntime
    engine = PostgresRuntime(ctx.config)
    index = json.loads((ctx.local / "analysis/index.json").read_text())
    with engine.request(ctx.run_id, allow_staged=True):
        for entry in index:
            module = import_module("app.queries." + entry["slug"].replace("-", "_").replace("_matrix", ""))
            function = module.run_club_comparison if entry["slug"] == "head-to-head" else module.run
            actual = function(engine, **entry["params"])
            expected = json.loads((ctx.local / entry["file"]).read_text())
            assert_equivalent(comparable(expected), comparable(actual), entry["id"])
            LOG.info("Đã đối chiếu Spark/HDFS ↔ PostgreSQL: %s.", entry["id"])
    atomic_json(ctx.local / "postgres_quality.json", {"passed": True, "results_compared": len(index), "run_id": ctx.run_id})
    ctx.completed("validate_postgres", {"results_compared": len(index)})


def publish(ctx):
    ctx.require("validate_postgres", "archive_results")
    from .database import publish as commit
    def exists(path):
        return subprocess.run([ctx.hdfs.binary, "dfs", "-test", "-e", path], env=ctx.hdfs.environment,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    if not exists(ctx.gold):
        if not exists(ctx.gold_staging):
            raise RuntimeError("Chưa có snapshot Gold staging hoàn chỉnh")
        ctx.hdfs.hdfs("dfs", "-touchz", ctx.gold_staging + "/_SUCCESS")
        ctx.hdfs.hdfs("dfs", "-mv", ctx.gold_staging, ctx.gold)
    elif not exists(ctx.gold + "/_SUCCESS"):
        raise RuntimeError("Đường dẫn Gold đã tồn tại nhưng chưa hoàn chỉnh; không ghi đè")
    commit(ctx)
    ctx.completed("publish", {"published_at_utc": now()})
    ctx.hdfs.hdfs("dfs", "-put", "-f", str(ctx.manifest_path), ctx.gold + "/artifacts")


STAGES = ("preflight", "register_raw_snapshot", "normalize", "build_identity", "build_silver", "validate_silver",
          "analyze", "export_reports", "archive_results", "load_postgres", "validate_postgres", "publish")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=STAGES)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    configure_logging()
    ctx = Context(args.run_id)
    log_dir = ctx.local / "logs"
    log_dir.mkdir(exist_ok=True)
    handler = logging.FileHandler(log_dir / (args.stage + ".log"), encoding="utf-8")
    from src.ingest.runtime import VietnameseFormatter
    handler.setFormatter(VietnameseFormatter())
    logging.getLogger().addHandler(handler)
    with process_lock(ctx.config.state / "stage.lock"):
        if ctx.manifest["stages"].get(args.stage, {}).get("status") == "success":
            LOG.info("Bước %s đã thành công cho %s; không xử lý trùng.", args.stage, args.run_id)
            return
        ctx.manifest["stages"][args.stage] = {"status": "running", "started_at_utc": now()}
        ctx.save()
        started = time.monotonic()
        try:
            LOG.info("Đang thực hiện bước %s; lần chạy %s...", args.stage, args.run_id)
            globals()[args.stage](ctx)
            ctx.manifest["stages"][args.stage]["duration_seconds"] = round(time.monotonic() - started, 2)
            ctx.save()
            LOG.info("Hoàn tất bước %s.", args.stage)
        except Exception:
            ctx.manifest["stages"][args.stage].update(status="failed", failed_at_utc=now())
            ctx.save()
            LOG.exception("Bước %s thất bại; giữ nguyên snapshot đang phục vụ.", args.stage)
            raise


if __name__ == "__main__":
    main()
