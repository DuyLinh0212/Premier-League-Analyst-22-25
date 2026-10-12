"""Runtime, immutable run contracts and small-output publishing helpers."""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import unicodedata
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

from .config import CLUBS, LOGIC_VERSION, SEASONS

ROOT = Path(__file__).resolve().parents[3]

def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def normalize(value):
    if value is None:
        return None
    value = unicodedata.normalize("NFKD", str(value).replace("&", " and "))
    value = "".join(c for c in value if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip() or None

_TRANSLATION = []
for _codepoint in range(192, 592):
    _character = chr(_codepoint)
    _decomposition = "".join(c for c in unicodedata.normalize("NFKD", _character) if not unicodedata.combining(c))
    if len(_decomposition) == 1 and _decomposition.isascii() and _decomposition.isalpha():
        _TRANSLATION.append((_character, _decomposition))

def normalized(column):
    """Equivalent Latin-name normalization using Spark SQL, without Python workers."""
    column = F.col(column) if isinstance(column, str) else column
    translated = F.translate(F.regexp_replace(column, "&", " and "), "".join(x for x, _ in _TRANSLATION), "".join(y for _, y in _TRANSLATION))
    value = F.trim(F.regexp_replace(F.lower(translated), "[^a-z0-9]+", " "))
    return F.when(F.length(value)>0, value)

def numeric(header, kind="double"):
    return F.regexp_replace(F.trim(F.col(f"`{header}`")), r"[, %£€$+]", "").cast(kind)

def money(header, currency):
    return F.regexp_replace(F.regexp_extract(F.col(f"`{header}`"), currency + r"\s*([0-9][0-9,]*)", 1), ",", "").cast("double")

def clean_json(value):
    if isinstance(value, dict):
        return {str(k): clean_json(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [clean_json(v) for v in value]
    if isinstance(value, Decimal):
        # Spark SQL DECIMAL metrics must remain numeric in JSON/JSONB, not
        # strings produced by json.dumps(default=str).
        return clean_json(float(value))
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "asDict"):
        return clean_json(value.asDict(recursive=True))
    return value

def json_write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(clean_json(value), ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    temporary.replace(path)

class Context:
    def __init__(self, run_id, hdfs_uri=None, base=None):
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", run_id):
            raise ValueError("run_id must contain only letters, numbers, hyphens and underscores")
        self.run_id = run_id
        self.uri = (hdfs_uri or os.getenv("HDFS_URI", "hdfs://localhost:9000")).rstrip("/")
        self.base = (base or os.getenv("HDFS_BASE_PATH", "/premier-league")).rstrip("/")
        self.local = ROOT / "pipeline_runs" / run_id
        self.local.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.local / "manifest.json"
        self.manifest = json.loads(self.manifest_path.read_text(encoding="utf-8")) if self.manifest_path.exists() else {
            "analysis_run_id": run_id, "silver_run_id": run_id, "started_at_utc": now(),
            "logic_version": LOGIC_VERSION, "seasons": list(SEASONS), "stages": {}, "tables": {},
        }
        self.silver = f"{self.uri}{self.base}/silver"
        self.gold = f"{self.uri}{self.base}/gold"

    @property
    def raw(self):
        return self.manifest["raw_snapshot_path"]

    def save(self):
        json_write(self.manifest_path, self.manifest)

    def hdfs(self, *args, check=True):
        executable = os.getenv("HDFS_BIN", "hdfs")
        return subprocess.run([executable, "dfs", "-Ddfs.blocksize=67108864", "-Ddfs.replication=2", *map(str, args)], text=True, capture_output=True, check=check)

    def session(self, name):
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        required_paths = [ROOT / "apps/api", ROOT / "apps/api/football", ROOT / "ml/inference", ROOT / "processing/spark"]
        os.environ["PYTHONPATH"] = os.pathsep.join(map(str, required_paths)) + os.pathsep + os.environ.get("PYTHONPATH", "")
        spark = (SparkSession.builder.appName("PLCA_" + name)
                 .master(os.getenv("SPARK_MASTER", "local[2]"))
                 .config("spark.hadoop.fs.defaultFS", self.uri)
                 .config("spark.hadoop.dfs.blocksize", "67108864").config("spark.hadoop.parquet.block.size", "67108864")
                 .config("spark.hadoop.dfs.client.use.datanode.hostname", "true").config("spark.hadoop.dfs.replication", "2")
                 .config("spark.sql.shuffle.partitions", "4")
                 .config("spark.driver.memory", "512m")
                 .config("spark.sql.parquet.compression.codec", "snappy")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.ansi.enabled", "false")
                 .config("spark.ui.enabled", "false")
                 .config("spark.ui.showConsoleProgress", "false")
                 .config("spark.sql.adaptive.enabled", "true").getOrCreate())
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def read(self, spark, name, kind="facts"):
        return spark.read.parquet(self.silver_table_path(name, kind))

    def silver_table_path(self, name, kind="facts"):
        if name == "dim_club":
            group = "teams"
        elif name.startswith("fact_team_season") or name == "fact_squad_wage":
            group = "team_season_stats"
        elif name.startswith("fact_player_"):
            group = "player_season_stats"
        elif name.startswith("fact_transfer"):
            group = "transfers"
        elif name.startswith("fact_team_match") or name.startswith("fact_match") or name in {"fact_goal_event", "goal_against_mirror"}:
            group = "matches"
        elif name in {"dim_player", "dim_transfermarkt_player", "bridge_fbref_player", "bridge_transfermarkt_player"}:
            group = "players"
        else:
            group = "players" if kind in {"dimensions", "bridges"} else "matches"
        return f"{self.silver}/{group}/{name}/runs/{self.run_id}"

    def gold_read(self, spark, name):
        for group in ("player_performance", "season_comparison", "league_standings", "tactical_insights", "prediction_features"):
            path = f"{self.gold}/{group}/{name}/runs/{self.run_id}"
            if self.hdfs("-test", "-e", path, check=False).returncode == 0:
                return spark.read.parquet(path)
        raise FileNotFoundError("Gold table not found for this run: " + name)

    def table(self, df, name, kind="facts", keys=(), sources=()):
        cached = df.cache()
        count = cached.count()
        if keys:
            duplicates = df.groupBy(*keys).count().where("count > 1").limit(1).count()
            if duplicates:
                raise RuntimeError(f"Duplicate key in {name}: {keys}")
        target = self.silver_table_path(name, kind)
        staging = target + "_staging"
        jvm = df.sparkSession._jvm
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(self.uri), df.sparkSession._jsc.hadoopConfiguration())
        path = jvm.org.apache.hadoop.fs.Path
        fs.delete(path(staging), True)
        writer = df.coalesce(1).write.mode("overwrite")
        if "season" in df.columns:
            writer = writer.partitionBy("season")
        writer.parquet(staging)
        persisted = df.sparkSession.read.parquet(staging)
        if persisted.count() != count or set(persisted.columns) != set(df.columns):
            raise RuntimeError(f"Parquet read-back failed: {name}")
        # Only this run's table is replaceable on retry; previous runs stay intact.
        if fs.exists(path(target)):
            fs.delete(path(target), True)
        if not fs.rename(path(staging), path(target)):
            raise RuntimeError(f"HDFS staging commit failed: {name}")
        schema = df.schema.jsonValue()
        if kind in {"audit", "bridges", "dimensions"}:
            self.export_rows(df, name)
        self.manifest["tables"][kind + "/" + name] = {
            "path": target, "row_count": count, "keys": list(keys), "schema": schema,
            "source_tables": list(sources), "validated_parquet": True,
        }
        self.save()
        cached.unpersist()
        print(f"TABLE {name}: {count} rows -> {target}", flush=True)
        return df.sparkSession.read.parquet(target)

    def export_rows(self, df, name):
        out = self.local / "tables" / name
        out.mkdir(parents=True, exist_ok=True)
        # Published marts are small; raw appearances/valuations stay distributed.
        with (out / "data.jsonl").open("w", encoding="utf-8") as j, (out / "data.csv").open("w", encoding="utf-8-sig", newline="") as c:
            writer = csv.DictWriter(c, fieldnames=df.columns)
            writer.writeheader()
            for row in df.toLocalIterator():
                item = clean_json(row.asDict(recursive=True))
                j.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")
                writer.writerow({k: json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict)) else v for k, v in item.items()})

    def completed(self, stage, details=None):
        self.manifest["stages"][stage] = {"status": "success", "completed_at_utc": now(), **(details or {})}
        self.save()

    def require(self, *stages):
        for stage in stages:
            if self.manifest["stages"].get(stage, {}).get("status") != "success":
                raise RuntimeError(f"Stage {stage} must succeed before continuing")

    def sql(self, spark, name, query):
        path = self.local / "sql" / (name + ".sql")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(query.strip() + "\n", encoding="utf-8")
        return spark.sql(query)

def club_views(spark):
    aliases = [(normalize(alias), cid) for cid, _, _, names in CLUBS for alias in names]
    aliases += [(normalize(name), cid) for cid, name, _, _ in CLUBS]
    spark.createDataFrame(list(set(aliases)), "club_name_key string, club_id string").createOrReplaceTempView("club_aliases")
    spark.createDataFrame([(cid, name, tm) for cid, name, tm, _ in CLUBS],
                          "club_id string, club_name string, tm_club_id string").createOrReplaceTempView("tracked_clubs")

def raw_fbref(ctx, spark, table, section, player=False, match=False):
    glob = f"{ctx.raw}/fbref/season=*/{section}/" + ("*/" if section == "04_teams" else "") + table + ".csv"
    df = spark.read.option("header", True).option("encoding", "UTF-8").option("mode", "FAILFAST").csv(glob)
    df = (df.withColumn("source_file", F.input_file_name())
          .withColumn("season_key", F.regexp_extract("source_file", r"/fbref/season=(\d{4})-(\d{2})/", 1))
          .withColumn("season_suffix", F.regexp_extract("source_file", r"/fbref/season=(\d{4})-(\d{2})/", 2))
          .withColumn("season", F.concat(F.col("season_key"), F.lit("-20"), F.col("season_suffix")))
          .filter(F.col("season").isin(*SEASONS)))
    if player:
        df = (df.filter(F.col("Player").isNotNull() & (F.col("Player") != "Player"))
              .withColumn("player_name", F.col("Player"))
              .withColumn("name_key", normalized("Player"))
              .withColumn("club_name_key", normalized("Squad")))
    elif match:
        df = (df.withColumn("match_date", F.to_date("Date"))
              .filter(F.col("match_date").isNotNull())
              .withColumn("club_name_key", normalized(F.regexp_extract("source_file", r"/04_teams/([^/]+)/", 1))))
    else:
        df = df.filter(F.col("Squad").isNotNull() & (F.col("Squad") != "Squad"))
        df = df.withColumn("club_name_key", normalized("Squad"))
    return df.join(spark.table("club_aliases"), "club_name_key", "left")

def raw_tm(ctx, spark, name):
    return spark.read.option("header", True).option("encoding", "UTF-8").option("mode", "FAILFAST").csv(f"{ctx.raw}/transfermarkt/_source/{name}.csv")

def metric_columns(df, mapping, prefix=""):
    return [numeric(header, kind).alias(prefix + target) if header in df.columns
            else F.lit(None).cast(kind).alias(prefix + target)
            for target, (header, kind) in mapping.items()]
