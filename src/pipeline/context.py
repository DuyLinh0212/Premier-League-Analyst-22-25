from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import subprocess
from zoneinfo import ZoneInfo

from scripts.hdfs_common import HdfsClient
from src.ingest.runtime import atomic_json
from .config import Config, ROOT

LOG = logging.getLogger(__name__)


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def safe_id(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", value):
        raise ValueError("run_id chỉ gồm chữ, số, dấu gạch nối/gạch dưới, tối đa 100 ký tự")
    return value


class Context:
    def __init__(self, run_id, config=None):
        self.config = config or Config()
        self.run_id = safe_id(run_id)
        self.local = self.config.state / "runs" / self.run_id
        self.local.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.local / "manifest.json"
        if self.manifest_path.exists():
            self.manifest = json.loads(self.manifest_path.read_text())
            if self.manifest["config"] != self.config.values:
                raise RuntimeError("Cấu hình đã thay đổi; cần run_id mới để không trộn dữ liệu")
        else:
            commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
            self.manifest = {"run_id": self.run_id, "report_date": datetime.now(ZoneInfo("Asia/Bangkok")).date().isoformat(),
                             "started_at_utc": now(), "commit_sha": commit, "config": self.config.values,
                             "seasons": self.config["seasons"], "stages": {}, "tables": {}}
            self.save()
        self.hdfs = HdfsClient()
        base = self.config.hdfs_root
        self.bronze = f"{base}/bronze/snapshots/{self.run_id}"
        self.silver = f"{base}/silver/runs/{self.run_id}"
        self.normalized = self.silver + "/normalized_sources"
        self.gold = f"{base}/golde/reports/report_date={self.manifest['report_date']}/run_id={self.run_id}"
        self.gold_staging = self.gold + ".staging"
        self.schema_name = "run_" + hashlib.sha256(self.run_id.encode()).hexdigest()[:24]

    def save(self):
        atomic_json(self.manifest_path, self.manifest)

    def completed(self, stage, details=None):
        previous = self.manifest["stages"].get(stage, {})
        self.manifest["stages"][stage] = {**previous, "status": "success", "completed_at_utc": now(), **(details or {})}
        self.save()

    def require(self, *stages):
        for stage in stages:
            if self.manifest["stages"].get(stage, {}).get("status") != "success":
                raise RuntimeError(f"Bước {stage} chưa thành công")

    def session(self, name):
        from pyspark.sql import SparkSession
        for key, value in self.hdfs.environment.items():
            if key in ("HADOOP_HOME", "HADOOP_CONF_DIR", "JAVA_HOME"):
                os.environ[key] = value
        os.environ["PYSPARK_PYTHON"] = self.config.python()
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        session = (SparkSession.builder.master(self.config["spark_master"])
                   .appName("PLCA-" + name).config("spark.sql.session.timeZone", "UTC")
                   .config("spark.driver.memory", self.config["spark_driver_memory"])
                   .config("spark.sql.shuffle.partitions", "4").config("spark.sql.ansi.enabled", "false")
                   .config("spark.ui.showConsoleProgress", "false").config("spark.ui.enabled", "false")
                   .config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
                   .config("spark.hadoop.dfs.replication", "2").getOrCreate())
        session.sparkContext.setLogLevel("ERROR")
        return session

    def path(self, name, kind="facts"):
        return f"{self.silver}/tables/{kind}/{name}"

    def read(self, spark, name, kind="facts"):
        return spark.read.parquet(self.path(name, kind))

    def table(self, frame, name, kind="facts", keys=(), sources=()):
        from pyspark.sql import functions as F
        from functools import reduce
        if self.manifest["stages"].get("publish", {}).get("status") == "success":
            raise RuntimeError("Snapshot đã công bố là bất biến; cần run_id mới")
        cached = frame.persist()
        try:
            count = cached.count()
            if keys and cached.filter(reduce(lambda a, b: a | b, (F.col(key).isNull() for key in keys))).limit(1).count():
                raise RuntimeError(f"Bảng {name} có khóa NULL: {keys}")
            if keys and cached.groupBy(*keys).count().filter("count > 1").limit(1).count():
                raise RuntimeError(f"Bảng {name} trùng khóa {keys}")
            destination = self.path(name, kind)
            stage = destination + ".staging"
            writer = cached.coalesce(1).write.mode("overwrite")
            if "season" in frame.columns:
                writer = writer.partitionBy("season")
            writer.parquet(stage)
            readback = frame.sparkSession.read.parquet(stage)
            if readback.count() != count or set(readback.columns) != set(frame.columns):
                raise RuntimeError(f"Đọc lại Parquet không khớp: {name}")
            fs = frame.sparkSession._jvm.org.apache.hadoop.fs.FileSystem.get(frame.sparkSession._jsc.hadoopConfiguration())
            path = frame.sparkSession._jvm.org.apache.hadoop.fs.Path
            if fs.getUri().getScheme() != "hdfs":
                raise RuntimeError("Đích ghi phải là HDFS")
            if fs.exists(path(destination)):
                fs.delete(path(destination), True)  # Only this run's not-yet-published Silver table.
            if not fs.rename(path(stage), path(destination)):
                raise RuntimeError(f"Không công bố được bảng {name}")
            self.manifest["tables"][kind + "/" + name] = {
                "path": destination, "row_count": count, "keys": list(keys), "schema": frame.schema.jsonValue(),
                "source_tables": list(sources), "validated_parquet": True}
            self.save()
            LOG.info("Đã ghi bảng Silver %s: %d dòng.", name, count)
        finally:
            cached.unpersist()
        return frame.sparkSession.read.parquet(destination)

    def sql(self, spark, name, query):
        directory = self.local / "sql"
        directory.mkdir(exist_ok=True)
        (directory / (name + ".sql")).write_text(query, encoding="utf-8")
        return spark.sql(query)
