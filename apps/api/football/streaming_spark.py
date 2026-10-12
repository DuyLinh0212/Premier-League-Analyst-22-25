"""Kafka Structured Streaming job: commit full league tables to project HDFS."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
import glob

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from league_table import parse_and_validate_csv, validate_season


CLUB_IDS = {
    "Arsenal": "ARS", "Aston Villa": "AVL", "Bournemouth": "BOU",
    "Brentford": "BRE", "Brighton": "BHA", "Chelsea": "CHE",
    "Crystal Palace": "CRY", "Everton": "EVE", "Fulham": "FUL",
    "Ipswich Town": "IPS", "Leicester City": "LEI", "Liverpool": "LIV",
    "Manchester City": "MCI", "Manchester Utd": "MUN", "Newcastle": "NEW",
    "Nottingham": "NFO", "Southampton": "SOU", "Tottenham": "TOT",
    "West Ham": "WHU", "Wolves": "WOL",
}

FACT_SCHEMA = StructType([
    StructField("club_id", StringType(), False),
    StructField("club_name", StringType(), False),
    StructField("rank", IntegerType(), False),
    StructField("matches_played", IntegerType(), False),
    StructField("wins", IntegerType(), False),
    StructField("draws", IntegerType(), False),
    StructField("losses", IntegerType(), False),
    StructField("goals_for", IntegerType(), False),
    StructField("goals_against", IntegerType(), False),
    StructField("goal_diff", IntegerType(), False),
    StructField("points", IntegerType(), False),
    StructField("points_per_mp", DoubleType(), False),
])


def fact_rows(csv_rows: list[dict[str, str]]) -> list[tuple]:
    if set(CLUB_IDS) != {row["Squad"] for row in csv_rows}:
        raise ValueError("CSV club list differs from the configured 2024-2025 baseline")
    return [(
        CLUB_IDS[row["Squad"]], row["Squad"], int(row["Rk"]), int(row["MP"]),
        int(row["W"]), int(row["D"]), int(row["L"]), int(row["GF"]),
        int(row["GA"]), int(row["GD"]), int(row["Pts"]), float(row["Pts/MP"]),
    ) for row in csv_rows]


class HdfsSink:
    def __init__(self, spark: SparkSession, uri: str, base_path: str):
        self.spark = spark
        self.uri = uri.rstrip("/")
        normalized_base = "/" + base_path.strip("/")
        if normalized_base == "/":
            raise ValueError("HDFS base path must not be empty or root")
        self.base_path = normalized_base
        self.jvm = spark.sparkContext._jvm
        self.Path = self.jvm.org.apache.hadoop.fs.Path
        self.fs = self.Path(self.uri + "/").getFileSystem(
            spark.sparkContext._jsc.hadoopConfiguration()
        )
        self.check_available()

    def path(self, relative: str):
        return self.Path(f"{self.uri}{relative}")

    def check_available(self) -> None:
        # getFileStatus raises when the NameNode is unavailable. Safe mode still
        # permits reads, so check it separately before accepting a Kafka batch.
        action = self.jvm.org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction.SAFEMODE_GET
        if self.fs.setSafeMode(action):
            raise RuntimeError("HDFS NameNode is in safe mode; Kafka offsets remain uncommitted")
        base = self.path(self.base_path)
        if not self.fs.exists(base) and not self.fs.mkdirs(base):
            raise RuntimeError(f"Could not create HDFS project root: {base}")
        self.fs.getFileStatus(base)

    def _read_bytes(self, path) -> bytes:
        source = self.fs.open(path)
        output = self.jvm.java.io.ByteArrayOutputStream()
        try:
            self.jvm.org.apache.hadoop.io.IOUtils.copyBytes(source, output, 4096, False)
            return bytes(bytearray(output.toByteArray()))
        finally:
            source.close()
            output.close()

    def _stage_bytes(self, path, data: bytes) -> None:
        output = self.fs.create(path, False)
        try:
            output.write(bytearray(data))
            output.hflush()
        finally:
            output.close()

    def _check_existing_parquet(self, final, marker, expected: list[tuple], checksum: str) -> bool:
        if not self.fs.exists(final):
            return False
        if self.fs.exists(marker):
            if self._read_bytes(marker).decode("ascii").strip() != checksum:
                raise ValueError(f"Parquet already exists with different CSV checksum: {final}")
            return True
        # Recover an interrupted commit that reached Parquet before its marker.
        actual_df = self.spark.read.parquet(final.toString())
        expected_fields = [(field.name, field.dataType.simpleString()) for field in FACT_SCHEMA.fields]
        actual_fields = [(field.name, field.dataType.simpleString()) for field in actual_df.schema.fields]
        if actual_fields != expected_fields or sorted(tuple(row) for row in actual_df.collect()) != sorted(expected):
            raise ValueError(f"Parquet already exists with different data: {final}")
        return True

    def write_season(self, event: dict) -> None:
        self.check_available()
        season = event["season"]
        validate_season(season)
        csv_text = event["csv"]
        csv_bytes = csv_text.encode("utf-8")
        checksum = hashlib.sha256(csv_bytes).hexdigest()
        if event["sha256"] != checksum:
            raise ValueError(f"Checksum mismatch for season {season}")
        expected = fact_rows(parse_and_validate_csv(csv_text))

        kafka_root = self.base_path + "/kafka"
        raw_parent = self.path(f"{kafka_root}/raw/fbref/{season}/01_league")
        raw_final = self.path(f"{kafka_root}/raw/fbref/{season}/01_league/league_table.csv")
        parquet_root = self.path(f"{kafka_root}/processed/facts/league_table")
        parquet_final = self.path(f"{kafka_root}/processed/facts/league_table/season={season}")
        marker = self.path(f"{kafka_root}/processed/facts/league_table/season={season}/_source_sha256")

        raw_exists = self.fs.exists(raw_final)
        if raw_exists and self._read_bytes(raw_final) != csv_bytes:
            raise ValueError(f"CSV already exists with different content: {raw_final}")
        parquet_exists = self._check_existing_parquet(parquet_final, marker, expected, checksum)
        if raw_exists and parquet_exists:
            print(f"Already committed {season}; skipping duplicate event", flush=True)
            return

        token = uuid.uuid4().hex
        raw_stage = self.path(f"{kafka_root}/raw/fbref/{season}/01_league/.league_table.{token}.tmp")
        parquet_stage = self.path(f"{kafka_root}/processed/facts/_league_table_staging/{season}-{token}")
        try:
            if not raw_exists:
                self.fs.mkdirs(raw_parent)
                self._stage_bytes(raw_stage, csv_bytes)
            if not parquet_exists:
                frame = self.spark.createDataFrame(expected, FACT_SCHEMA).withColumn("season", lit(season))
                frame.coalesce(1).write.mode("errorifexists").partitionBy("season").parquet(
                    parquet_stage.toString()
                )
                staged_partition = self.Path(parquet_stage, f"season={season}")
                self._stage_bytes(self.Path(staged_partition, "_source_sha256"), checksum.encode("ascii"))

            # HDFS has no transaction spanning two paths. Both writes are staged;
            # an interruption between renames is repaired when Kafka replays this batch.
            self.check_available()
            if not raw_exists:
                if not self.fs.rename(raw_stage, raw_final):
                    raise RuntimeError(f"Could not commit raw CSV for {season}")
            if not parquet_exists:
                self.fs.mkdirs(parquet_root)
                if not self.fs.rename(staged_partition, parquet_final):
                    raise RuntimeError(f"Could not commit Parquet for {season}")
            print(f"Committed {season} to raw CSV and Parquet", flush=True)
        finally:
            # Staging is disposable. Keep the original failure if HDFS is down.
            for candidate in (raw_stage, parquet_stage):
                try:
                    if self.fs.exists(candidate):
                        self.fs.delete(candidate, True)
                except Exception:
                    pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-server", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "league_table_simulated"))
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--hdfs-uri", default=os.getenv("HDFS_URI", "hdfs://localhost:9000"))
    parser.add_argument("--hdfs-base-path", default=os.getenv("HDFS_BASE_PATH", "/premier-league"))
    parser.add_argument(
        "--checkpoint-location",
        default=os.getenv("SPARK_CHECKPOINT"),
    )
    args = parser.parse_args()
    if not args.checkpoint_location:
        args.checkpoint_location = (
            args.hdfs_uri.rstrip("/")
            + "/"
            + args.hdfs_base_path.strip("/")
            + "/kafka/checkpoints/league_table_stream"
        )
    spark = (
        SparkSession.builder.appName("SimulatedLeagueTableStream")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", "512m")
        .config("spark.hadoop.fs.defaultFS", args.hdfs_uri)
        .config("spark.hadoop.dfs.blocksize", "67108864").config("spark.hadoop.parquet.block.size", "67108864")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true").config("spark.hadoop.dfs.replication", "2")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars", ",".join(glob.glob(os.getenv("SPARK_EXTRA_JARS", "/opt/kafka-jars/*.jar"))))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    sink = HdfsSink(spark, args.hdfs_uri, args.hdfs_base_path)

    def process_batch(frame, batch_id: int) -> None:
        sink.check_available()
        # A season is one small Kafka record. The trigger cap bounds driver memory.
        for row in sorted(frame.selectExpr("CAST(value AS STRING) AS payload", "partition", "offset").collect(),
                          key=lambda item: (item.partition, item.offset)):
            event = json.loads(row.payload)
            sink.write_season(event)

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 100)
        .load()
    )
    writer = (
        stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", args.checkpoint_location)
    )
    query = (writer.trigger(availableNow=True) if args.available_now else writer).start()
    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
