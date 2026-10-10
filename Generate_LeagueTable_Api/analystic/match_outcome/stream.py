"""Bounded Kafka consumption: HDFS first, Neo4j second, then checkpoint."""
import argparse
import glob
import hashlib
import json
import os
from pathlib import Path
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from .inference import load_artifact, load_published_artifact, predict
from .schema import validate_stats
from .graph import schema, publish_predictions

def replay(output):
    producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"), acks="all", retries=3,
                             value_serializer=lambda x: json.dumps(x, ensure_ascii=False, allow_nan=False).encode())
    try:
        for line in (Path(output) / "matches.jsonl").read_text(encoding="utf-8").splitlines():
            event = json.loads(line)
            producer.send("match_statistics", key=event["event_id"].encode(), value=event).get(timeout=30)
        producer.flush()
    finally:
        producer.close()

def process_events(spark, events, output, base):
    artifact = load_published_artifact(output)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path
    prepared = {}
    for event in events:
        if event.get("schema_version") != 1 or event.get("kind") not in {"historical", "scenario"}:
            raise ValueError("Unsupported Kafka schema/kind")
        event_id = event.get("event_id", "")
        if len(event_id) not in {32, 64} or any(c not in "0123456789abcdef" for c in event_id):
            raise ValueError("Invalid Kafka event id")
        for side in ("home", "away"):
            validate_stats(event[side + "_stats"])
        if event["kind"] == "historical" and (event.get("is_simulated") or event.get("season") not in {"2022-2023", "2023-2024", "2024-2025"}):
            raise ValueError("Simulated input cannot be historical training data")
        payload = json.dumps(event, sort_keys=True, ensure_ascii=False, allow_nan=False)
        sha = hashlib.sha256(payload.encode()).hexdigest()
        if event_id in prepared and prepared[event_id][0] != sha:
            raise ValueError("Conflicting duplicate event")
        prepared[event_id] = (sha, payload, event)
    results = []
    pending = []
    for event_id, (sha, payload, event) in prepared.items():
        target = f"{base}/facts/match_statistics/event_id={event_id}"
        marker = path(target + "/sha256_" + sha)
        if fs.exists(path(target)):
            if not fs.exists(marker):
                raise ValueError("HDFS event conflict: " + event_id)
        else:
            pending.append((event_id, sha, payload, event))
        requested_version = event.get("model_version", artifact["model_version"])
        import re
        if not re.fullmatch(r"wdl_[0-9a-f]{20}", requested_version):
            raise ValueError("Invalid model version")
        selected = artifact if requested_version == artifact["model_version"] else load_artifact(Path(output) / "versions" / requested_version)
        results.append((event, predict(event, selected)))
    if pending:
        batch_key = hashlib.sha256("".join(sorted(x[0] for x in pending)).encode()).hexdigest()
        staging = base + "/staging/" + batch_key
        fs.delete(path(staging), True)
        data = [(eid, e.get("match_id"), e.get("season"), e["kind"], bool(e.get("is_simulated")), payload)
                for eid, sha, payload, e in pending]
        spark.createDataFrame(data, "event_id string, match_id string, season string, kind string, is_simulated boolean, source_parameters_json string") \
            .coalesce(1).write.mode("overwrite").partitionBy("event_id").parquet(staging)
        if spark.read.parquet(staging).count() != len(pending):
            raise RuntimeError("HDFS batch read-back failed")
        fs.mkdirs(path(base + "/facts/match_statistics"))
        for eid, sha, payload, event in pending:
            folder = staging + "/event_id=" + eid
            fs.create(path(folder + "/sha256_" + sha), False).close()
            if not fs.rename(path(folder), path(base + "/facts/match_statistics/event_id=" + eid)):
                raise RuntimeError("HDFS event commit failed")
        fs.delete(path(staging), True)
    publish_predictions(results)
    return len(results)

def consume(output):
    uri = os.getenv("HDFS_URI", "hdfs://namenode:9000")
    spark = (SparkSession.builder.appName("MatchStatisticsKafka").master(os.getenv("SPARK_MASTER", "local[2]"))
             .config("spark.driver.memory", "512m")
             .config("spark.hadoop.fs.defaultFS", uri).config("spark.hadoop.dfs.blocksize", "67108864").config("spark.hadoop.parquet.block.size", "67108864")
             .config("spark.hadoop.dfs.client.use.datanode.hostname", "true").config("spark.hadoop.dfs.replication", "2").config("spark.sql.shuffle.partitions", "4")
             .config("spark.jars", ",".join(glob.glob(os.getenv("SPARK_EXTRA_JARS", "/opt/kafka-jars/*.jar"))))
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    schema()
    try:
        frame = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
                 .option("subscribe", "match_statistics").option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", 100).load())
        def batch(rows, batch_id):
            events = [json.loads(r.payload) for r in rows.selectExpr("CAST(value AS STRING) AS payload").collect()]
            if events:
                print(f"Batch {batch_id}: {process_events(spark, events, output, uri + '/bigdata/processed/kafka')} events", flush=True)
        query = (frame.writeStream.foreachBatch(batch).option("checkpointLocation", uri + "/bigdata/checkpoints/match_statistics")
                 .trigger(availableNow=True).start())
        query.awaitTermination()
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=os.getenv("MATCH_MODEL_DIR", str(Path(__file__).parent / "outputs")))
    parser.add_argument("--replay", action="store_true")
    args = parser.parse_args()
    replay(args.output) if args.replay else consume(args.output)
