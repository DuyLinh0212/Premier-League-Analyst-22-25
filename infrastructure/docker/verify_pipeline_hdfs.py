"""Verify the Airflow run's canonical Bronze/Silver/Gold HDFS output."""
import json
import os
from pathlib import Path
from urllib.request import urlopen

from pyspark.sql import SparkSession


def main():
    pipeline_root = Path(os.getenv("PIPELINE_RUNS_DIR", "/opt/project/pipeline_runs"))
    manifests = sorted(pipeline_root.glob("*/manifest.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    manifest = next((json.loads(path.read_text(encoding="utf-8")) for path in manifests
                     if json.loads(path.read_text(encoding="utf-8")).get("stages", {}).get("merged_publish", {}).get("status") == "success"), None)
    if manifest is None:
        raise RuntimeError("No completed Airflow pipeline manifest was found")
    uri = os.environ["HDFS_URI"]
    nn_uri = os.environ["NAMENODE_HTTP_URI"].rstrip("/")
    with urlopen(nn_uri + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", timeout=15) as response:
        nn = json.load(response)["beans"][0]
    live_nodes = len(json.loads(nn.get("LiveNodes", "{}")))
    if live_nodes != 3 or nn.get("Safemode"):
        raise RuntimeError(f"HDFS cluster readiness mismatch: live_nodes={live_nodes}, safe_mode={nn.get('Safemode')}")

    required = [
        f"{uri}/premier-league/bronze/fbref/season={season}" for season in ("2022-23", "2023-24", "2024-25")
    ] + [
        f"{uri}/premier-league/bronze/transfermarkt/season={season}" for season in ("2022-23", "2023-24", "2024-25")
    ] + [f"{uri}/premier-league/silver/{name}" for name in
         ("teams", "players", "matches", "transfers", "team_season_stats", "player_season_stats")]
    spark = (SparkSession.builder.appName("PLCA_CI_HDFS_Verification").master("local[1]")
             .config("spark.ui.enabled", "false").config("spark.driver.memory", "512m")
             .config("spark.hadoop.fs.defaultFS", uri).config("spark.hadoop.dfs.blocksize", "67108864")
             .config("spark.hadoop.parquet.block.size", "67108864").config("spark.hadoop.dfs.client.use.datanode.hostname", "true").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        path_cls = spark._jvm.org.apache.hadoop.fs.Path
        missing = [path for path in required if not fs.exists(path_cls(path))]
        if missing:
            raise RuntimeError("HDFS layout paths missing: " + ", ".join(missing))
        tables = manifest.get("tables", {})
        if len(tables) != 21:
            raise RuntimeError(f"Expected 21 validated Silver tables, found {len(tables)}")
        total_rows = 0
        for name, info in tables.items():
            observed = spark.read.parquet(info["path"]).count()
            if observed != info["row_count"]:
                raise RuntimeError(f"HDFS read-back mismatch for {name}: {observed} != {info['row_count']}")
            total_rows += observed
        for group in ("player_performance", "season_comparison", "league_standings", "tactical_insights", "prediction_features"):
            if not fs.exists(path_cls(f"{uri}/premier-league/gold/{group}")):
                raise RuntimeError("Gold category missing: " + group)
        sample = spark.read.parquet(tables["facts/fact_match_enriched"]["path"]).inputFiles()[0]
        status = fs.getFileStatus(path_cls(sample))
        block_size = int(status.getBlockSize())
        if block_size != 67108864:
            raise RuntimeError(f"HDFS file block size {block_size} != 67108864")
        output = {"run_id": manifest.get("analysis_run_id"), "live_datanodes": live_nodes,
                  "hdfs_base": "/premier-league", "bronze_seasons": ["2022-23", "2023-24", "2024-25"],
                  "silver_table_count": len(tables), "silver_row_count": total_rows,
                  "silver_paths": sorted({str(info["path"]).split("/silver/",1)[-1].split("/",1)[0] for info in tables.values()}),
                  "gold_categories": ["player_performance", "season_comparison", "league_standings", "tactical_insights", "prediction_features"],
                  "verified_block_size_bytes": block_size}
        print(json.dumps(output, ensure_ascii=False, sort_keys=True), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
