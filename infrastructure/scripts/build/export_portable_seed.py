"""Export validated Silver from HDFS without migration backups or local paths."""
import json
import os
import shutil
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[3]
RUN = "plca_954634acee6255faafab8270"
seed = ROOT / "data/samples/pipeline-seed"
seed.mkdir(parents=True, exist_ok=True)
spark = (SparkSession.builder.appName("ExportPortableSeed").master("local[2]")
         .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000").config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("WARN")
try:
    jvm, conf = spark._jvm, spark._jsc.hadoopConfiguration()
    fs, path = jvm.org.apache.hadoop.fs.FileSystem.get(conf), jvm.org.apache.hadoop.fs.Path
    remote = "/bigdata/processed/" + RUN
    iterator = fs.listFiles(path(remote), True)
    files = 0
    while iterator.hasNext():
        status = iterator.next()
        name = str(status.getPath().toUri().getPath())
        if "/.blocksize_backup/" in name or ".__64m_" in name:
            continue
        if status.getBlockSize() != 67108864:
            raise ValueError("Source is not 64 MiB: " + name)
        local = seed / "silver" / name[len(remote)+1:]
        local.parent.mkdir(parents=True, exist_ok=True)
        jvm.org.apache.hadoop.io.IOUtils.copyBytes(fs.open(path(name)), jvm.java.io.FileOutputStream(str(local)), conf, True)
        files += 1
    season_path = seed / "silver/facts/fact_team_season"
    df = spark.read.parquet(season_path.as_uri())
    from pl_pipeline.config import CLUBS
    names = spark.createDataFrame([(c, n) for c, n, _, _ in CLUBS], "club_id string, club_name string")
    if "club_name" not in df.columns:
        df = df.join(names, "club_id", "left")
    df = df.withColumn("goal_diff", F.col("goals_for") - F.col("goals_against")).withColumn("points_per_mp", F.col("points") / F.col("matches_played"))
    staging = seed / "silver/facts/fact_team_season_updated"
    df.coalesce(1).write.mode("overwrite").partitionBy("season").parquet(staging.as_uri())
    shutil.rmtree(season_path)
    staging.rename(season_path)
    for name, relative in [("match_outcome", "match_outcome"), ("knn", "knn")]:
        shutil.copytree(ROOT / "data/models" / relative, seed / name, dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns("__pycache__", ".*.crc"))
    counts = {table: spark.read.parquet((seed / "silver/facts" / table).as_uri()).count() for table in ["fact_team_season", "fact_team_match", "fact_match_enriched"]}
    manifest = {"source_run": RUN, "source_files_64mib": files, "counts": counts, "blocksize": 67108864, "replication": 2}
    (seed / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest), flush=True)
finally:
    spark.stop()
