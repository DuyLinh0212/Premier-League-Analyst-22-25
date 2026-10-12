"""Portable bootstrap and serial Spark stage runner."""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path
from pl_pipeline.core import Context, ROOT

OUTPUT = Path(os.getenv("MATCH_MODEL_DIR", str(ROOT / "data/models/match_outcome")))
KNN = Path(os.getenv("KNN_OUTPUT_DIR", str(ROOT / "data/models/knn")))

def hdfs(*args, check=True):
    return subprocess.run([os.getenv("HDFS_BIN", "hdfs"), "dfs", "-Ddfs.blocksize=67108864", "-Ddfs.replication=2", *map(str, args)], check=check)

def bootstrap():
    from analytics.publish import publish_from_silver
    silver = ROOT / 'data/lake/silver'
    publish_from_silver(silver, os.getenv('SILVER_SNAPSHOT_KEY', 'premier-league-22-25:silver:bootstrap'))

def stage(name, run_id=None):
    from ingestion.hdfs.upload_raw import preflight, register_raw_snapshot
    from pl_pipeline.identity import build_identity
    from pl_pipeline.silver import build_silver, validate_silver, publish_silver
    run_id = run_id or "football_" + hashlib.sha256(os.getenv("PIPELINE_RUN_ID", "manual").encode()).hexdigest()[:24]
    ctx = Context(run_id)
    functions = {f.__name__: f for f in [preflight, register_raw_snapshot, build_identity, build_silver, validate_silver, publish_silver]}
    if name in functions:
        functions[name](ctx)
    elif name == "knn":
        ctx.require("publish_silver")
        subprocess.run(["spark-submit", "--master", "local[2]", "--driver-memory", "512m",
                        "--conf", "spark.hadoop.dfs.blocksize=67108864", "--conf", "spark.hadoop.parquet.block.size=67108864", "--conf", "spark.hadoop.dfs.replication=2", "--conf", "spark.hadoop.dfs.client.use.datanode.hostname=true",
                        str(ROOT / "ml/inference/knn/knn_league_table.py"),
                        "--input-path", ctx.silver_table_path("fact_team_season"), "--output-dir", str(KNN),
                        "--additional-input-path", ctx.uri + ctx.base + "/kafka/processed/facts/league_table",
                        "--checkpoint-location", ctx.uri + ctx.base + "/checkpoints/analytics/knn_league_table"], check=True)
        ctx.completed("knn")
    elif name == "match_outcome":
        ctx.require("knn")
        from match_outcome.train import train
        from match_outcome.compare import compare
        train(ROOT, OUTPUT, ctx.silver_table_path("fact_match_enriched"))
        report = compare(OUTPUT)
        ctx.completed("match_outcome", {"model_version": report["model_version"], "test": report["test"]})
    elif name == "publish":
        ctx.require("match_forecast_v2")
        # The following merged_publish stage atomically stores the complete
        # report bundle in Neon.
        result = {"model_version": json.loads((OUTPUT / "model.json").read_text(encoding="utf-8"))["model_version"],
                  "forecast_version": json.loads((OUTPUT / "forecast_v2.json").read_text(encoding="utf-8"))["model_version"],
                  "target": "Neon PostgreSQL"}
        ctx.completed("publish", result)
    elif name == 'match_forecast_v2':
        ctx.require('match_outcome')
        from match_outcome.train_v2 import train
        from pl_pipeline.core import clean_json
        spark=ctx.session('MatchForecastV2')
        try:
            goals=[clean_json(r.asDict()) for r in spark.read.parquet(ctx.silver_table_path('fact_goal_event')).collect()]
            for g in goals:
                g['stoppage_minutes']=int(g.get('stoppage_minutes') or 0)
            matches=[json.loads(line) for line in (OUTPUT/'matches.jsonl').read_text(encoding='utf-8').splitlines()]
            report=train(matches,goals,OUTPUT)
            for filename in ('forecast_v2.json','forecast_v2_metrics.json'):
                target=ctx.gold+'/prediction_features/models/'+report['model_version']+'/'+filename[:-5]
                spark.createDataFrame([((OUTPUT/filename).read_text(encoding='utf-8'),)],'payload_json string').coalesce(1).write.mode('overwrite').parquet(target)
                if spark.read.parquet(target).first()['payload_json']!=(OUTPUT/filename).read_text(encoding='utf-8'):
                    raise RuntimeError('HDFS v2 model read-back mismatch')
            ctx.completed('match_forecast_v2',{'model_version':report['model_version'],'pregame':report['pregame'],'live':report['live']})
        finally:
            spark.stop()
    elif name == "merged_publish":
        ctx.require("publish")
        from analytics.publish import publish_hdfs
        ctx.completed("merged_publish", publish_hdfs(ctx))
    else:
        raise ValueError("Unknown stage: " + name)

if __name__ == "__main__":
    import fcntl
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["bootstrap", "stage"])
    parser.add_argument("stage", nargs="?")
    args = parser.parse_args()
    state = Path(os.getenv("STATE_DIR", "/state"))
    state.mkdir(parents=True, exist_ok=True)
    with (state / "spark.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if args.command == "bootstrap":
            bootstrap()
        else:
            from analytics import neon_store
            run_id = "football_" + hashlib.sha256(os.getenv("PIPELINE_RUN_ID", "manual").encode()).hexdigest()[:24]
            neon_store.set_job(run_id, args.stage, "running")
            try:
                stage(args.stage, run_id)
                neon_store.set_job(run_id, args.stage, "success", Context(run_id).manifest)
            except Exception as exc:
                neon_store.set_job(run_id, args.stage, "failed", error=str(exc)[:1000])
                raise
