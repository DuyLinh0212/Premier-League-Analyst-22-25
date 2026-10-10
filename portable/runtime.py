"""Portable bootstrap and serial Spark stage runner."""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path
from pl_pipeline.core import Context, ROOT
from match_outcome.graph import publish_bundle

OUTPUT = Path(os.getenv("MATCH_MODEL_DIR", str(ROOT / "Generate_LeagueTable_Api/analystic/match_outcome/outputs")))
KNN = Path(os.getenv("KNN_OUTPUT_DIR", str(ROOT / "Generate_LeagueTable_Api/analystic/knn/outputs")))

def hdfs(*args, check=True):
    return subprocess.run([os.getenv("HDFS_BIN", "hdfs"), "dfs", "-Ddfs.blocksize=67108864", "-Ddfs.replication=2", *map(str, args)], check=check)

def bootstrap():
    from analytics.ingest import import_sources
    from analytics.publish import publish
    publish(import_sources())

def stage(name, run_id=None):
    from pl_pipeline.ingest import preflight, register_raw_snapshot
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
                        str(ROOT / "Generate_LeagueTable_Api/analystic/knn/knn_league_table.py"),
                        "--input-path", ctx.silver + "/facts/fact_team_season", "--output-dir", str(KNN),
                        "--checkpoint-location", ctx.uri + "/bigdata/checkpoints/analytics/knn_league_table"], check=True)
        ctx.completed("knn")
    elif name == "match_outcome":
        ctx.require("knn")
        from match_outcome.train import train
        from match_outcome.compare import compare
        train(ROOT, OUTPUT, ctx.silver)
        report = compare(OUTPUT)
        ctx.completed("match_outcome", {"model_version": report["model_version"], "test": report["test"]})
    elif name == "publish":
        ctx.require("match_outcome")
        result = publish_bundle(OUTPUT, KNN, ctx.run_id)
        ctx.completed("publish", result)
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
            from analytics import store
            run_id = "football_" + hashlib.sha256(os.getenv("PIPELINE_RUN_ID", "manual").encode()).hexdigest()[:24]
            job_key = store.namespace() + ":job:" + run_id + ":" + args.stage
            store.schema()
            store.query("MERGE (j:PLJob {key:$key}) SET j.namespace=$ns,j.run_id=$run,j.stage=$stage,j.status='running',j.started_at=$now,j.error=null", {"key":job_key,"ns":store.namespace(),"run":run_id,"stage":args.stage,"now":store.now()})
            try:
                stage(args.stage, run_id)
                store.query("MATCH (j:PLJob {key:$key}) SET j.status='success',j.completed_at=$now,j.manifest_json=$manifest", {"key":job_key,"now":store.now(),"manifest":store.encode(Context(run_id).manifest)})
            except Exception as exc:
                store.query("MATCH (j:PLJob {key:$key}) SET j.status='failed',j.completed_at=$now,j.error=$error", {"key":job_key,"now":store.now(),"error":str(exc)[:1000]})
                raise
