"""Run a single retryable stage, or the complete non-demo production pipeline."""
import argparse
import traceback
import subprocess
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pl_pipeline.core import Context, now
from pl_pipeline.ingest import preflight, register_raw_snapshot
from pl_pipeline.identity import build_identity
from pl_pipeline.silver import build_silver, validate_silver, publish_silver


def stages():
    from portable.runtime import stage
    result = {f.__name__: f for f in [preflight, register_raw_snapshot, build_identity, build_silver, validate_silver, publish_silver]}
    for name in ("knn", "match_outcome", "publish"):
        result[name] = lambda ctx, name=name: stage(name, ctx.run_id)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--stage", default="all")
    parser.add_argument("--inject-quality-failure", action="store_true")
    parser.add_argument("--from-stage")
    parser.add_argument("--to-stage")
    args = parser.parse_args()
    ctx = Context(args.run_id)
    active_stage = args.stage or "invalid_cli_request"
    try:
        if not args.from_stage and not args.to_stage and args.stage in {"preflight", "register_raw_snapshot", "build_identity", "build_silver", "validate_silver", "publish_silver"}:
            fn = globals()[args.stage]
            fn(ctx, inject_failure=True) if args.inject_quality_failure else fn(ctx)
        else:
            available = stages()
            names = list(available)
            if args.from_stage or args.to_stage:
                selected = names[names.index(args.from_stage) if args.from_stage else 0:(names.index(args.to_stage)+1) if args.to_stage else len(names)]
            else:
                selected = names if args.stage == "all" else [args.stage]
            for name in selected:
                active_stage = name
                if len(selected)>1:
                    result = subprocess.run([sys.executable, __file__, "--run-id", args.run_id, "--stage", name])
                    if result.returncode:
                        raise SystemExit(result.returncode)
                else:
                    available[name](ctx)
    except Exception as exc:
        stage_name = active_stage + "_failure_test" if args.inject_quality_failure else active_stage
        ctx.manifest["stages"][stage_name] = {"status": "expected_failure" if args.inject_quality_failure else "failed", "failed_at_utc": now(), "error": str(exc)}
        ctx.save()
        traceback.print_exc()
        raise SystemExit(1)
