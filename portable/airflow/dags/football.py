"""Container-only orchestration; no host paths, SSH or Docker socket."""
from datetime import timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime

DEFAULTS = {"owner": "football", "retries": 2, "retry_delay": timedelta(seconds=30), "pool": "spark_serial"}
with DAG("football_pipeline", schedule=None, start_date=datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
         catchup=False, max_active_runs=1, max_active_tasks=1, default_args=DEFAULTS, tags=["spark", "knn", "outcomes"]) as pipeline:
    previous = None
    for stage in ["preflight", "register_raw_snapshot", "build_identity", "build_silver", "validate_silver", "publish_silver", "knn", "match_outcome", "match_forecast_v2", "publish", "merged_publish"]:
        task = BashOperator(task_id=stage, bash_command="$SPARK_PYTHON /opt/project/portable/runtime.py stage " + stage,
                            env={"PIPELINE_RUN_ID": "{{ run_id | replace(':', '_') | replace('+', '_') | replace('.', '_') }}"}, append_env=True,
                            execution_timeout=timedelta(hours=2))
        if previous:
            previous >> task
        previous = task

with DAG("football_kafka_analytics", schedule="* * * * *", start_date=datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
         catchup=False, max_active_runs=1, max_active_tasks=1, default_args=DEFAULTS, tags=["kafka", "spark"]) as streaming:
    readiness = "$SPARK_PYTHON /opt/project/portable/wait_services.py && "
    league = BashOperator(task_id="league_normalization", bash_command=readiness + "$SPARK_PYTHON /opt/project/Generate_LeagueTable_Api/streaming_spark.py --available-now")
    matches = BashOperator(task_id="match_statistics", bash_command=readiness + "$SPARK_PYTHON -m match_outcome.stream")
    league >> matches
    transfers = BashOperator(task_id="transfer_simulation", bash_command=readiness + "$SPARK_PYTHON -m analytics.streaming")
    matches >> transfers
