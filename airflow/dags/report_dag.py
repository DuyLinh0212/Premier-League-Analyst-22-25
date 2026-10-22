"""Historical pipeline executed by the Airflow 3 scheduler in Ubuntu/WSL."""
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT = os.environ.get("PLCA_PROJECT_ROOT", str(Path(__file__).resolve().parents[2]))
PYTHON = os.environ.get("PLCA_SPARK_PYTHON", str(Path.home() / ".venvs/plca-spark/bin/python"))
STAGES = ("preflight", "register_raw_snapshot", "normalize", "build_identity", "build_silver",
          "validate_silver", "analyze", "export_reports", "archive_results", "load_postgres",
          "validate_postgres", "publish")

with DAG(
    dag_id="premier_league_reports",
    description="Nạp Bronze → Silver Parquet → 10 phân tích → HDFS → PostgreSQL",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), schedule=None, catchup=False,
    max_active_runs=1, max_active_tasks=1, is_paused_upon_creation=False,
    dagrun_timeout=timedelta(hours=8),
    default_args={"owner": "premier-league", "retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["premier-league", "hdfs", "postgresql", "khong-docker"],
    doc_md="""Chạy thủ công với cấu hình JSON chứa run_id. Mỗi bước chạy một tiến trình thật,
    lưu manifest và log tiếng Việt. Chỉ công bố snapshot sau khi đối chiếu Parquet HDFS với
    PostgreSQL. Retry dùng lại run_id, không nạp trùng. Không dùng Silver CSV hoặc ML.""",
) as dag:
    previous = None
    for stage in STAGES:
        task = BashOperator(
            task_id=stage,
            bash_command='exec "$PLCA_SPARK_PYTHON" -m src.pipeline.stages ' + stage + ' --run-id "$PLCA_RUN_ID"',
            cwd=PROJECT,
            env={"PLCA_SPARK_PYTHON": PYTHON, "PLCA_RUN_ID": "{{ dag_run.conf['run_id'] }}",
                 "PYTHONPATH": PROJECT, "PYTHONUNBUFFERED": "1", "PYTHONUTF8": "1"},
            append_env=True, pool="plca_spark", do_xcom_push=False,
            execution_timeout=timedelta(hours=4 if stage == "normalize" else 2),
        )
        if previous is not None:
            previous >> task
        previous = task
