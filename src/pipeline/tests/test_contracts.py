import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from src.pipeline.config import Config, ROOT
from src.pipeline.context import Context, safe_id
from src.pipeline.database import postgres_sql
from src.pipeline.evidence import redact
from src.pipeline.postgres_setup import read_credentials, write_credentials
from src.pipeline.stages import assert_equivalent


@pytest.mark.parametrize("value", ["../other", "a/b", "x;touch p", "", "a"*101, "manual:run"])
def test_run_id_cannot_escape_snapshot(value):
    with pytest.raises(ValueError):
        safe_id(value)


def test_context_rejects_config_change_for_existing_run(tmp_path, monkeypatch):
    monkeypatch.setenv("PLCA_PIPELINE_STATE", str(tmp_path / "state"))
    original = Context("test_snapshot")
    original.completed("preflight", {"rows": 60})
    assert Context("test_snapshot").manifest["stages"]["preflight"]["rows"] == 60
    config = Config()
    config.values = {**config.values, "minimum_minutes": 1000}
    with pytest.raises(RuntimeError, match="Cấu hình"):
        Context("test_snapshot", config)


def test_credential_file_is_atomic_and_private(tmp_path):
    path = tmp_path / "protected/credentials.json"
    value = {"port": 5432, **{role: {"user": "plca_"+role, "password": "test_only"} for role in ("writer", "reader", "airflow")}}
    write_credentials(path, value)
    assert read_credentials(path) == value
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(ValueError):
        write_credentials(path, {"broken": True})
    assert read_credentials(path) == value
    assert not list(path.parent.glob(".plca-credentials-*"))


def test_postgres_transpilation_preserves_percentile_null_ordering():
    statement, names = postgres_sql("SELECT first(p.name,true) AS playerName, percent_rank() OVER (ORDER BY p.goals DESC NULLS LAST) AS goalsPct FROM players p GROUP BY p.goals")
    assert "MIN(p.name)" in statement
    assert "NULLS LAST" in statement
    assert "PERCENT_RANK()" in statement
    assert names["playername"] == "playerName"


def test_matchday_capture_is_a_native_postgresql_function():
    statement, _ = postgres_sql("SELECT CAST(nullif(regexp_extract(round, '([0-9]+)', 1), '') AS INT) AS matchday FROM tm_games")
    assert "REGEXP_SUBSTR(" in statement.upper()
    assert "REGEXP_EXTRACT(" not in statement.upper()
    assert "NULLIF(" in statement.upper()


def test_equivalence_preserves_missing_values_and_metric_precision():
    assert_equivalent({"points": 10.0, "value": None}, {"points": 10, "value": None})
    with pytest.raises(AssertionError):
        assert_equivalent({"value": None}, {"value": 0})
    with pytest.raises(AssertionError):
        assert_equivalent({"points": 10}, {"points": 11})


def test_artifacts_do_not_expose_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("PLCA_PIPELINE_STATE", str(tmp_path))
    config = Config()
    config.credentials = lambda: {role: {"password": "dummy_" + role} for role in ("writer", "reader", "airflow")}
    output = redact("password dummy_writer / dummy_reader / dummy_airflow\nALTER ROLE user PASSWORD 'unknown-secret';", config)
    assert "dummy_" not in output and "unknown-secret" not in output


def test_api_import_does_not_import_spark_or_connect_database():
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "apps/analytics-web/api")])}
    code = "import app.main,sys; assert 'pyspark' not in sys.modules; assert app.main.spark is None; print('PASS')"
    result = subprocess.run([sys.executable, "-c", code], env=env, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_dag_imports_on_installed_airflow():
    config = Config()
    python = config.python("airflow")
    if not Path(python).is_file():
        pytest.skip("Airflow installed in WSL is needed for DAG import validation")
    code = """from airflow.models.dagbag import DagBag
bag=DagBag(dag_folder='airflow/dags')
assert not bag.import_errors, bag.import_errors
dag=bag.dags['premier_league_reports']
assert len(dag.tasks)==12
assert dag.max_active_runs==1 and dag.schedule is None
assert all(t.task_type=='BashOperator' and t.retries==2 and t.pool=='plca_spark' for t in dag.tasks)
assert len(dag.roots)==1 and len(dag.leaves)==1
print('PASS')
"""
    result = subprocess.run([python, "-c", code], cwd=ROOT, text=True, capture_output=True, timeout=90)
    assert result.returncode == 0, result.stderr[-5000:]


def test_airflow_client_authenticates_once_inside_context(tmp_path, monkeypatch):
    import httpx
    import src.pipeline.ops as ops
    monkeypatch.setenv("PLCA_PIPELINE_STATE", str(tmp_path))
    client_type = httpx.Client
    requests = []
    def handle(request):
        requests.append(request.url.path)
        return httpx.Response(201, json={"access_token": "test-only-token"}) if request.url.path == "/auth/token" else httpx.Response(200, json={"ok": True})
    monkeypatch.setattr(ops.httpx, "Client", lambda **kwargs: client_type(**kwargs, transport=httpx.MockTransport(handle)))
    with ops.airflow_client(Config()) as client:
        assert client.get("/api/v2/dags").json()["ok"]
    assert requests == ["/auth/token", "/api/v2/dags"]
    assert client.is_closed


def test_hdfs_failure_stops_before_upload_or_local_fallback(tmp_path, monkeypatch):
    from src.pipeline.stages import register_raw_snapshot
    from unittest.mock import Mock
    monkeypatch.setenv("PLCA_PIPELINE_STATE", str(tmp_path))
    ctx = Context("test_hdfs_unavailable")
    client = Mock()
    client.require_ready.side_effect = RuntimeError("HDFS unavailable - injected test")
    ctx.hdfs = client
    with pytest.raises(RuntimeError, match="HDFS unavailable"):
        register_raw_snapshot(ctx)
    client.hdfs.assert_not_called()
    assert not (ctx.local / "source_manifest.json").exists()


def test_api_postgres_failure_returns_503_without_spark(monkeypatch):
    from fastapi.testclient import TestClient
    api = str(ROOT / "apps/analytics-web/api")
    monkeypatch.syspath_prepend(api)
    import app.main as main
    import psycopg
    def unavailable(*args, **kwargs):
        raise psycopg.OperationalError("database unavailable - injected test")
    monkeypatch.setattr(main.runtime, "request", unavailable)
    with TestClient(main.app) as client:
        response = client.get("/health")
    assert response.status_code == 503
    assert response.json()["database"] is False
    assert "pyspark" not in sys.modules


def test_analysis_query_failure_is_not_hidden(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / "apps/analytics-web/api"))
    from app.queries import age_distribution
    from unittest.mock import Mock
    engine = Mock()
    engine.sql.side_effect = RuntimeError("analysis unavailable - injected test")
    with pytest.raises(RuntimeError, match="analysis unavailable"):
        age_distribution.run(engine, "2024-2025")


def test_interrupted_postgres_copy_keeps_active_snapshot(tmp_path, monkeypatch):
    from src.pipeline.database import connect, load_snapshot
    from unittest.mock import Mock
    from uuid import uuid4
    config = Config()
    if not Path(config["credentials_file"]).expanduser().is_file():
        pytest.skip("PostgreSQL setup is required for transactional recovery check")
    monkeypatch.setenv("PLCA_PIPELINE_STATE", str(tmp_path))
    ctx = Context("test_recovery_" + uuid4().hex)
    with connect() as db:
        before = db.execute("SELECT run_id FROM plca.active WHERE id=1").fetchone()
    ctx.manifest["tables"] = {"facts/test_partial_copy": {"path": "/test-injected-source", "row_count": 2, "keys": ["id"],
        "schema": {"fields": [{"name": "id", "type": "long", "nullable": False}]}}}
    def interrupted():
        yield (1,)
        raise RuntimeError("source interrupted during COPY - injected test")
    spark = Mock()
    spark.read.parquet.return_value.select.return_value.toLocalIterator.side_effect = interrupted
    with pytest.raises(RuntimeError, match="source interrupted"):
        load_snapshot(ctx, spark)
    with connect() as db:
        assert db.execute("SELECT run_id FROM plca.active WHERE id=1").fetchone() == before
        assert db.execute("SELECT to_regnamespace(%s) AS name", (ctx.schema_name,)).fetchone()["name"] is None
        assert db.execute("SELECT run_id FROM plca.runs WHERE run_id=%s", (ctx.run_id,)).fetchone() is None


def test_static_report_rows_come_from_postgres_not_image_json(tmp_path):
    from src.pipeline.database import PostgresRuntime, REQUEST
    from unittest.mock import Mock
    directory = tmp_path / "reports/2024-2025"
    directory.mkdir(parents=True)
    (directory / "age-distribution.json").write_text(json.dumps({"report": {"title": "Độ tuổi", "data": [{"stale": True}]}}))
    db = Mock()
    db.execute.return_value.fetchone.return_value = {"payload": {"data": [{"age_band": "23–27", "player_count": 100}], "meta": {}}}
    token = REQUEST.set({"db": db, "run": {"run_id": "published_run", "artifact_path": str(tmp_path), "manifest": {}}})
    try:
        value = PostgresRuntime().report("2024-2025", "age-distribution")
        assert value["data"] == [{"age_band": "23–27", "player_count": 100}]
        assert value["meta"]["source"] == "postgresql"
        assert value["meta"]["run_id"] == "published_run"
        assert db.execute.call_args.args[1] == ("age-distribution__2024-2025",)
    finally:
        REQUEST.reset(token)
