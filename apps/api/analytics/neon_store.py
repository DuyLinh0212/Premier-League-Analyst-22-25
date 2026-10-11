"""Neon/PostgreSQL serving tables for immutable analytics reports.

The lakehouse remains in HDFS. This module writes only run metadata, report
payloads and active-report pointers to PostgreSQL.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb


_schema_lock = threading.Lock()
_schema_ready = False


def _dsn() -> str:
    value = os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not value:
        raise RuntimeError("NEON_DATABASE_URL is required for Neon publication")
    return value


def _connect():
    return psycopg.connect(_dsn(), connect_timeout=15, application_name="premier-league-analytics")


def _now():
    return datetime.now(timezone.utc)


def _filter_key(filters):
    packed = json.dumps(filters or {}, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(packed.encode("utf-8")).hexdigest(), packed


def schema():
    """Create the serving schema idempotently; source rows are never copied."""
    global _schema_ready
    if _schema_ready:
        return
    with _schema_lock:
        if _schema_ready:
            return
        schema_file = Path(__file__).resolve().parents[3] / "database/schemas/analytics.sql"
        statements = [item.strip() for item in schema_file.read_text(encoding="utf-8").split(";") if item.strip()]
        with _connect() as connection:
            with connection.cursor() as cursor:
                for statement in statements:
                    cursor.execute(statement)
        _schema_ready = True


def begin_run(run_id, dataset_id, parameters=None, source_manifest=None):
    schema()
    with _connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO pl_analysis_runs (run_id,dataset_id,status,parameters,source_manifest) VALUES (%s,%s,'processing',%s,%s) ON CONFLICT (run_id) DO NOTHING",
            (run_id, dataset_id, Jsonb(parameters or {}), Jsonb(source_manifest or {})),
        )
        cursor.execute("SELECT status,dataset_id FROM pl_analysis_runs WHERE run_id=%s", (run_id,))
        existing = cursor.fetchone()
        if existing[0] == "complete":
            if existing[1] != dataset_id:
                raise ValueError("Immutable run ID is already bound to a different dataset")
            return False
        if existing[0] == "failed":
            cursor.execute("UPDATE pl_analysis_runs SET status='processing',error=NULL,created_at=now(),completed_at=NULL WHERE run_id=%s", (run_id,))
    return True


def publish_reports(run_id, dataset_id, reports, source_manifest=None):
    """Commit a complete report bundle and pointers atomically."""
    begin_run(run_id, dataset_id, source_manifest=source_manifest)
    prepared = []
    for name, payload, filters in reports:
        filter_hash, _ = _filter_key(filters)
        model_version = payload.get("model_version") if isinstance(payload, dict) else None
        prepared.append((name, filter_hash, filters or {}, payload, model_version))
    if not prepared:
        raise ValueError("Cannot publish an empty report bundle")
    try:
        with _connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT status FROM pl_analysis_runs WHERE run_id=%s FOR UPDATE", (run_id,))
                row = cursor.fetchone()
                if row is None or row[0] == "failed":
                    raise RuntimeError("Analysis run is not publishable")
                for name, filter_hash, filters, payload, model_version in prepared:
                    cursor.execute(
                        "INSERT INTO pl_analysis_reports (run_id,report_name,filter_key,filters,payload,model_version) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (run_id,report_name,filter_key) DO NOTHING",
                        (run_id, name, filter_hash, Jsonb(filters), Jsonb(payload), model_version),
                    )
                    cursor.execute("SELECT payload FROM pl_analysis_reports WHERE run_id=%s AND report_name=%s AND filter_key=%s", (run_id, name, filter_hash))
                    if cursor.fetchone()[0] != payload:
                        raise ValueError("Completed report is immutable: " + name)
                cursor.execute("SELECT count(*) FROM pl_analysis_reports WHERE run_id=%s", (run_id,))
                observed = cursor.fetchone()[0]
                if observed != len(prepared):
                    raise RuntimeError(f"Incomplete publication: {observed} reports, expected {len(prepared)}")
                cursor.execute("UPDATE pl_analysis_runs SET status='complete',report_count=%s,completed_at=now(),error=NULL WHERE run_id=%s", (observed, run_id))
                for name, filter_hash, *_ in prepared:
                    cursor.execute(
                        "INSERT INTO pl_analysis_active_reports (report_name,filter_key,run_id,updated_at) VALUES (%s,%s,%s,now()) ON CONFLICT (report_name,filter_key) DO UPDATE SET run_id=EXCLUDED.run_id,updated_at=now()",
                        (name, filter_hash, run_id),
                    )
    except Exception as exc:
        with _connect() as connection, connection.cursor() as cursor:
            cursor.execute("UPDATE pl_analysis_runs SET status='failed',error=%s,completed_at=now() WHERE run_id=%s AND status<>'complete'", (str(exc)[:1000], run_id))
        raise


def run_complete(run_id):
    schema()
    with _connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT status FROM pl_analysis_runs WHERE run_id=%s", (run_id,))
        row = cursor.fetchone()
        return bool(row and row[0] == "complete")


def report(kind, filters=None, run_id=None):
    schema()
    filter_hash, _ = _filter_key(filters)
    with _connect() as connection, connection.cursor() as cursor:
        if run_id:
            cursor.execute("SELECT p.payload,r.run_id,r.model_version,r.completed_at,r.status FROM pl_analysis_reports p JOIN pl_analysis_runs r USING (run_id) WHERE p.run_id=%s AND p.report_name=%s AND p.filter_key=%s AND r.status='complete' ORDER BY r.completed_at DESC LIMIT 1", (run_id, kind, filter_hash))
        else:
            cursor.execute("SELECT p.payload,r.run_id,r.model_version,r.completed_at,r.status FROM pl_analysis_active_reports a JOIN pl_analysis_reports p USING (run_id,report_name,filter_key) JOIN pl_analysis_runs r USING (run_id) WHERE a.report_name=%s AND a.filter_key=%s AND r.status='complete'", (kind, filter_hash))
        row = cursor.fetchone()
    if not row:
        raise LookupError("Chưa có báo cáo Neon được công bố: " + kind)
    payload, published_run, model_version, completed_at, status = row
    return payload, {"run_id": published_run, "version": model_version, "completed_at": completed_at.isoformat() if completed_at else None, "status": status}


def runs(limit=50):
    schema()
    with _connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT run_id,dataset_id,status,model_version,created_at,completed_at,report_count,error,parameters FROM pl_analysis_runs ORDER BY created_at DESC LIMIT %s", (limit,))
        names = [column.name for column in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]


def pipeline(limit=100):
    schema()
    with _connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT run_id,stage,status,started_at,completed_at,error,details FROM pl_pipeline_jobs ORDER BY started_at DESC LIMIT %s", (limit,))
        names = [column.name for column in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]


def catalog():
    """Expose published Neon reports as the serving catalog, not HDFS source rows."""
    schema()
    with _connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT p.report_name AS name, "
            "CASE WHEN jsonb_typeof(p.payload)='array' THEN jsonb_array_length(p.payload) ELSE 1 END AS row_count, "
            "jsonb_build_object('storage','neon','run_id',r.run_id,'model_version',p.model_version,"
            "'payload_type',jsonb_typeof(p.payload)) AS metadata "
            "FROM (SELECT DISTINCT ON (report_name) * FROM pl_analysis_active_reports "
            "ORDER BY report_name,updated_at DESC) a "
            "JOIN pl_analysis_reports p USING (run_id,report_name,filter_key) "
            "JOIN pl_analysis_runs r USING (run_id) "
            "WHERE r.status='complete' ORDER BY p.report_name"
        )
        names = [column.name for column in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]


def table_rows(name, offset=0, limit=100):
    """Read a page from one published report payload stored in Neon."""
    schema()
    with _connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT p.payload FROM pl_analysis_active_reports a "
            "JOIN pl_analysis_reports p USING (run_id,report_name,filter_key) "
            "JOIN pl_analysis_runs r USING (run_id) "
            "WHERE a.report_name=%s AND r.status='complete' "
            "ORDER BY a.updated_at DESC LIMIT 1",
            (name,),
        )
        row = cursor.fetchone()
    if not row:
        raise LookupError("Chưa có báo cáo Neon được công bố: " + name)
    payload = row[0]
    rows = payload if isinstance(payload, list) else [payload]
    return rows[offset:offset + limit]


def report_by_run(run_id, kind):
    """Load a saved version and its exact filters from Neon history."""
    schema()
    with _connect() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT p.payload,p.filters,r.model_version,r.completed_at "
            "FROM pl_analysis_reports p JOIN pl_analysis_runs r USING (run_id) "
            "WHERE p.run_id=%s AND p.report_name=%s AND r.status='complete' "
            "ORDER BY p.created_at DESC LIMIT 1",
            (run_id, kind),
        )
        row = cursor.fetchone()
    if not row:
        raise LookupError("Không có báo cáo đã lưu")
    payload, filters, model_version, completed_at = row
    return {"payload": payload, "filters": filters, "model_version": model_version,
            "completed_at": completed_at.isoformat() if completed_at else None}


def set_job(run_id, stage, status, details=None, error=None):
    schema()
    with _connect() as connection, connection.cursor() as cursor:
        cursor.execute("INSERT INTO pl_pipeline_jobs (run_id,stage,status,details,error,completed_at) VALUES (%s,%s,%s,%s,%s,CASE WHEN %s='running' THEN NULL ELSE now() END) ON CONFLICT (run_id,stage) DO UPDATE SET status=EXCLUDED.status,details=EXCLUDED.details,error=EXCLUDED.error,completed_at=EXCLUDED.completed_at", (run_id, stage, status, Jsonb(details or {}), error, status))


def save_user_analysis(kind, parameters, result, simulated=False):
    run_id = kind + "_" + uuid.uuid4().hex
    reports = [(kind, result, parameters or {})]
    publish_reports(run_id, "user-analysis", reports, {"is_simulated": bool(simulated)})
    return {**result, "run_id": run_id, "persistence": "saved"}


if __name__ == "__main__":
    schema()
    print("Neon analytics schema is ready", flush=True)
