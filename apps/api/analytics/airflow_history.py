"""Mirror local Airflow metadata, including retries and externally terminated tasks.

AIRFLOW_METADATA_SQLITE points to Airflow's own database. It is opened read-only;
Neon stores execution history separately from published report versions.
"""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import time

from psycopg.types.json import Jsonb
from . import neon_store


def pipeline_id(run_id):
    value = re.sub(r'[:+.]', '_', run_id)
    return 'football_' + hashlib.sha256(value.encode()).hexdigest()[:24]


def utc(value):
    if not value:
        return None
    stamp = datetime.fromisoformat(value) if isinstance(value, str) else value
    return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp


def snapshot(path):
    # Keep all three reads in one transaction so a retry cannot mix snapshots.
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        predicate = "dag_id IN ('football_pipeline','football_kafka_analytics')"
        runs = [dict(r) for r in db.execute('SELECT dag_id,run_id,state,start_date,end_date FROM dag_run WHERE ' + predicate)]
        columns = 'dag_id,run_id,task_id,map_index,try_number,state,start_date,end_date'
        tasks = [dict(r) for r in db.execute('SELECT ' + columns + ' FROM task_instance WHERE ' + predicate)]
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        attempts = [dict(r) for r in db.execute('SELECT ' + columns + ' FROM task_instance_history WHERE ' + predicate)] if 'task_instance_history' in tables else []
    return runs, tasks, attempts


def sync(path):
    runs, tasks, attempts = snapshot(path)
    neon_store.schema()
    with neon_store._connect() as conn, conn.cursor() as cursor:
        for row in runs:
            cursor.execute('''INSERT INTO pl_airflow_runs
                (dag_id,airflow_run_id,pipeline_run_id,state,started_at,completed_at)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (dag_id,airflow_run_id) DO UPDATE SET
                state=EXCLUDED.state,started_at=EXCLUDED.started_at,
                completed_at=EXCLUDED.completed_at,synced_at=now()''',
                (row['dag_id'],row['run_id'],pipeline_id(row['run_id']),row['state'],utc(row['start_date']),utc(row['end_date'])))
        for row in attempts + tasks:
            cursor.execute('''INSERT INTO pl_airflow_task_history
                (dag_id,airflow_run_id,task_id,map_index,try_number,state,started_at,completed_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (dag_id,airflow_run_id,task_id,map_index,try_number) DO UPDATE SET
                state=EXCLUDED.state,started_at=EXCLUDED.started_at,
                completed_at=EXCLUDED.completed_at,synced_at=now()''',
                (row['dag_id'],row['run_id'],row['task_id'],row['map_index'],row['try_number'],row['state'],utc(row['start_date']),utc(row['end_date'])))
        # Project authoritative current task state into the existing Web history.
        for row in tasks:
            if not row['start_date'] or row['map_index'] != -1:
                continue
            state = row['state']
            status = state or 'queued'
            details = {'airflow': {'dag_id': row['dag_id'], 'run_id': row['run_id'], 'state': state, 'try_number': row['try_number']}}
            error = ('Airflow task state: ' + state) if status in ('failed','upstream_failed','removed') else None
            cursor.execute('''INSERT INTO pl_pipeline_jobs
                (run_id,stage,status,started_at,completed_at,details,error)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (run_id,stage) DO UPDATE SET status=EXCLUDED.status,
                started_at=EXCLUDED.started_at,completed_at=EXCLUDED.completed_at,
                details=pl_pipeline_jobs.details || EXCLUDED.details,
                error=CASE WHEN EXCLUDED.status IN ('failed','upstream_failed','removed') THEN COALESCE(pl_pipeline_jobs.error,EXCLUDED.error) ELSE NULL END''',
                (pipeline_id(row['run_id']),row['task_id'],status,utc(row['start_date']),utc(row['end_date']),Jsonb(details),error))
    return {'runs': len(runs), 'tasks': len(tasks), 'previous_attempts': len(attempts)}


def history(limit=100):
    neon_store.schema()
    with neon_store._connect() as conn, conn.cursor() as cursor:
        cursor.execute('SELECT dag_id,airflow_run_id,pipeline_run_id,state,started_at,completed_at,synced_at FROM pl_airflow_runs ORDER BY started_at DESC NULLS LAST LIMIT %s', (limit,))
        names = [c.name for c in cursor.description]
        runs = [dict(zip(names,r)) for r in cursor.fetchall()]
        cursor.execute('SELECT dag_id,airflow_run_id,task_id,map_index,try_number,state,started_at,completed_at,synced_at FROM pl_airflow_task_history ORDER BY started_at DESC NULLS LAST LIMIT %s', (limit * 20,))
        names = [c.name for c in cursor.description]
        return {'runs': runs, 'tasks': [dict(zip(names,r)) for r in cursor.fetchall()]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sqlite', default=os.getenv('AIRFLOW_METADATA_SQLITE'))
    parser.add_argument('--interval', type=int, default=0)
    args = parser.parse_args()
    if not args.sqlite or args.interval < 0:
        parser.error('Provide the local Airflow SQLite path and a nonnegative interval.')
    while True:
        try:
            print(json.dumps(sync(args.sqlite)), flush=True)
        except Exception as exc:
            # Do not log DSNs or connection exception text that may contain secrets.
            print('Airflow history sync failed: ' + type(exc).__name__, flush=True)
            if not args.interval:
                raise SystemExit(1)
        if not args.interval:
            return
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
