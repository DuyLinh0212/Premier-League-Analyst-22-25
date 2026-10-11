import hashlib
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from analytics.airflow_history import pipeline_id, snapshot, utc


class AirflowHistoryTest(unittest.TestCase):
    def test_retry_keeps_previous_attempt_and_filters_other_dags(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'airflow.db'
            with closing(sqlite3.connect(path)) as db, db:
                db.execute('CREATE TABLE dag_run (dag_id,run_id,state,start_date,end_date)')
                columns = 'dag_id,run_id,task_id,map_index,try_number,state,start_date,end_date'
                db.execute('CREATE TABLE task_instance (' + columns + ')')
                db.execute('CREATE TABLE task_instance_history (' + columns + ')')
                db.execute("INSERT INTO dag_run VALUES ('football_pipeline','run1','running','2026-10-11 10:00:00',NULL)")
                db.execute("INSERT INTO dag_run VALUES ('other_project','private','success',NULL,NULL)")
                db.execute("INSERT INTO task_instance_history VALUES ('football_pipeline','run1','preflight',-1,1,'failed','2026-10-11 10:00:00','2026-10-11 10:01:00')")
                db.execute("INSERT INTO task_instance VALUES ('football_pipeline','run1','preflight',-1,2,'running','2026-10-11 10:02:00',NULL)")
            original = path.read_bytes()
            runs, tasks, previous = snapshot(path)
            self.assertEqual([r['dag_id'] for r in runs], ['football_pipeline'])
            self.assertEqual((tasks[0]['try_number'], tasks[0]['state']), (2, 'running'))
            self.assertEqual((previous[0]['try_number'], previous[0]['state']), (1, 'failed'))
            self.assertEqual(path.read_bytes(), original)

    def test_metadata_time_and_id_match_runtime(self):
        raw = 'manual__2026-10-11T10:00:00+00:00'
        normalized = raw.replace(':','_').replace('+','_').replace('.','_')
        self.assertEqual(pipeline_id(raw), 'football_' + hashlib.sha256(normalized.encode()).hexdigest()[:24])
        self.assertEqual(utc('2026-10-11 10:00:00').isoformat(), '2026-10-11T10:00:00+00:00')
        self.assertIsNone(utc(None))


if __name__ == '__main__':
    unittest.main()
