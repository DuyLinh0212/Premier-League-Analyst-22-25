"""Assert each replayed historical Kafka event is stored once in Neon."""
import json
import os
from pathlib import Path
from match_outcome.inference import load_published_artifact
from analytics import neon_store

def main():
    version = load_published_artifact(Path(os.environ['MATCH_MODEL_DIR']))['model_version']
    neon_store.schema()
    with neon_store._connect() as connection, connection.cursor() as cursor:
        cursor.execute("""SELECT p.payload #>> '{request,season}' AS season, count(*) AS count,
                          count(DISTINCT p.payload #>> '{event_id}') AS unique_count
                   FROM pl_analysis_reports p JOIN pl_analysis_runs r USING(run_id)
                   WHERE p.report_name='match_prediction' AND r.status='complete'
                     AND p.model_version=%s AND p.payload #>> '{request,kind}'='historical'
                   GROUP BY season""", (version,))
        counts=[{'season':row[0],'count':row[1],'unique_count':row[2]} for row in cursor.fetchall()]
        cursor.execute("SELECT count(*) FROM pl_analysis_runs WHERE status NOT IN ('complete','failed')")
        incomplete=cursor.fetchone()[0]
    assert {r['season']:r['count'] for r in counts}=={s:380 for s in ['2022-2023','2023-2024','2024-2025']},counts
    assert all(r['count']==r['unique_count'] for r in counts),counts
    assert incomplete==0
    result={'historical_events':1140,'model_version':version,'seasons':counts,'incomplete_runs':incomplete}
    Path('/state/replay_acceptance.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result))

if __name__=='__main__':
    main()
