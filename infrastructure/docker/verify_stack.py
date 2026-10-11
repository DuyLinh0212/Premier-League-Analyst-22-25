"""Container acceptance checks for HDFS, Neon, API and Kafka-backed predictions."""
import json
import math
import os
import urllib.request
from pathlib import Path
from analytics import neon_store

def get(path):
    with urllib.request.urlopen('http://api:8000' + path, timeout=30) as response:
        return json.load(response)

def main():
    bootstrap = json.loads(Path('/state/bootstrap.json').read_text())
    assert bootstrap['live_datanodes'] == 3, bootstrap
    assert bootstrap['fact_team_season'] == 60 and bootstrap['fact_team_match'] == 2280 and bootstrap['fact_match_enriched'] == 1140
    assert bootstrap['hdfs_files_64mib'] > 0
    matches = get('/matches')
    assert len(matches) == 1140 and len({r['match_id'] for r in matches}) == 1140
    assert len(get('/knn/predictions')) == 20 and len(get('/knn/neighbors/ARS')) == 5
    row = matches[0]
    payload = {k:row[k] for k in ['home_team','away_team','home_stats','away_stats']}
    request = urllib.request.Request('http://api:8000/predict', data=json.dumps(payload).encode(), headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(request,timeout=30) as response:
        prediction = json.load(response)
    p = list(prediction['probabilities'].values())
    assert all(math.isfinite(x) and 0 <= x <= 1 for x in p) and abs(sum(p)-1) < 1e-12
    saved = neon_store.report_by_run(prediction['run_id'], 'match_prediction')
    assert saved['payload']['probabilities'] == prediction['probabilities']
    with neon_store._connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM pl_analysis_runs WHERE status NOT IN ('complete','failed')")
        incomplete = cursor.fetchone()[0]
    assert incomplete == 0
    report = {'bootstrap':bootstrap,'api_probability_sum':sum(p),'prediction':prediction,'neon_saved_run':prediction['run_id'],'incomplete_runs':incomplete}
    Path('/state/acceptance.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report))

if __name__ == '__main__':
    main()
