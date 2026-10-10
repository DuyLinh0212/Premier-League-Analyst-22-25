"""Container acceptance checks. Run after bootstrap and Kafka DAG completion."""
import json
import math
import os
import urllib.request
from pathlib import Path
from match_outcome.graph import query

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
    counts = query("MATCH (r:AnalysisRun {analysis_run_id:'football_v3_seed'})-[:PRODUCED]->(n) RETURN labels(n)[0] AS label,count(n) AS count")
    assert {r['label']:r['count'] for r in counts} == {'MatchPrediction':1140,'KNNPrediction':20,'KNNNeighbor':100}
    incomplete = query("MATCH (r:AnalysisRun {project:'football-v3'}) WHERE r.status<>'complete' RETURN count(r) AS count")[0]['count']
    assert incomplete == 0
    report = {'bootstrap':bootstrap,'api_probability_sum':sum(p),'prediction':prediction,'neo4j_seed':counts,'incomplete_runs':incomplete}
    Path('/state/acceptance.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report))

if __name__ == '__main__':
    main()
