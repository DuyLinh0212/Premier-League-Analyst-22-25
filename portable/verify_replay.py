"""Assert every historical Kafka event was published once using the current model."""
import json
import os
from pathlib import Path
from match_outcome.graph import query
from match_outcome.inference import load_published_artifact

def main():
    version = load_published_artifact(Path(os.environ['MATCH_MODEL_DIR']))['model_version']
    counts = query("""MATCH (r:AnalysisRun)-[:PRODUCED]->(p:MatchPrediction)
        WHERE r.analysis_run_id STARTS WITH 'kafka_' AND r.status='complete'
          AND p.kind='historical' AND p.model_version=$version
        RETURN p.season AS season,count(p) AS count,count(DISTINCT p.key) AS unique_count""",{'version':version})
    assert {r['season']:r['count'] for r in counts}=={s:380 for s in ['2022-2023','2023-2024','2024-2025']},counts
    assert all(r['count']==r['unique_count'] for r in counts),counts
    incomplete=query("MATCH (r:AnalysisRun {project:'football-v3'}) WHERE r.status<>'complete' RETURN count(r) AS count")[0]['count']
    assert incomplete==0
    result={'historical_events':1140,'model_version':version,'seasons':counts,'incomplete_runs':incomplete}
    Path('/state/replay_acceptance.json').write_text(json.dumps(result,indent=2))
    print(json.dumps(result))

if __name__=='__main__':
    main()
