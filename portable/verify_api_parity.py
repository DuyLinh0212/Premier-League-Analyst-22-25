"""HTTP probability check against the artifact validated against Spark on 1,140 rows."""
import csv
import json
import os
import urllib.request
from pathlib import Path

def main():
    seed=Path(__file__).parent/'seed/match_outcome'
    matches=[json.loads(line) for line in (seed/'matches.jsonl').read_text(encoding='utf-8').splitlines()]
    with (seed/'predictions.csv').open(encoding='utf-8-sig',newline='') as stream:
        expected={r['match_id']:r for r in csv.DictReader(stream)}
    errors=[]
    for index in [0,189,379,380,569,759,760,949,1138,1139]:
        row=matches[index]
        payload={k:row[k] for k in ['home_team','away_team','home_stats','away_stats']}
        request=urllib.request.Request(os.getenv('API_URL','http://127.0.0.1:8000')+'/predict',data=json.dumps(payload).encode(),headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=30) as response:
            result=json.load(response)
        reference=expected[row['match_id']]
        assert result['model_version']==reference['model_version']
        errors.extend(abs(result['probabilities'][k]-float(reference[k])) for k in ['home_win','draw','away_win'])
    error=max(errors)
    assert error<=1e-8,error
    report={'http_matches':10,'max_absolute_error':error,'spark_artifact_max_absolute_error':json.loads((seed/'metrics.json').read_text())['spark_numpy_max_absolute_error']}
    (Path(__file__).parent/'evidence/api_parity.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report))

if __name__=='__main__':
    main()
