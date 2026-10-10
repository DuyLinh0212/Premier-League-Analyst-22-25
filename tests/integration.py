"""Real Neo4j/Kafka/HTTP acceptance; CI HDFS sink is explicitly a local fixture."""
import json
import os
import tempfile
import urllib.request
from pathlib import Path
from analytics import store
from analytics.ingest import import_sources
from analytics.streaming import persist_events


def http(path, body=None, base='http://localhost:8000'):
    req = urllib.request.Request(base + path, data=json.dumps(body).encode() if body is not None else None,
                                 headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.load(response)


def main():
    first = import_sources()
    before = store.query('MATCH (n:PLRecord) WHERE n.table_key STARTS WITH $prefix RETURN count(n) AS count', {'prefix': first + ':'})[0]['count']
    assert import_sources() == first
    after = store.query('MATCH (n:PLRecord) WHERE n.table_key STARTS WITH $prefix RETURN count(n) AS count', {'prefix': first + ':'})[0]['count']
    assert before == after
    assert len(http('/matches')) == 1140
    assert len(http('/knn/predictions')) == 20
    assert len(http('/knn/neighbors/ARS')) == 5
    assert len(http('/players')) == 1350
    assert len({p['id'] for p in http('/players')}) == 1350
    assert len(http('/outlook?season=2024-2025&window=3')['players']) == 562
    phases = http('/phases')
    assert sum(len(v) for v in phases['seasons'].values()) == 60
    gmm = http('/rfm/gmm')
    assert gmm['method'] == 'Spark MLlib GaussianMixture'
    assert all(abs(sum(r['probability']) - 1) < 1e-8 for r in gmm['clubs'])
    assert http('/rfm/rules')['method'] != gmm['method']
    assert len(http('/transfers?season=2024-2025')['roi_sql']) == 20
    valuation = http('/valuation', {'age':22,'position':'FW','market_value_mil':50,'goals_assists':20,'minutes':2500,'tier':'tier2'})
    assert valuation['persistence'] == 'saved'
    assert http('/reports/' + valuation['run_id'] + '/valuation')[0]['payload']['estimated_fee_mil_eur'] == valuation['estimated_fee_mil_eur']
    prediction = http('/predict', {k:http('/matches')[0][k] for k in ['home_team','away_team','home_stats','away_stats']})
    assert prediction['persistence'] == 'saved' and abs(sum(prediction['probabilities'].values())-1) < 1e-10
    simulation = http('/transfers/simulate', {'batch_size':5,'season':'2025-2026','seed':42})
    events = simulation['records']
    with tempfile.TemporaryDirectory() as folder:
        def sink(event, checksum):
            path = Path(folder) / event['event_id']
            if path.exists() and path.read_text() != checksum:
                raise ValueError('Fixture event conflict')
            path.write_text(checksum)
        persist_events(events, sink)
        persist_events(events, sink)
    count = store.query('MATCH (n:PLEvent {namespace:$ns,kind:"transfer"}) RETURN count(n) AS count', {'ns':store.namespace()})[0]['count']
    assert count == 5
    # Real DB: partial and failed runs must not replace the previous complete report.
    old, _ = store.report('rfm_gmm')
    run = store.begin_run('integration_partial', first)
    store.save_report(run, 'rfm_gmm', {'invalid':True})
    try:
        store.finish_run(run, 2)
        raise AssertionError('Incomplete run was published')
    except RuntimeError:
        pass
    assert store.report('rfm_gmm')[0] == old
    store.fail_run(run, 'intentional recovery test')
    # Serving must be independent of local source/report files, even after API restart.
    root = Path('/opt/project/Generate_LeagueTable_Api/analystic/match_outcome/outputs')
    hidden = root.with_name('outputs_disabled_for_test')
    root.rename(hidden)
    try:
        assert len(http('/matches')) == 1140
        assert http('/model/metrics')
        assert http('/features')
        assert len(http('/knn/predictions')) == 20
    finally:
        hidden.rename(root)
    with urllib.request.urlopen('http://web:3000',timeout=60) as response:
        assert response.status == 200
    proxied = http('/api/football/outlook?season=2022-2023&window=1',base='http://web:3000')
    assert proxied['season'] == '2022-2023' and proxied['window'] == 1
    print(json.dumps({'status':'passed','source_records':after,'silver_rows':73075,'phases':240,'stream_events':count,'hdfs':'local fixture only; real HDFS tested separately'}))


if __name__ == '__main__':
    main()
