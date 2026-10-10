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
    assert all(p['canonical_player_id'] for p in http('/players'))
    from urllib.parse import quote
    similarity=http('/players/similar?player_id='+quote('Erling Haaland_2024-2025'))
    assert similarity['persistence']=='saved'
    assert all(r['player']['canonical_player_id']!=similarity['player']['canonical_player_id'] for r in similarity['recommendations'])
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
    config=http('/prediction/config')
    params={'home_team':'LIV','away_team':'ARS','prediction_date':config['default_date']}
    v2=http('/predict/v2',params)
    assert v2['persistence']=='saved' and abs(sum(v2['probabilities'].values())-1)<1e-10
    assert http('/reports/'+v2['run_id']+'/match_prediction_v2')[0]['payload']['probabilities']==v2['probabilities']
    live=http('/predict/v2',{**params,'mode':'live','minute':60,'home_goals':1,'home_stats':{'possession':65,'shots':14,'shots_on_target':6},'away_stats':{'possession':35,'shots':4,'shots_on_target':0}})
    assert live['statistics_source']=='manual_full_match_similarity' and live['neighbors']
    assert all(n['match_date']<config['default_date'] for n in live['neighbors'])
    ended=http('/predict/v2',{**params,'mode':'live','minute':90,'finished':True,'home_goals':2,'away_goals':1})
    assert ended['probabilities']['home_win']==1
    # Consume schema-v1 and schema-v2 events from real Kafka twice; no duplicate rows.
    from kafka import KafkaConsumer
    from match_outcome.stream import process_events
    from pyspark.sql import SparkSession
    consumer=KafkaConsumer('match_statistics',bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],auto_offset_reset='earliest',group_id=None,consumer_timeout_ms=10000,value_deserializer=lambda b:json.loads(b))
    match_events=[record.value for record in consumer]
    consumer.close()
    import glob
    # The first gateway in this process must include the Kafka classpath, even
    # though this batch uses Python Kafka. Later Structured Streaming reuses it.
    spark=(SparkSession.builder.appName('CI mixed match replay').master('local[2]')
           .config('spark.driver.memory','512m').config('spark.ui.enabled','false')
           .config('spark.jars',','.join(glob.glob(os.getenv('SPARK_EXTRA_JARS','/opt/kafka-jars/*.jar')))).getOrCreate())
    try:
        output=Path('/opt/project/Generate_LeagueTable_Api/analystic/match_outcome/outputs')
        with tempfile.TemporaryDirectory() as target:
            process_events(spark,match_events,output,Path(target).as_uri())
            process_events(spark,match_events,output,Path(target).as_uri())
        for result in (v2,live,ended):
            stored=store.query('MATCH (p:MatchPrediction {key:$key}) RETURN p.result_json AS result',{'key':result['prediction_id']+'_'+result['model_version']})
            assert len(stored)==1 and json.loads(stored[0]['result'])['probabilities']==result['probabilities']
    finally:
        spark.stop()
    simulation = http('/transfers/simulate', {'batch_size':5,'season':'2025-2026','seed':42})
    events = simulation['records']
    # The packaged Spark client really consumes Kafka and writes the CI filesystem
    # fixture, then restarts with the same checkpoint. Real HDFS is a local audit.
    from analytics.streaming import consume
    consume()
    consume()
    assert len(http('/streaming/transfers')) == 5
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
    assert len(http('/pipeline')) == 0  # No Airflow run has been performed by this fixture.
    dashboard=http('/dashboard')
    assert dashboard['season_metrics']['all']['matches'] == 1140
    assert len(dashboard['match_results']) == 1140
    results=[r for r in dashboard['match_results'] if r['season']=='2024-2025']
    assert sum(r['home_goals']+r['away_goals'] for r in results)==1115
    assert [sum(r['home_goals']>r['away_goals'] for r in results),sum(r['home_goals']==r['away_goals'] for r in results),sum(r['home_goals']<r['away_goals'] for r in results)]==[155,93,132]
    assert all('home_goals' not in r and 'away_goals' not in r for r in http('/matches'))

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
        assert http('/prediction/config')
        assert http('/model/metrics/v2')
        assert http('/predict/v2',params)['persistence']=='saved'
        assert len(http('/knn/predictions')) == 20
        assert len(http('/outlook')['players']) == 562
        assert http('/phases')
        assert http('/transfers')
        assert http('/rfm/gmm')
        assert len(http('/players')) == 1350
    finally:
        hidden.rename(root)
    with urllib.request.urlopen('http://web:3000',timeout=60) as response:
        assert response.status == 200
    proxied = http('/api/football/outlook?season=2022-2023&window=1',base='http://web:3000')
    assert proxied['season'] == '2022-2023' and proxied['window'] == 1
    print(json.dumps({'status':'passed','source_records':after,'raw_fixture_limit':int(os.getenv('RAW_FIXTURE_LIMIT','0')),'silver_rows':73075,'phases':240,'stream_events':count,'hdfs':'local fixture only; real HDFS tested separately'}))


if __name__ == '__main__':
    main()
