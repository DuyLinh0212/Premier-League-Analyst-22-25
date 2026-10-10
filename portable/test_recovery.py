"""Fault/replay acceptance helper, with service stops controlled from the host."""
import argparse
import json
import os
import uuid
from pathlib import Path
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from match_outcome.inference import load_artifact
from match_outcome.stream import process_events
from match_outcome.graph import query

STATE = Path(os.getenv('STATE_DIR','/state'))
OUTPUT = Path(os.environ['MATCH_MODEL_DIR'])

def event_file(name):
    return STATE / ('recovery_'+name+'.json')

def queue(name, duplicate=False):
    if duplicate:
        event = json.loads(event_file(name).read_text())
    else:
        row = json.loads((OUTPUT/'matches.jsonl').read_text(encoding='utf-8').splitlines()[0])
        event = {k:row[k] for k in ('home_team','away_team','home_stats','away_stats','home_name','away_name')}
        event.update(event_id=uuid.uuid4().hex,kind='scenario',schema_version=1,is_simulated=True,source='acceptance',model_version=load_artifact(OUTPUT)['model_version'])
        event_file(name).write_text(json.dumps(event))
    producer = KafkaProducer(bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],acks='all',value_serializer=lambda x:json.dumps(x).encode())
    producer.send('match_statistics',key=event['event_id'].encode(),value=event).get(timeout=30)
    producer.close()
    print(event['event_id'])

def fail_after_hdfs(name):
    event = json.loads(event_file(name).read_text())
    spark = (SparkSession.builder.master('local[2]').appName('RecoveryAcceptance').config('spark.driver.memory','512m')
             .config('spark.hadoop.fs.defaultFS',os.environ['HDFS_URI']).config('spark.hadoop.dfs.blocksize','67108864').config('spark.hadoop.parquet.block.size','67108864')
             .config('spark.hadoop.dfs.replication','2').config('spark.ui.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        try:
            process_events(spark,[event],OUTPUT,os.environ['HDFS_URI']+'/bigdata/processed/kafka')
        except Exception as exc:
            from urllib.error import URLError
            if not isinstance(exc,URLError):
                raise
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            path = spark._jvm.org.apache.hadoop.fs.Path('/bigdata/processed/kafka/facts/match_statistics/event_id='+event['event_id'])
            assert fs.exists(path),'HDFS must commit before the intentional Neo4j failure'
            (STATE/'recovery_partial.json').write_text(json.dumps({'event_id':event['event_id'],'hdfs_committed':True,'neo4j_failed':True}))
            print('Expected Neo4j connection failure after HDFS commit')
        else:
            raise AssertionError('Neo4j was expected to be offline')
    finally:
        spark.stop()

def verify(name):
    event = json.loads(event_file(name).read_text())
    key = event['event_id']+'_'+event['model_version']
    result = query("MATCH (p:MatchPrediction {key:$key})<-[:PRODUCED]-(r:AnalysisRun) RETURN count(p) AS count,collect(r.status) AS statuses",{'key':key})[0]
    assert result['count']==1 and result['statuses']==['complete'],result
    result.update(event_id=event['event_id'],model_version=event['model_version'])
    (STATE/('recovery_result_'+name+'.json')).write_text(json.dumps(result))
    print(json.dumps(result))

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('command',choices=['queue','duplicate','fail_after_hdfs','verify'])
    parser.add_argument('--name',default='neo4j')
    args=parser.parse_args()
    if args.command in {'queue','duplicate'}:
        queue(args.name,args.command=='duplicate')
    elif args.command=='fail_after_hdfs':
        fail_after_hdfs(args.name)
    else:
        verify(args.name)
