"""HDFS-first transfer event consumer; Neon stores only the published analysis result."""
import argparse
import glob
import json
import os
from pathlib import Path
from . import codec


def persist_events(events, sink):
    prepared = {}
    for event in events:
        event_id = event.get('event_id', '')
        if len(event_id) != 32 or any(c not in '0123456789abcdef' for c in event_id):
            raise ValueError('Invalid transfer event ID')
        if event.get('schema_version') != 1 or not event.get('is_simulated'):
            raise ValueError('Transfer simulation requires schema v1 and simulation flag')
        checksum = codec.digest(event)
        if event_id in prepared and prepared[event_id][0] != checksum:
            raise ValueError('Conflicting transfer event in batch')
        prepared[event_id] = (checksum, event)
    # The HDFS sink owns the idempotency marker. Do not copy event/source rows
    # into Neon; API-published analysis results are already stored there.
    for checksum, event in prepared.values():
        sink(event, checksum)
    return len(prepared)


def consume():
    from pyspark.sql import SparkSession
    uri = os.environ['HDFS_URI']
    base = os.getenv('HDFS_BASE_PATH', '/bigdata').rstrip('/') + '/merged_stream'
    spark = (SparkSession.builder.appName('PremierLeagueTransferStreaming').master(os.getenv('SPARK_MASTER', 'local[2]'))
             .config('spark.hadoop.fs.defaultFS', uri).config('spark.hadoop.dfs.blocksize', '67108864').config('spark.hadoop.dfs.client.use.datanode.hostname','true').config('spark.hadoop.dfs.replication', '2')
             .config('spark.jars', ','.join(glob.glob(os.getenv('SPARK_EXTRA_JARS', '/opt/kafka-jars/*.jar'))))
             .config('spark.sql.shuffle.partitions', '2').config('spark.driver.memory', '512m').config('spark.ui.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path
    def sink(event, checksum):
        target = base + '/facts/transfers/event_id=' + event['event_id']
        marker = target + '/sha256_' + checksum
        if fs.exists(path(target)):
            if not fs.exists(path(marker)):
                raise ValueError('HDFS transfer event conflict')
            return
        staging = base + '/staging/' + event['event_id']
        spark.createDataFrame([(event['event_id'], event['season'], True, codec.encode(event))], 'event_id string,season string,is_simulated boolean,payload_json string').coalesce(1).write.mode('overwrite').parquet(staging)
        if spark.read.parquet(staging).count() != 1:
            raise RuntimeError('HDFS transfer read-back failed')
        fs.create(path(staging + '/sha256_' + checksum), False).close()
        fs.mkdirs(path(base + '/facts/transfers'))
        if not fs.rename(path(staging), path(target)):
            raise RuntimeError('HDFS transfer commit failed')
    try:
        frame = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', os.environ['KAFKA_BOOTSTRAP_SERVERS'])
                 .option('subscribe', os.getenv('TRANSFER_TOPIC', 'fact_transfers_topic')).option('startingOffsets', 'earliest').option('maxOffsetsPerTrigger', 100).load())
        def batch(frame, batch_id):
            events = [json.loads(r.payload) for r in frame.selectExpr('CAST(value AS STRING) AS payload').collect()]
            print('TRANSFER_BATCH', batch_id, persist_events(events, sink), flush=True)
        job = frame.writeStream.foreachBatch(batch).option('checkpointLocation', uri + base + '/checkpoints').trigger(availableNow=True).start()
        job.awaitTermination()
    finally:
        spark.stop()


if __name__ == '__main__':
    consume()
