"""Read-only acceptance checks for normalized HDFS files and canonical KNN."""
import csv
import json
import math
import os
from pathlib import Path
from pyspark.sql import SparkSession

def main():
    output = Path(os.environ['KNN_OUTPUT_DIR'])
    checks = {}
    with (output / 'knn_predictions.csv').open(encoding='utf-8-sig', newline='') as stream:
        predictions = list(csv.DictReader(stream))
    metrics = json.loads((output / 'knn_metrics.json').read_text(encoding='utf-8'))
    overall = metrics['overall']
    assert len(predictions) == 20, f"Expected 20 KNN club estimates, got {len(predictions)}"
    assert len({row['query_club_id'] for row in predictions}) == 20
    assert set(metrics['features']) == {'points_per_mp', 'goal_diff_per_match', 'recent_form', 'goals_for_per_match'}
    assert 'MinMaxScaler' in metrics['scaling']
    for metric in ('top4_accuracy', 'top4_precision', 'top4_recall', 'top4_f1', 'rank_mae', 'rank_rmse', 'rank_spearman'):
        assert overall.get(metric) is not None and math.isfinite(float(overall[metric])), f"Missing or invalid KNN metric: {metric}"
    checks['knn_model'] = {'prediction_rows':len(predictions),'evaluation_samples':overall['samples'],'features':metrics['features'],'scaling':metrics['scaling']}
    spark = (SparkSession.builder.master('local[2]').appName('HdfsAcceptance')
             .config('spark.driver.memory','512m').config('spark.ui.enabled','false')
             .config('spark.hadoop.fs.defaultFS',os.environ['HDFS_URI'])
             .config('spark.hadoop.dfs.blocksize','67108864').config('spark.hadoop.parquet.block.size','67108864').config('spark.hadoop.dfs.replication','2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        iterator = fs.listFiles(spark._jvm.org.apache.hadoop.fs.Path('/bigdata/processed'), True)
        files = []
        while iterator.hasNext():
            status = iterator.next()
            if '/.blocksize_backup/' in str(status.getPath()):
                continue
            files.append({'path':str(status.getPath()),'bytes':int(status.getLen()),'blocksize':int(status.getBlockSize()),'replication':int(status.getReplication())})
        live = fs.getDataNodeStats(spark._jvm.org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType.LIVE)
        assert len(live)==3
        checks.update(live_datanodes=len(live),hdfs_file_count=len(files),hdfs_files=files)
    finally:
        spark.stop()
    checks['invalid_files']=[f for f in checks['hdfs_files'] if f['blocksize']!=67108864 or f['replication']!=2]
    Path('/state/data_acceptance.json').write_text(json.dumps(checks,indent=2))
    print(json.dumps({k:v for k,v in checks.items() if k!='hdfs_files'}))
    assert not checks['invalid_files'],'HDFS metadata mismatch; inspect /state/data_acceptance.json'

if __name__=='__main__':
    main()
