"""Read-only acceptance checks for normalized HDFS files and canonical KNN."""
import csv
import json
import os
from pathlib import Path
from pyspark.sql import SparkSession

def main():
    output = Path(os.environ['KNN_OUTPUT_DIR'])
    reference = Path('/opt/project/data/samples/pipeline-seed/knn')
    checks = {}
    for name, keys in [('knn_predictions.csv', ['query_season','query_club_id']),
                       ('knn_neighbors.csv', ['query_season','query_club_id','neighbor_order'])]:
        def read(folder):
            with (folder/name).open(encoding='utf-8-sig', newline='') as stream:
                return {tuple(row[k] for k in keys):row for row in csv.DictReader(stream)}
        expected, actual = read(reference), read(output)
        assert expected.keys() == actual.keys(), name
        for key, row in expected.items():
            assert row.keys() == actual[key].keys(), name
            for column, value in row.items():
                if column.endswith('_name'):
                    continue  # Canonical club IDs are unchanged; display aliases may expand.
                try:
                    assert abs(float(value)-float(actual[key][column])) <= 1e-6, (name,key,column)
                except ValueError:
                    assert value == actual[key][column], (name,key,column)
        checks[name] = {'rows':len(actual),'numeric_tolerance':1e-6,'equivalent':True}
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
