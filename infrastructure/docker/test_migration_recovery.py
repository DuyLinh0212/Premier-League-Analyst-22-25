"""Simulate an interrupted rename in an isolated HDFS test directory."""
import hashlib
import json
import os
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession
from infrastructure.scripts.build.migrate_blocks import migrate

def spark_session():
    return (SparkSession.builder.master('local[2]').appName('MigrationRecoveryAcceptance')
            .config('spark.driver.memory','512m').config('spark.ui.enabled','false')
            .config('spark.hadoop.fs.defaultFS',os.environ['HDFS_URI'])
            .config('spark.hadoop.dfs.blocksize','67108864')
            .config('spark.hadoop.parquet.block.size','67108864')
            .config('spark.hadoop.dfs.replication','2').getOrCreate())

def main():
    root='/bigdata/tests/migration_recovery'
    original=root+'/data.parquet'
    backup=root+'/.blocksize_backup/data.parquet.interrupted'
    report=Path('/state/migration_recovery_journal.json')
    spark=spark_session()
    spark.sparkContext.setLogLevel('WARN')
    try:
        jvm,conf=spark._jvm,spark._jsc.hadoopConfiguration()
        fs=jvm.org.apache.hadoop.fs.FileSystem.get(conf)
        path=jvm.org.apache.hadoop.fs.Path
        iterator=fs.listFiles(path('/bigdata/tests/parquet64'),True)
        while iterator.hasNext():
            source=iterator.next().getPath()
            if str(source).endswith('.parquet'):
                break
        fs.delete(path(root),True)
        fs.mkdirs(path(root))
        # Construct a real 128 MiB file with unchanged Parquet contents.
        jvm.org.apache.hadoop.io.IOUtils.copyBytes(fs.open(source),fs.create(path(original),False,65536,2,134217728),conf,True)
        assert int(fs.getFileStatus(path(original)).getBlockSize())==134217728
        stream=fs.open(path(original));buffer=jvm.java.io.ByteArrayOutputStream()
        jvm.org.apache.hadoop.io.IOUtils.copyBytes(stream,buffer,conf,True)
        sha=hashlib.sha256(bytes(buffer.toByteArray())).hexdigest()
        fs.mkdirs(path(root+'/.blocksize_backup'))
        assert fs.rename(path(original),path(backup))
        report.write_text(json.dumps({'files':[{'path':original,'backup':backup,'sha256':sha,'status':'swapping'}]}))
        assert not fs.exists(path(original))
    finally:
        spark.stop()
    result=migrate(root,report)
    assert result[0]['status']=='restored' and result[-1]['status']=='verified',result
    assert result[-1]['sha256']==sha and result[-1]['rows']==1
    spark=spark_session()
    try:
        fs=spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        assert int(fs.getFileStatus(spark._jvm.org.apache.hadoop.fs.Path(original)).getBlockSize())==67108864
        assert [(r.id,r.description) for r in spark.read.parquet(original).collect()]==[(1,'blocksize probe')]
    finally:
        spark.stop()
    print(json.dumps({'interrupted_source_restored':True,'sha256':sha,'rows':1,'final_blocksize':67108864}))

if __name__=='__main__':
    main()
