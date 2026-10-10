"""Prove Parquet's block-size hint cannot override the requested HDFS 64 MiB."""
import json
import os
from pathlib import Path
from pyspark.sql import SparkSession

def main():
    spark=(SparkSession.builder.master('local[2]').appName('ParquetBlockSizeAcceptance')
           .config('spark.driver.memory','512m').config('spark.ui.enabled','false')
           .config('spark.hadoop.fs.defaultFS',os.environ['HDFS_URI'])
           .config('spark.hadoop.dfs.blocksize','67108864')
           .config('spark.hadoop.parquet.block.size','67108864')
           .config('spark.hadoop.dfs.replication','2').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        target='/bigdata/tests/parquet64'
        spark.createDataFrame([(1,'blocksize probe')], 'id int,description string').coalesce(1).write.mode('overwrite').parquet(target)
        assert spark.read.parquet(target).count()==1
        fs=spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        iterator=fs.listFiles(spark._jvm.org.apache.hadoop.fs.Path(target),True)
        files=[]
        while iterator.hasNext():
            status=iterator.next()
            assert int(status.getBlockSize())==67108864 and int(status.getReplication())==2,str(status.getPath())
            files.append({'path':str(status.getPath()),'blocksize':int(status.getBlockSize()),'replication':int(status.getReplication())})
        Path('/state/parquet64_write.json').write_text(json.dumps(files,indent=2))
        print(json.dumps(files))
    finally:
        spark.stop()

if __name__=='__main__':
    main()
