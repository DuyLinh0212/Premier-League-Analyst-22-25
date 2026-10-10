"""Small Hadoop FileSystem CLI using Spark's existing Hadoop client jars."""
import os
import sys


def main(argv=None):
    from pyspark.sql import SparkSession
    argv = list(argv or sys.argv[1:])
    if argv and argv[0] == 'dfs':
        argv.pop(0)
    properties = {'dfs.blocksize':'67108864', 'dfs.replication':'2','dfs.client.use.datanode.hostname':'true'}
    for value in list(argv):
        if value.startswith('-D'):
            name, setting = value[2:].split('=', 1)
            properties[name] = setting
            argv.remove(value)
    command = argv.pop(0)
    recursive = '-r' in argv or '-R' in argv
    force = '-f' in argv
    if command != '-test':
        argv = [a for a in argv if a not in ('-r','-R','-f','-p')]
    builder = (SparkSession.builder.appName('HadoopClientCommand').master('local[1]')
               .config('spark.ui.enabled','false').config('spark.driver.memory','512m')
               .config('spark.hadoop.fs.defaultFS',os.environ['HDFS_URI']))
    for name, setting in properties.items():
        builder = builder.config('spark.hadoop.' + name, setting)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path
    try:
        if command == '-mkdir':
            for item in argv:
                if not fs.mkdirs(path(item)):
                    raise OSError('mkdir failed: ' + item)
        elif command == '-test':
            mode, target = argv
            present = fs.exists(path(target))
            if present and mode == '-d':
                present = fs.getFileStatus(path(target)).isDirectory()
            if present and mode == '-f':
                present = fs.getFileStatus(path(target)).isFile()
            return 0 if present else 1
        elif command == '-put':
            fs.copyFromLocalFile(False, force, path(argv[0]), path(argv[1]))
        elif command == '-get':
            fs.copyToLocalFile(False, path(argv[0]), path(argv[1]), True)
        elif command == '-mv':
            if not fs.rename(path(argv[0]), path(argv[1])):
                raise OSError('rename failed')
        elif command == '-rm':
            for item in argv:
                if not fs.delete(path(item), recursive) and not force:
                    raise FileNotFoundError(item)
        elif command == '-touchz':
            for item in argv:
                fs.create(path(item), False).close()
        elif command == '-ls':
            for status in fs.listStatus(path(argv[-1])):
                print(('d' if status.isDirectory() else '-') + ' ' + str(status.getLen()) + ' ' + str(status.getPath()))
        elif command == '-cat':
            for item in argv:
                incoming = fs.open(path(item))
                try:
                    print(spark._jvm.org.apache.commons.io.IOUtils.toString(incoming, 'UTF-8'), end='')
                finally:
                    incoming.close()
        else:
            raise ValueError('Unsupported Hadoop client command: ' + command)
        return 0
    finally:
        spark.stop()


if __name__ == '__main__':
    sys.exit(main())
