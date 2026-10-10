"""Rewrite HDFS files with 64 MiB blocks, verifying content and Parquet grain."""
import argparse
import json
import os
import uuid
from pathlib import Path

BLOCK_SIZE = 67108864

def migrate(root, report):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("BlocksizeMigration").master("local[2]")
             .config("spark.driver.memory", "512m")
             .config("spark.hadoop.fs.defaultFS", os.getenv("HDFS_URI", "hdfs://localhost:9000"))
             .config("spark.hadoop.dfs.blocksize", str(BLOCK_SIZE)).config("spark.hadoop.parquet.block.size", str(BLOCK_SIZE)).config("spark.hadoop.dfs.replication", "2")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    jvm, conf = spark._jvm, spark._jsc.hadoopConfiguration()
    fs, JPath = jvm.org.apache.hadoop.fs.FileSystem.get(conf), jvm.org.apache.hadoop.fs.Path
    def digest(path):
        sha = jvm.java.security.MessageDigest.getInstance("SHA-256")
        inp = jvm.java.security.DigestInputStream(fs.open(JPath(path)), sha)
        jvm.org.apache.hadoop.io.IOUtils.copyBytes(inp, jvm.java.io.OutputStream.nullOutputStream(), conf, True)
        return bytes(int(b) & 255 for b in sha.digest()).hex()
    def parquet_contract(path):
        # Footer row counts plus identical full-file SHA-256 verify every row
        # without launching thousands of redundant one-file Spark jobs.
        reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(
            jvm.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(JPath(path), conf))
        try:
            footer = reader.getFooter()
            blocks = footer.getBlocks()
            return str(footer.getFileMetaData().getSchema()), sum(int(blocks.get(i).getRowCount()) for i in range(blocks.size()))
        finally:
            reader.close()
    paths, rows = [], []
    report = Path(report)
    report.parent.mkdir(parents=True, exist_ok=True)
    if report.exists():
        previous = json.loads(report.read_text(encoding="utf-8"))
        rows = previous.get("files", [])
        for row in rows:
            if row.get("status") == "swapping" and not fs.exists(JPath(row["path"])) and fs.exists(JPath(row["backup"])):
                if not fs.rename(JPath(row["backup"]), JPath(row["path"])):
                    raise RuntimeError("Cannot recover interrupted migration")
                row['status']='restored'
            elif row.get('status')=='swapping' and fs.exists(JPath(row['path'])):
                if int(fs.getFileStatus(JPath(row['path'])).getBlockSize())==BLOCK_SIZE and digest(row['path'])==row['sha256']:
                    row['status']='verified'
                elif digest(row['path'])!=row['sha256']:
                    raise RuntimeError('Interrupted migration target has unexpected content')
    iterator = fs.listFiles(JPath(root), True)
    while iterator.hasNext():
        status = iterator.next()
        path = str(status.getPath())
        if "/.blocksize_backup/" not in path and ".__64m_" not in path:
            paths.append(path)
    def save(complete=False):
        temporary = report.with_suffix(".tmp")
        temporary.write_text(json.dumps({"root": root, "checked_files": len(paths), "files": rows, "all_64mib": complete}, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(report)
    try:
        for path in paths:
            blocksize = int(fs.getFileStatus(JPath(path)).getBlockSize())
            if blocksize == BLOCK_SIZE:
                if int(fs.getFileStatus(JPath(path)).getReplication()) != 2:
                    fs.setReplication(JPath(path), 2)
                continue
            sha = digest(path)
            token = uuid.uuid4().hex
            staging = path.rsplit('/',1)[0] + '/.' + path.rsplit('/',1)[1] + ".__64m_" + token
            backup = path.rsplit("/", 1)[0] + "/.blocksize_backup/" + path.rsplit("/", 1)[1] + "." + token
            evidence = {"path": path, "old_blocksize": blocksize, "new_blocksize": BLOCK_SIZE, "sha256": sha,
                        "backup": backup, "staging": staging, "status": "staging"}
            inp = fs.open(JPath(path))
            out = fs.create(JPath(staging), False, 65536, 2, BLOCK_SIZE)
            jvm.org.apache.hadoop.io.IOUtils.copyBytes(inp, out, conf, True)
            if digest(staging) != sha or int(fs.getFileStatus(JPath(staging)).getBlockSize()) != BLOCK_SIZE:
                fs.delete(JPath(staging), False)
                raise RuntimeError(f"Copy verification failed: {path}")
            if path.endswith(".parquet"):
                before, after = parquet_contract(path), parquet_contract(staging)
                if before != after:
                    raise RuntimeError(f"Parquet verification failed: {path}")
                evidence.update(rows=before[1], schema=before[0], verification="full-file SHA-256 and Parquet footer schema/row counts")
            evidence["status"] = "swapping"
            rows.append(evidence)
            save()
            fs.mkdirs(JPath(backup.rsplit("/", 1)[0]))
            if not fs.rename(JPath(path), JPath(backup)):
                raise RuntimeError("Backup rename failed")
            try:
                if not fs.rename(JPath(staging), JPath(path)):
                    raise RuntimeError("Replacement rename failed")
            except Exception:
                fs.rename(JPath(backup), JPath(path))
                raise
            evidence["status"] = "verified"
            save()
            print(f"64 MiB verified: {path}", flush=True)
        remaining = [p for p in paths if int(fs.getFileStatus(JPath(p)).getBlockSize()) != BLOCK_SIZE]
        if remaining:
            raise RuntimeError(f"Unconverted files: {remaining}")
        save(True)
        return rows
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="/bigdata/processed")
    parser.add_argument("--report", default="pipeline_runs/blocksize_migration.json")
    args = parser.parse_args()
    migrate(args.root, args.report)
