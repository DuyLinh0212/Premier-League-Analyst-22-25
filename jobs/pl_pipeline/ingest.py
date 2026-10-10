"""Register the complete supplied dataset, without overwriting legacy raw data."""
import csv
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

from .core import ROOT, json_write, now


def preflight(ctx):
    files = list((ROOT / "FBref_Data").rglob("*.csv")) + list((ROOT / "Transfermark_Data").glob("*.csv"))
    if len(files) != 585:
        raise RuntimeError(f"Expected 585 source CSVs, found {len(files)}")
    result = ctx.hdfs("-ls", ctx.base, check=False)
    if result.returncode:
        ctx.hdfs("-mkdir", "-p", ctx.base)
    ctx.completed("preflight", {"source_files": len(files), "runtime": "Spark 3.5.1 / Java 17", "checked_at_utc": now()})


def register_raw_snapshot(ctx):
    ctx.require("preflight")
    entries = []
    for folder, prefix in [(ROOT / "FBref_Data", "fbref"), (ROOT / "Transfermark_Data", "transfermarkt")]:
        for path in sorted(folder.rglob("*.csv")):
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
            with path.open(encoding="utf-8-sig", newline="") as stream:
                reader = csv.reader(stream)
                headers = next(reader)
                rows = sum(1 for _ in reader)
            entries.append({"path": prefix + "/" + path.relative_to(folder).as_posix(), "bytes": path.stat().st_size,
                            "sha256": digest.hexdigest(), "physical_rows": rows, "columns": headers})
    dataset_id = "csv_" + hashlib.sha256(json.dumps(entries, sort_keys=True).encode()).hexdigest()[:16]
    target = f"{ctx.uri}{ctx.base}/raw/snapshots/{dataset_id}"
    inventory = {"dataset_id": dataset_id, "files": len(entries), "bytes": sum(e["bytes"] for e in entries),
                 "registered_at_utc": now(), "entries": entries}
    json_write(ctx.local / "source_inventory.json", inventory)
    if ctx.hdfs("-test", "-e", target + "/_READY", check=False).returncode:
        staging = target + "_staging"
        ctx.hdfs("-rm", "-r", "-f", staging, check=False)
        ctx.hdfs("-mkdir", "-p", staging)
        ctx.hdfs("-put", str(ROOT / "FBref_Data"), staging + "/fbref")
        ctx.hdfs("-put", str(ROOT / "Transfermark_Data"), staging + "/transfermarkt")
        ctx.hdfs("-put", str(ctx.local / "source_inventory.json"), staging + "/manifest.json")
        ctx.hdfs("-touchz", staging + "/_READY")
        ctx.hdfs("-mv", staging, target)
    ctx.manifest.update(dataset_id=dataset_id, raw_snapshot_path=target, raw_files=len(entries), raw_bytes=inventory["bytes"])
    ctx.completed("register_raw_snapshot", {"dataset_id": dataset_id})
