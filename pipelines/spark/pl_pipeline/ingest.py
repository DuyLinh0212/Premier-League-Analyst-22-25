"""Register the complete supplied dataset, without overwriting legacy raw data."""
import csv
import hashlib
import json
import os
import shutil
import subprocess
import csv
from datetime import date
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

from .core import ROOT, json_write, now


def preflight(ctx):
    files = list((ROOT / "data/bronze/fbref").rglob("*.csv")) + list((ROOT / "data/bronze/transfermarkt").glob("*.csv"))
    if len(files) != 585:
        raise RuntimeError(f"Expected 585 source CSVs, found {len(files)}")
    hdfs_uri = urlparse(ctx.uri)
    nn_uri = os.getenv("NAMENODE_HTTP_URI", f"http://{hdfs_uri.hostname}:9870").rstrip("/")
    with urlopen(nn_uri + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo", timeout=15) as response:
        node_info = json.load(response)["beans"][0]
    live_nodes = len(json.loads(node_info.get("LiveNodes", "{}")))
    if node_info.get("Safemode"):
        raise RuntimeError("HDFS NameNode is still in safe mode")
    required_nodes = int(os.getenv("HDFS_MIN_DATANODES", "3"))
    if live_nodes < required_nodes:
        raise RuntimeError(f"HDFS requires {required_nodes} live DataNodes; found {live_nodes}")
    result = ctx.hdfs("-ls", ctx.base, check=False)
    if result.returncode:
        ctx.hdfs("-mkdir", "-p", ctx.base)
    ctx.completed("preflight", {"source_files": len(files), "live_datanodes": live_nodes, "required_datanodes": required_nodes,
                                "hdfs_block_size": 67108864, "runtime": "Spark 3.5.1 / Java 17", "checked_at_utc": now()})


def _partition_transfermarkt(ctx, staging):
    """Keep immutable source CSVs and add season-partitioned Bronze extracts."""
    source = ROOT / "data/bronze/transfermarkt"
    seasons = {"2022": "2022-23", "2023": "2023-24", "2024": "2024-25"}
    game_season = {}
    with (source / "games.csv").open(encoding="utf-8-sig", newline="") as stream:
        for row in csv.DictReader(stream):
            season = seasons.get(str(row.get("season", "")))
            if row.get("competition_id") == "GB1" and season:
                game_season[row["game_id"]] = season
    local = ctx.local / "bronze_transfermarkt"
    if local.exists():
        shutil.rmtree(local)
    local.mkdir(parents=True)

    def partition_file(filename, season, predicate):
        src, dst = source / filename, local / season / filename
        if not src.exists():
            return
        dst.parent.mkdir(parents=True, exist_ok=True)
        with src.open(encoding="utf-8-sig", newline="") as input_stream, dst.open("w", encoding="utf-8-sig", newline="") as output_stream:
            reader = csv.DictReader(input_stream)
            writer = csv.DictWriter(output_stream, fieldnames=reader.fieldnames)
            writer.writeheader()
            writer.writerows(row for row in reader if predicate(row))

    for year, season in seasons.items():
        partition_file("games.csv", season, lambda row, y=year: row.get("competition_id") == "GB1" and row.get("season") == y)
        partition_file("appearances.csv", season, lambda row, s=season: game_season.get(row.get("game_id")) == s)
        first_year = int(year)
        start, end = date(first_year, 7, 1), date(first_year + 1, 7, 1)
        partition_file("transfers.csv", season, lambda row, a=start, b=end: bool(row.get("transfer_date")) and a <= date.fromisoformat(row["transfer_date"][:10]) < b)
        partition_file("player_valuations.csv", season, lambda row, a=start, b=end: bool(row.get("date")) and a <= date.fromisoformat(row["date"][:10]) < b)

    for season in seasons.values():
        season_dir = local / season
        destination = f"{staging}/transfermarkt/season={season}"
        ctx.hdfs("-mkdir", "-p", destination)
        files = sorted(season_dir.glob("*.csv")) if season_dir.exists() else []
        if files:
            ctx.hdfs("-put", *map(str, files), destination)


def register_raw_snapshot(ctx):
    ctx.require("preflight")
    entries = []
    for folder, prefix in [(ROOT / "data/bronze/fbref", "fbref"), (ROOT / "data/bronze/transfermarkt", "transfermarkt")]:
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
    target = f"{ctx.uri}{ctx.base}/bronze"
    inventory = {"dataset_id": dataset_id, "files": len(entries), "bytes": sum(e["bytes"] for e in entries),
                 "registered_at_utc": now(), "entries": entries}
    json_write(ctx.local / "source_inventory.json", inventory)
    ready = ctx.hdfs("-test", "-e", target + "/_READY", check=False).returncode == 0
    if ready:
        stored = json.loads(ctx.hdfs("-cat", target + "/manifest.json").stdout)
        if stored.get("dataset_id") != dataset_id:
            raise RuntimeError("Bronze path already contains a different immutable dataset; refusing to overwrite it")
    else:
        exists = ctx.hdfs("-test", "-e", target, check=False).returncode == 0
        if exists:
            raise RuntimeError("Bronze path exists without a READY marker; refusing to delete or overwrite existing HDFS data")
        staging = f"{ctx.uri}{ctx.base}/bronze_staging_{dataset_id}"
        ctx.hdfs("-rm", "-r", "-f", staging, check=False)
        ctx.hdfs("-mkdir", "-p", staging + "/fbref", staging + "/transfermarkt/_source")
        for folder in sorted((ROOT / "data/bronze/fbref").iterdir()):
            if not folder.is_dir():
                continue
            year = folder.name.split("-")[0]
            suffix = folder.name.split("-")[-1][-2:]
            season = year + "-" + suffix
            destination = f"{staging}/fbref/season={season}"
            ctx.hdfs("-mkdir", "-p", destination)
            contents = sorted(folder.iterdir())
            if contents:
                ctx.hdfs("-put", *map(str, contents), destination)
        tm_sources = sorted((ROOT / "data/bronze/transfermarkt").glob("*.csv"))
        if tm_sources:
            ctx.hdfs("-put", *map(str, tm_sources), staging + "/transfermarkt/_source")
        _partition_transfermarkt(ctx, staging)
        ctx.hdfs("-put", str(ctx.local / "source_inventory.json"), staging + "/manifest.json")
        ctx.hdfs("-touchz", staging + "/_READY")
        ctx.hdfs("-mv", staging, target)
    ctx.manifest.update(dataset_id=dataset_id, raw_snapshot_path=target, raw_files=len(entries), raw_bytes=inventory["bytes"])
    ctx.completed("register_raw_snapshot", {"dataset_id": dataset_id})
