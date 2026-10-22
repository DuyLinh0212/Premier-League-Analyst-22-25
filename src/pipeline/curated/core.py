"""Compatibility views read typed normalized Parquet, never old CSV snapshots."""
from functools import reduce
import json
from pathlib import PurePosixPath
import re
import unicodedata
from pyspark.sql import functions as F
from .config import CLUBS, SEASONS


def json_write(path, value):
    from src.ingest.runtime import atomic_json
    atomic_json(path, value)


def normalize(value):
    value = unicodedata.normalize("NFKD", str(value).replace("&", " and "))
    return re.sub(r"[^a-z0-9]+", " ", "".join(c for c in value if not unicodedata.combining(c)).lower()).strip()


_translation = [(chr(n), "".join(c for c in unicodedata.normalize("NFKD", chr(n)) if not unicodedata.combining(c)))
                for n in range(192, 592)]
_translation = [(a, b) for a, b in _translation if len(b) == 1 and b.isascii() and b.isalpha()]


def normalized(column):
    column = F.col(column) if isinstance(column, str) else column
    translated = F.translate(F.regexp_replace(column, "&", " and "),
                             "".join(a for a, _ in _translation), "".join(b for _, b in _translation))
    value = F.trim(F.regexp_replace(F.lower(translated), "[^a-z0-9]+", " "))
    return F.when(F.length(value) > 0, value)


def numeric(header, kind="double"):
    return F.regexp_replace(F.trim(F.col(f"`{header}`").cast("string")), r"[, %£€$+]", "").cast(kind)


def money(header, currency):
    return F.regexp_replace(F.regexp_extract(F.col(f"`{header}`"), currency + r"\s*([0-9][0-9,]*)", 1), ",", "").cast("double")


def club_views(spark):
    aliases = {(normalize(alias), cid) for cid, name, _, variants in CLUBS for alias in [name, *variants]}
    spark.createDataFrame(sorted(aliases), "club_name_key string, club_id string").createOrReplaceTempView("club_aliases")
    spark.createDataFrame([(cid, name, tm) for cid, name, tm, _ in CLUBS],
                          "club_id string, club_name string, tm_club_id string").createOrReplaceTempView("tracked_clubs")


def source_manifest(ctx, spark):
    if not hasattr(ctx, "normalized_manifest"):
        lines = spark.read.text(ctx.normalized + "/_metadata/normalization_manifest").collect()
        ctx.normalized_manifest = json.loads("\n".join(row.value for row in lines))["files"]
    return ctx.normalized_manifest


def source_frame(ctx, spark, item):
    frame = spark.read.parquet(ctx.normalized + "/" + item["output"])
    mapping = item["column_mapping"]
    # Money parsing retains the original raw string under a *_raw column.
    columns = [F.col("`" + target + "`").cast("string").alias(original)
               for original, target in mapping.items()]
    return frame.select(*columns).withColumn("source_file", F.lit(item["source"]))


def raw_fbref(ctx, spark, table, section, player=False, match=False):
    items = [item for item in source_manifest(ctx, spark)
             if item["source"].startswith("fbref/") and f"/{section}/" in item["source"]
             and PurePosixPath(item["source"]).name == table + ".csv"]
    if not items:
        raise RuntimeError(f"Thiếu nguồn Parquet FBref: {section}/{table}")
    frames = []
    for item in items:
        season = item["source"].split("/")[1]
        if season in SEASONS:
            frames.append(source_frame(ctx, spark, item).withColumn("season", F.lit(season)))
    df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
    if player:
        df = (df.filter(F.col("Player").isNotNull() & (F.col("Player") != "Player"))
              .withColumn("player_name", F.col("Player")).withColumn("name_key", normalized("Player"))
              .withColumn("club_name_key", normalized("Squad")))
    elif match:
        df = (df.withColumn("match_date", F.to_date("Date")).filter(F.col("match_date").isNotNull())
              .withColumn("club_name_key", normalized(F.regexp_extract("source_file", r"/04_teams/([^/]+)/", 1))))
    else:
        df = df.filter(F.col("Squad").isNotNull() & (F.col("Squad") != "Squad"))
        df = df.withColumn("club_name_key", normalized("Squad"))
    return df.join(spark.table("club_aliases"), "club_name_key", "left")


def raw_tm(ctx, spark, name):
    expected = "transfermarkt/appearances.csv" if name == "appearances" else f"transfermarkt/source_tables/{name}.csv"
    item = next((i for i in source_manifest(ctx, spark) if i["source"] == expected), None)
    if not item:
        raise RuntimeError(f"Thiếu nguồn Transfermarkt: {expected}")
    return source_frame(ctx, spark, item).drop("source_file")


def metric_columns(df, mapping, prefix=""):
    return [numeric(header, kind).alias(prefix + target) if header in df.columns
            else F.lit(None).cast(kind).alias(prefix + target) for target, (header, kind) in mapping.items()]
