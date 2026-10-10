"""FBref-based internal registry: no invented native FBref IDs or fuzzy auto-linking."""
from functools import reduce
from pyspark.sql import functions as F
from .config import PLAYER_ALIASES
from .core import club_views, metric_columns, normalized, raw_fbref, raw_tm


def player_source(ctx, spark, table):
    df = raw_fbref(ctx, spark, table, "03_player", player=True)
    if "Born" in df.columns:
        df = df.withColumn("birth_year", F.col("Born").cast("int"))
    else:
        df = df.withColumn("birth_year", F.lit(None).cast("int"))
    return df


def keyed_player(ctx, spark, table):
    df = player_source(ctx, spark, table)
    bridge = ctx.read(spark, "bridge_fbref_player", "bridges")
    return df.join(bridge.select("name_key", "club_id", "season", "player_id"), ["name_key", "club_id", "season"], "left")


def build_identity(ctx):
    ctx.require("register_raw_snapshot")
    spark = ctx.session("identity")
    club_views(spark)
    union = reduce(lambda a, b: a.unionByName(b), [
        player_source(ctx, spark, "player_" + table).select("name_key", "player_name", "birth_year", "club_id", "season")
        for table in ["standard", "shooting", "miscellaneous", "playing_time", "gk"]]).cache()
    if union.filter("club_id is null").count():
        raise RuntimeError("Unmapped FBref club; add a verified explicit alias")
    union.createOrReplaceTempView("player_source_union")
    registry = ctx.sql(spark, "player_registry", """
        SELECT concat('fbref:',substring(sha2(concat(name_key,'|',coalesce(cast(birth_year AS string),'unknown_birth')),256),1,24)) player_id,
               name_key, min(player_name) player_name, birth_year, 'fbref_profile_composite' id_basis,
               CASE WHEN birth_year IS NULL THEN 'provisional_missing_birth_no_cross_source_link' ELSE 'source_name_birth' END identity_status
        FROM player_source_union GROUP BY name_key,birth_year
    """)
    registry = ctx.table(registry, "dim_player", "dimensions", keys=["player_id"])
    left = union.select("name_key", "birth_year", "club_id", "season").distinct().alias("s")
    right = registry.alias("r")
    bridge = left.join(right, (F.col("s.name_key")==F.col("r.name_key")) & F.col("s.birth_year").eqNullSafe(F.col("r.birth_year"))).select("s.*", "r.player_id")
    bridge = ctx.table(bridge, "bridge_fbref_player", "bridges", keys=["name_key", "club_id", "season"])

    clubs = raw_tm(ctx, spark, "clubs").select(F.col("club_id").alias("tm_club_id"), F.col("name").alias("tm_club_name"))
    clubs = clubs.join(spark.table("tracked_clubs"), "tm_club_id", "full").select(
        F.coalesce("club_id", F.concat(F.lit("tm:"), "tm_club_id")).alias("club_id"), "tm_club_id",
        F.coalesce("club_name", "tm_club_name").alias("club_name"))
    ctx.table(clubs, "dim_club", "dimensions", keys=["club_id"])

    tm = raw_tm(ctx, spark, "players").select(F.col("player_id").alias("tm_player_id"), F.col("name").alias("tm_name"),
        normalized("name").alias("match_name"), F.year(F.to_date("date_of_birth")).alias("birth_year"))
    aliases = spark.createDataFrame(list(PLAYER_ALIASES.items()), "name_key string, alias_name string")
    registry = registry.join(aliases, "name_key", "left").withColumn("match_name", F.coalesce("alias_name", "name_key"))
    candidates = registry.join(tm, ["match_name", "birth_year"], "inner")
    unique = candidates.groupBy("player_id").count().filter("count = 1").select("player_id")
    candidates = candidates.join(unique, "player_id")
    games = raw_tm(ctx, spark, "games").filter("competition_id = 'GB1' AND season IN ('2022','2023','2024')").select("game_id", F.concat_ws("-", "season", (F.col("season").cast("int") + 1).cast("string")).alias("season"))
    roster = raw_tm(ctx, spark, "appearances").join(games, "game_id").select(F.col("player_id").alias("tm_player_id"), "season", F.col("player_club_id").alias("tm_club_id")).distinct()
    roster = roster.join(spark.table("tracked_clubs").select("tm_club_id", "club_id"), "tm_club_id")
    verified = candidates.join(roster, "tm_player_id").join(bridge.select("player_id", "season", "club_id"), ["player_id", "season", "club_id"]).select("player_id").distinct()
    mapping = candidates.join(verified, "player_id").select("player_id", "tm_player_id", "tm_name").withColumn("match_method", F.lit("unique_normalized_name_birth_year_and_historical_roster"))
    ctx.table(mapping, "bridge_transfermarkt_player", "bridges", keys=["player_id"])
    unmatched = ctx.read(spark, "dim_player", "dimensions").join(mapping.select("player_id"), "player_id", "left_anti").withColumn("review_status", F.lit("unmatched_no_auto_fuzzy"))
    ctx.table(unmatched, "identity_review", "audit", keys=["player_id"])
    ctx.completed("build_identity", {"players": ctx.manifest["tables"]["dimensions/dim_player"]["row_count"],
        "tm_matched": ctx.manifest["tables"]["bridges/bridge_transfermarkt_player"]["row_count"],
        "unmatched": ctx.manifest["tables"]["audit/identity_review"]["row_count"], "native_fbref_ids_present": False})
    spark.stop()
