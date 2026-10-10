"""Typed, explicit-grain Silver facts for all 585 input files."""
from pyspark.sql import functions as F
from .config import PLAYER_CORE, SHOOTING, MISC, GK, PLAYING, SEASONS
from .core import club_views, raw_fbref, raw_tm, metric_columns, numeric, money, normalized
from .identity import keyed_player


def build_silver(ctx):
    ctx.require("build_identity")
    spark = ctx.session("silver")
    club_views(spark)
    fbref_tables = ["fact_player_season","fact_player_wage","fact_squad_wage","fact_team_season","fact_team_match","fact_match_enriched","fact_goal_event"]
    if not all("facts/"+name in ctx.manifest["tables"] for name in fbref_tables) or "audit/goal_against_mirror" not in ctx.manifest["tables"]:
        _build_fbref(ctx, spark)
    _build_transfermarkt(ctx, spark)
    ctx.completed("build_silver")
    spark.stop()


def _build_fbref(ctx, spark):
    keys = ["player_id", "club_id", "season"]
    standard = keyed_player(ctx, spark, "player_standard")
    core = standard.select(*keys, "player_name", numeric("Born", "int").alias("birth_year"),
                           F.regexp_extract("Pos", r"^(GK|DF|MF|FW)", 1).alias("position"),
                           numeric("Age").alias("age"), *metric_columns(standard, PLAYER_CORE))
    for table, mapping, prefix in [("shooting", SHOOTING, "shooting_"), ("miscellaneous", MISC, "misc_"),
                                    ("playing_time", PLAYING, "playing_"), ("gk", GK, "gk_")]:
        data = keyed_player(ctx, spark, "player_" + table)
        data = data.select(*keys, *metric_columns(data, mapping, prefix))
        data = ctx.table(data, "fact_player_" + table, keys=keys, sources=["player_" + table])
        core = core.join(data, keys, "left")
    ctx.table(core, "fact_player_season", keys=keys, sources=["player_standard", "player_shooting", "player_miscellaneous", "player_playing_time", "player_gk"])

    wage = raw_fbref(ctx, spark, "player_wage", "03_player", player=True)
    # Wage rows without a standard-stat row are retained, never coerced to zero.
    wb = ctx.read(spark, "bridge_fbref_player", "bridges").select("name_key", "club_id", "season", "player_id")
    wage = wage.join(wb, ["name_key", "club_id", "season"], "left").select("player_id", "name_key", "player_name", "club_id", "season",
        money("Annual Wages", "£").alias("annual_wage_gbp"), money("Annual Wages", "€").alias("annual_wage_eur"),
        money("Weekly Wages", "£").alias("weekly_wage_gbp"), F.col("Notes").alias("wage_notes"),
        F.lower(F.coalesce(F.col("Notes"), F.lit(""))).contains("estimate").alias("wage_estimated"))
    ctx.table(wage, "fact_player_wage", keys=["name_key", "club_id", "season"], sources=["player_wage"])

    league = raw_fbref(ctx, spark, "league_table", "01_league")
    team = league.select("club_id", "season", *metric_columns(league, {
        "rank": ("Rk", "int"), "matches_played": ("MP", "int"), "wins": ("W", "int"), "draws": ("D", "int"),
        "losses": ("L", "int"), "goals_for": ("GF", "int"), "goals_against": ("GA", "int"), "points": ("Pts", "int")}))
    for table, mapping, prefix in [("standard", {**PLAYER_CORE, "possession": ("Poss", "double"), "squad_age": ("Age", "double"), "players_used": ("# Pl", "int")}, "standard_"),
                                  ("shooting", SHOOTING, "shooting_"), ("miscellaneous", MISC, "misc_"),
                                  ("playing_time", PLAYING, "playing_"), ("gk", GK, "gk_")]:
        data = raw_fbref(ctx, spark, "squad_" + table, "02_squad")
        team = team.join(data.select("club_id", "season", *metric_columns(data, mapping, prefix)), ["club_id", "season"], "left")
    sw = raw_fbref(ctx, spark, "squad_wage", "02_squad").select("club_id", "season",
        money("Annual Wages", "£").alias("annual_wage_gbp"), money("Annual Wages", "€").alias("annual_wage_eur"),
        numeric("% Estimated").alias("wage_estimated_pct"))
    ctx.table(sw, "fact_squad_wage", keys=["club_id", "season"], sources=["squad_wage"])
    team = team.join(sw, ["club_id", "season"], "left").withColumn("earned_points", F.col("wins")*3 + F.col("draws")).withColumn("points_adjustment", F.col("points") - F.col("earned_points"))
    team = team.join(spark.table("tracked_clubs").select("club_id", "club_name"), "club_id", "left")
    team = team.withColumn("goal_diff", F.col("goals_for") - F.col("goals_against")).withColumn("points_per_mp", F.col("points") / F.col("matches_played"))
    ctx.table(team, "fact_team_season", keys=["club_id", "season"], sources=["league_table", "squad_standard", "squad_gk", "squad_shooting", "squad_miscellaneous", "squad_playing_time", "squad_wage"])

    fixtures = raw_fbref(ctx, spark, "score_fixtures", "04_teams", match=True)
    opponents = spark.table("club_aliases").select(F.col("club_name_key").alias("opponent_key"), F.col("club_id").alias("opponent_club_id"))
    fixtures = fixtures.withColumn("opponent_key", normalized("Opponent")).join(opponents, "opponent_key", "left")
    fixtures = fixtures.select("club_id", "season", "match_date", "opponent_club_id", F.col("Venue").alias("venue"),
        F.col("Result").alias("result"), numeric("GF_match", "int").alias("goals_for"), numeric("GA_match", "int").alias("goals_against"),
        numeric("Poss").alias("possession"), numeric("Attendance", "int").alias("attendance"), F.col("Formation").alias("formation"),
        F.col("Opp Formation").alias("opponent_formation"), F.col("Referee").alias("referee"))
    match_keys = ["club_id", "season", "match_date"]
    for table, mapping, prefix in [("shooting_for", SHOOTING, "shooting_"), ("shooting_against", SHOOTING, "opponent_shooting_"),
                                  ("miscellaneous_for", MISC, "misc_"), ("miscellaneous_against", MISC, "opponent_misc_"),
                                  ("gk_for", {**GK, "goals_against": ("GA_gk", "int")}, "gk_"),
                                  ("gk_against", {**GK, "goals_against": ("GA_gk", "int")}, "opponent_gk_")]:
        data = raw_fbref(ctx, spark, table, "04_teams", match=True)
        fixtures = fixtures.join(data.select(*match_keys, *metric_columns(data, mapping, prefix)), match_keys, "left")
    fixtures = fixtures.withColumn("home_club_id", F.when(F.col("venue") == "Home", F.col("club_id")).otherwise(F.col("opponent_club_id"))).withColumn(
        "away_club_id", F.when(F.col("venue") == "Away", F.col("club_id")).otherwise(F.col("opponent_club_id")))
    fixtures = fixtures.withColumn("match_id", F.concat_ws("_", "season", F.col("match_date").cast("string"), "home_club_id", "away_club_id"))
    games = raw_tm(ctx, spark, "games").filter("competition_id = 'GB1' AND season IN ('2022','2023','2024')")
    games = games.withColumn("season", F.concat_ws("-", "season", (F.col("season").cast("int")+1).cast("string"))).withColumn("match_date", F.to_date("date"))
    club_map = ctx.read(spark, "dim_club", "dimensions")
    for side in ["home", "away"]:
        games = games.join(club_map.select(F.col("tm_club_id").alias(side + "_tm_id"), F.col("club_id").alias(side + "_club_id_canonical")), games[side + "_club_id"] == F.col(side + "_tm_id"), "left")
    tm_games = games.select(F.col("game_id").alias("tm_game_id"), "season", "match_date",
        F.col("home_club_id_canonical").alias("home_club_id"), F.col("away_club_id_canonical").alias("away_club_id"),
        F.col("home_club_goals").cast("int").alias("tm_home_goals"), F.col("away_club_goals").cast("int").alias("tm_away_goals"),
        F.col("home_club_manager_name").alias("home_manager"), F.col("away_club_manager_name").alias("away_manager"), "stadium")
    fixtures = fixtures.join(tm_games, ["season", "match_date", "home_club_id", "away_club_id"], "left")
    fixtures = ctx.table(fixtures, "fact_team_match", keys=["match_id", "club_id"], sources=["score_fixtures", "shooting_for", "shooting_against", "gk_for", "gk_against", "miscellaneous_for", "miscellaneous_against", "games"])
    matches = fixtures.filter("venue = 'Home'").select("match_id", "tm_game_id", "season", "match_date", "home_club_id", "away_club_id",
        F.col("goals_for").alias("home_goals"), F.col("goals_against").alias("away_goals"), "tm_home_goals", "tm_away_goals", "home_manager", "away_manager", "stadium")
    metrics = [c for c in fixtures.columns if c.startswith(("shooting_", "gk_", "misc_")) or c in ["possession", "formation", "referee", "attendance"]]
    for venue, prefix in [("Home", "home_"), ("Away", "away_")]:
        side = fixtures.filter(F.col("venue")==venue).select("match_id", *[F.col(c).alias(prefix+c) for c in metrics])
        matches = matches.join(side, "match_id", "left")
    ctx.table(matches, "fact_match_enriched", keys=["match_id"], sources=["score_fixtures", "games", "shooting_for", "shooting_against", "gk_for", "gk_against", "miscellaneous_for", "miscellaneous_against"])

    # The against table is a mirror for auditing only, not a second event stream.
    for table in ["goals_scores", "goals_against"]:
        goals = raw_fbref(ctx, spark, table, "04_teams", match=True).withColumn("opponent_key", normalized("Opponent")).join(opponents, "opponent_key", "left")
        goals = goals.join(fixtures.select(*match_keys, "match_id"), match_keys, "left").select("match_id", "club_id", "opponent_club_id", "season", "match_date",
            F.col("Scorer").alias("scorer"), F.col("Assist").alias("assist"), F.col("Minute").alias("minute_label"),
            F.regexp_extract("Minute", r"^(\d+)", 1).cast("int").alias("minute"),
            F.regexp_extract("Minute", r"\+(\d+)", 1).cast("int").alias("stoppage_minutes"), F.col("Notes").alias("notes"))
        goals = goals.withColumn("is_own_goal", F.lower(F.coalesce("notes", F.lit(""))).rlike("own|og"))
        goals = goals.withColumn("event_id", F.sha2(F.concat_ws("|", "match_id", "club_id", "scorer", "minute_label", F.coalesce("notes", F.lit(""))), 256))
        ctx.table(goals, "fact_goal_event" if table == "goals_scores" else "goal_against_mirror", "facts" if table == "goals_scores" else "audit", keys=["event_id"], sources=[table])


def _build_transfermarkt(ctx, spark):
    club_map = ctx.read(spark, "dim_club", "dimensions")
    tm_games = raw_tm(ctx, spark, "games").filter("competition_id = 'GB1' AND season IN ('2022','2023','2024')").select(
        F.col("game_id").alias("tm_game_id"), F.concat_ws("-", "season", (F.col("season").cast("int")+1).cast("string")).alias("season"))
    tm_players = raw_tm(ctx, spark, "players").select(F.col("player_id").alias("tm_player_id"), F.col("name").alias("player_name"),
        F.to_date("date_of_birth").alias("birth_date"), F.col("position").alias("tm_position"), F.col("sub_position").alias("sub_position"))
    mapping = ctx.read(spark, "bridge_transfermarkt_player", "bridges")
    if all("facts/"+n in ctx.manifest["tables"] for n in ["fact_appearance","fact_transfer"]):
        appearances = ctx.read(spark, "fact_appearance")
        transfers = ctx.read(spark, "fact_transfer")
    else:
        appearances = raw_tm(ctx, spark, "appearances").join(tm_games.select(F.col("tm_game_id").alias("game_id"), "season"), "game_id")
        appearances = appearances.withColumnRenamed("player_id", "tm_player_id").join(mapping.select("tm_player_id", "player_id"), "tm_player_id", "left")
        appearances = appearances.join(club_map.select(F.col("tm_club_id").alias("player_club_id"), "club_id"), "player_club_id", "left")
        appearances = appearances.select("appearance_id", "tm_player_id", F.coalesce("player_id", F.concat(F.lit("tm:"), "tm_player_id")).alias("player_id"),
            "game_id", "club_id", "season", F.to_date("date").alias("appearance_date"), *[F.col(c).cast("int").alias(c) for c in ["minutes_played", "goals", "assists", "yellow_cards", "red_cards"]])
        appearances = ctx.table(appearances, "fact_appearance", keys=["appearance_id"], sources=["appearances", "games"])
        transfers = raw_tm(ctx, spark, "transfers").withColumnRenamed("player_id", "tm_player_id").withColumn("transfer_date", F.to_date("transfer_date"))
        transfers = transfers.filter("transfer_date >= DATE '2022-07-01' AND transfer_date <= DATE '2025-06-30'")
        for side in ["from", "to"]:
            transfers = transfers.join(club_map.select(F.col("tm_club_id").alias(side + "_tm_id"), F.col("club_id").alias(side + "_club_id_canonical")), transfers[side + "_club_id"] == F.col(side + "_tm_id"), "left")
            transfers = transfers.withColumn(side + "_club_id_canonical", F.coalesce(side + "_club_id_canonical", F.concat(F.lit("tm:"), F.col(side + "_club_id"))))
        transfers = transfers.filter(~F.col("from_club_id_canonical").startswith("tm:") | ~F.col("to_club_id_canonical").startswith("tm:"))
        transfers = transfers.join(mapping.select("tm_player_id", "player_id"), "tm_player_id", "left").join(tm_players.select("tm_player_id", "birth_date"), "tm_player_id", "left")
        transfers = transfers.select("tm_player_id", F.coalesce("player_id", F.concat(F.lit("tm:"), "tm_player_id")).alias("player_id"), "player_name", "birth_date", "transfer_date", "transfer_season",
            F.col("from_club_id_canonical").alias("from_club_id"), F.col("to_club_id_canonical").alias("to_club_id"), F.col("from_club_name"), F.col("to_club_name"),
            F.col("transfer_fee").cast("double").alias("fee_eur"), F.col("market_value_in_eur").cast("double").alias("listed_market_value_eur"))
        transfers = transfers.withColumn("season", F.concat_ws("-", F.when(F.month("transfer_date") >= 7, F.year("transfer_date")).otherwise(F.year("transfer_date")-1), F.when(F.month("transfer_date") >= 7, F.year("transfer_date")+1).otherwise(F.year("transfer_date"))))
        active = ctx.read(spark, "fact_team_season").select("club_id", "season")
        for side in ["from", "to"]:
            membership = active.select(F.col("club_id").alias(side+"_active_club"), F.col("season").alias(side+"_active_season"))
            transfers = transfers.join(membership, (transfers[side+"_club_id"]==membership[side+"_active_club"]) & (transfers["season"]==membership[side+"_active_season"]), "left")
            transfers = transfers.withColumn(side+"_active_pl_season", F.col(side+"_active_club").isNotNull()).drop(side+"_active_club", side+"_active_season")
        transfers = transfers.filter("from_active_pl_season OR to_active_pl_season")
        transfers = transfers.withColumn("fee_status", F.when(F.col("fee_eur").isNull(), "unknown").when(F.col("fee_eur") == 0, "zero_fee_type_unknown").otherwise("reported_positive"))
        transfers = transfers.withColumn("transfer_id", F.sha2(F.concat_ws("|", "tm_player_id", F.col("transfer_date").cast("string"), "from_club_id", "to_club_id"), 256))
        transfers = ctx.table(transfers, "fact_transfer", keys=["transfer_id"], sources=["transfers", "players", "clubs"])
    selected = appearances.select("tm_player_id").union(transfers.select("tm_player_id")).distinct()
    valuations = raw_tm(ctx, spark, "player_valuations").withColumnRenamed("player_id", "tm_player_id").join(selected, "tm_player_id")
    valuations = valuations.select("tm_player_id", F.to_date("date").alias("valuation_date"), F.col("market_value_in_eur").cast("double").alias("market_value_eur")).filter("valuation_date >= DATE '2021-07-01' AND valuation_date <= DATE '2025-06-30'")
    ctx.table(valuations, "fact_player_valuation", keys=["tm_player_id", "valuation_date"], sources=["player_valuations"])
    selected_tm = tm_players.join(selected, "tm_player_id").join(mapping.select("tm_player_id", "player_id"), "tm_player_id", "left").withColumn("player_id", F.coalesce("player_id", F.concat(F.lit("tm:"), "tm_player_id")))
    ctx.table(selected_tm, "dim_transfermarkt_player", "dimensions", keys=["tm_player_id"], sources=["players"])


def validate_silver(ctx, inject_failure=False):
    ctx.require("build_silver")
    spark = ctx.session("validate_silver")
    checks = []
    def check(name, actual, expected):
        checks.append({"check": name, "actual": actual, "expected": expected, "passed": actual == expected})
    for name, expected in [("fact_team_season", 60), ("fact_team_match", 2280), ("fact_match_enriched", 1140), ("fact_player_season", 1723), ("fact_goal_event", 3445)]:
        df = ctx.read(spark, name)
        check(name + "_rows", df.count(), expected)
        meta = ctx.manifest["tables"]["facts/" + name]
        check(name + "_duplicate_keys", df.groupBy(*meta["keys"]).count().filter("count > 1").count(), 0)
    matches = ctx.read(spark, "fact_match_enriched")
    for season in SEASONS:
        check(season + "_matches", matches.filter(F.col("season") == season).count(), 380)
    check("tm_match_unmatched", matches.filter("tm_game_id is null").count(), 0)
    check("score_disagreement", matches.filter("home_goals != tm_home_goals OR away_goals != tm_away_goals").count(), 0)
    players = ctx.read(spark, "fact_player_season")
    check("null_player_id", players.filter("player_id is null").count(), 0)
    check("player_reference_orphans", players.join(ctx.read(spark, "dim_player", "dimensions").select("player_id"), "player_id", "left_anti").count(), 0)
    check("null_club_id", ctx.read(spark, "fact_team_match").filter("club_id is null OR opponent_club_id is null").count(), 0)
    for season in SEASONS:
        check(season+"_club_seasons", ctx.read(spark, "fact_team_season").filter(F.col("season")==season).count(), 20)
    # Reconcile historical membership from the FBref bridge, not TM current_club.
    check("player_membership_orphans", players.join(ctx.read(spark, "bridge_fbref_player", "bridges").select("player_id", "club_id", "season"), ["player_id", "club_id", "season"], "left_anti").count(), 0)
    check("goal_mirror_count", ctx.read(spark, "goal_against_mirror", "audit").count(), 3445)
    events = ctx.read(spark, "fact_goal_event")
    mirror = ctx.read(spark, "goal_against_mirror", "audit").select("match_id", F.col("opponent_club_id").alias("club_id"), "scorer", "minute_label")
    check("goal_mirror_unmatched", events.join(mirror, ["match_id", "club_id", "scorer", "minute_label"], "left_anti").count(), 0)
    if inject_failure:
        check("intentional_failure_gate_test", 1, 0)
    from .core import json_write
    json_write(ctx.local / ("silver_quality_failure_test.json" if inject_failure else "silver_quality.json"), checks)
    spark.stop()
    failed = [c for c in checks if not c["passed"]]
    if failed:
        raise RuntimeError("Silver gate failed: " + str(failed))
    ctx.completed("validate_silver", {"checks": len(checks), "passed": True})


def publish_silver(ctx):
    ctx.require("validate_silver")
    ctx.hdfs("-touchz", ctx.silver + "/_VALIDATED")
    ctx.completed("publish_silver")
