"""Explicit source contracts and versioned analytical defaults."""

SEASONS = ("2022-2023", "2023-2024", "2024-2025")
LOGIC_VERSION = "football-v3.0.0"
RANDOM_SEED = 42

# These Transfermarkt IDs are verified against the supplied clubs.csv.
CLUBS = [
    ("ARS", "Arsenal", "11", ["arsenal", "arsenal fc"]),
    ("AVL", "Aston Villa", "405", ["aston villa"]),
    ("BOU", "Bournemouth", "989", ["bournemouth", "afc bournemouth"]),
    ("BRE", "Brentford", "1148", ["brentford", "brentford fc"]),
    ("BHA", "Brighton & Hove Albion", "1237", ["brighton", "brighton and hove albion"]),
    ("BUR", "Burnley", "1132", ["burnley", "burnley fc"]),
    ("CHE", "Chelsea", "631", ["chelsea", "chelsea fc"]),
    ("CRY", "Crystal Palace", "873", ["crystal palace"]),
    ("EVE", "Everton", "29", ["everton", "everton fc"]),
    ("FUL", "Fulham", "931", ["fulham", "fulham fc"]),
    ("IPS", "Ipswich Town", "677", ["ipswich town"]),
    ("LEE", "Leeds United", "399", ["leeds united"]),
    ("LEI", "Leicester City", "1003", ["leicester city"]),
    ("LIV", "Liverpool", "31", ["liverpool", "liverpool fc"]),
    ("LUT", "Luton Town", "1031", ["luton town"]),
    ("MCI", "Manchester City", "281", ["manchester city"]),
    ("MUN", "Manchester United", "985", ["manchester united", "manchester utd"]),
    ("NEW", "Newcastle United", "762", ["newcastle", "newcastle united", "newcastle utd"]),
    ("NFO", "Nottingham Forest", "703", ["nottingham", "nottingham forest"]),
    ("SHU", "Sheffield United", "350", ["sheffield united"]),
    ("SOU", "Southampton", "180", ["southampton", "southampton fc"]),
    ("TOT", "Tottenham Hotspur", "148", ["tottenham", "tottenham hotspur"]),
    ("WHU", "West Ham United", "379", ["west ham", "west ham united"]),
    ("WOL", "Wolverhampton Wanderers", "543", ["wolves", "wolverhampton wanderers"]),
]

# Alias is a name variant, not an inferred player ID. Birth-year and roster
# validation are still required before an automatic cross-source match.
PLAYER_ALIASES = {
    "andy robertson": "andrew robertson",
    "abdul fatawu issahaku": "abdul fatawu",
    "bobby de cordova reid": "bobby de cordova reid",
}


# Alias -> (source header, target Spark type). Metrics are namespaced before
# joining; notably GA_gk and GA_match must never overwrite one another.
PLAYER_CORE = {
    "matches_played": ("MP", "int"), "starts": ("Starts", "int"),
    "minutes": ("Min", "int"), "goals": ("Gls", "int"),
    "assists": ("Ast", "int"), "non_penalty_goals": ("G-PK", "int"),
    "penalties": ("PK", "int"), "penalties_attempted": ("PKatt", "int"),
    "yellow_cards": ("CrdY", "int"), "red_cards": ("CrdR", "int"),
}
SHOOTING = {
    "goals": ("Gls", "int"), "shots": ("Sh", "int"),
    "shots_on_target": ("SoT", "int"), "sot_pct": ("SoT%", "double"),
    "goals_per_shot": ("G/Sh", "double"), "goals_per_sot": ("G/SoT", "double"),
    "penalties": ("PK", "int"), "penalties_attempted": ("PKatt", "int"),
}
MISC = {
    "yellow_cards": ("CrdY", "int"), "red_cards": ("CrdR", "int"),
    "second_yellow_cards": ("2CrdY", "int"), "fouls_committed": ("Fls", "int"),
    "fouls_drawn": ("Fld", "int"), "offsides": ("Off", "int"),
    "crosses": ("Crs", "int"), "interceptions": ("Int", "int"),
    "tackles_won": ("TklW", "int"), "penalties_won": ("PKwon", "int"),
    "penalties_conceded": ("PKcon", "int"), "own_goals": ("OG", "int"),
}
GK = {
    "goals_against": ("GA", "int"), "goals_against_per90": ("GA90", "double"),
    "shots_on_target_against": ("SoTA", "int"), "saves": ("Saves", "int"),
    "save_pct": ("Save%", "double"), "clean_sheets": ("CS", "int"),
    "clean_sheet_pct": ("CS%", "double"), "matches_played": ("MP", "int"),
    "minutes": ("Min", "int"), "penalties_attempted": ("PKatt", "int"),
    "penalties_against": ("PKA", "int"), "penalties_saved": ("PKsv", "int"),
    "penalties_missed": ("PKm", "int"), "penalty_save_pct": ("PK_Save%", "double"),
}
PLAYING = {
    "matches_played": ("MP", "int"), "minutes": ("Min", "int"),
    "starts": ("Starts", "int"), "completed_matches": ("Compl", "int"),
    "substitutions": ("Subs", "int"), "unused_substitutes": ("unSub", "int"),
    "minutes_per_match": ("Mn/MP", "double"), "minutes_pct": ("Min%", "double"),
    "minutes_per_start": ("Mn/Start", "double"), "minutes_per_sub": ("Mn/Sub", "double"),
    "points_per_match": ("PPM", "double"), "on_goals": ("onG", "int"),
    "on_goals_against": ("onGA", "int"), "on_off": ("On-Off", "double"),
}
