"""Independent, small-output reconciliation against the original FBref rows."""
import csv
import hashlib
import json
from collections import defaultdict
from .config import CLUBS, SEASONS
from .core import ROOT, normalize, json_write, now


def audit_outputs(ctx):
    checks = []
    def check(name, actual, expected):
        checks.append({"check": name, "actual": actual, "expected": expected, "passed": actual == expected})
    def rows(table):
        return [json.loads(line) for line in (ctx.local / "tables" / table / "data.jsonl").read_text(encoding="utf-8").splitlines() if line]
    aliases = {normalize(alias): code for code, name, _, names in CLUBS for alias in [name, *names]}
    expected_members, expected_core = set(), set()
    for season in SEASONS:
        for kind in ["standard", "shooting", "miscellaneous", "playing_time", "gk"]:
            with (ROOT / "data/raw/fbref" / season / "03_player" / ("player_"+kind+".csv")).open(encoding="utf-8-sig", newline="") as file:
                for item in csv.DictReader(file):
                    if not item.get("Player") or item["Player"] == "Player":
                        continue
                    birth = item.get("Born") or "unknown_birth"
                    player_id = "fbref:" + hashlib.sha256((normalize(item["Player"])+"|"+birth).encode()).hexdigest()[:24]
                    membership = (player_id, aliases[normalize(item["Squad"])], season)
                    expected_members.add(membership)
                    if kind == "standard":
                        expected_core.add(membership)
    actual_members = {(r["player_id"],r["club_id"],r["season"]) for r in rows("bridge_fbref_player")}
    check("raw_FBref_membership_missing", len(expected_members-actual_members), 0)
    check("raw_FBref_membership_extra", len(actual_members-expected_members), 0)
    players = rows("player_season_performance")
    actual_core = {(r["player_id"],r["club_id"],r["season"]) for r in players}
    check("raw_standard_player_club_season_symmetric_difference", len(expected_core ^ actual_core), 0)
    tm = rows("bridge_transfermarkt_player")
    check("cross_source_duplicate_TM_ids", len(tm)-len({r["tm_player_id"] for r in tm}), 0)
    registry = rows("dim_player")
    provisional = {r["player_id"] for r in registry if r["birth_year"] is None}
    check("missing_birth_cross_source_links", len(provisional & {r["player_id"] for r in tm}), 0)
    active = {(r["club_id"],r["season"]) for r in rows("club_season_profile")}
    roi = {(r["transfer_id"],r["evaluation_year"]): r for r in rows("transfer_roi")}
    check("transfer_arrivals_not_in_active_PL_season", sum((r["club_id"],r["season"]) not in active for r in roi.values()), 0)
    check("nonpositive_unknown_fee_has_ROI_division", sum((r["fee_eur"] is None or r["fee_eur"] <= 0) and
        (r["minutes_per_million_eur"] is not None or r["goal_contributions_per_million_eur"] is not None) for r in roi.values()), 0)
    check("valuation_future_leakage", sum((r["before_valuation_date"] is not None and r["before_valuation_date"] > roi[(r["transfer_id"],r["evaluation_year"])]["transfer_date"]) or
        (r["after_valuation_date"] is not None and r["after_valuation_date"] > roi[(r["transfer_id"],r["evaluation_year"])]["evaluation_end"]) for r in rows("transfer_value_change")), 0)
    wages = rows("player_wage_efficiency")
    check("missing_wage_replaced_by_zero", sum(not r["wage_covered"] and (r["annual_wage_gbp"] is not None or r["wage_gbp_per_minute"] is not None) for r in wages), 0)
    check("similarity_self_links", sum(r["player_id"] == r["similar_player_id"] for r in rows("player_similarity")), 0)
    check("unsupported_role_inferred", sum(r["role"] in ["Deep playmaker", "Defensive fullback"] for r in rows("player_roles")), 0)
    for key in ctx.manifest["tables"]:
        if key.startswith("gold/"):
            data = rows(key.split("/")[1])
            check(key+"_numeric_coverage", sum(r["coverage_pct"] is not None and not isinstance(r["coverage_pct"], (int,float)) for r in data), 0)
    membership_groups = defaultdict(set)
    names = {r["player_id"]: r["player_name"] for r in registry}
    for pid, club, season in actual_members:
        membership_groups[(pid,season)].add(club)
    examples = [{"player_name": names[pid], "season": season, "clubs": sorted(clubs)} for (pid,season),clubs in sorted(membership_groups.items()) if len(clubs)>1]
    mapped = {r["player_id"] for r in tm}
    value = {"checked_at_utc": now(), "checks": checks, "passed": all(r["passed"] for r in checks),
        "multi_club_examples": examples, "core_cross_source_coverage_pct": 100*sum(r["player_id"] in mapped for r in players)/len(players),
        "core_matched_rows": sum(r["player_id"] in mapped for r in players), "core_rows": len(players)}
    json_write(ctx.local / "independent_audit.json", value)
    if not value["passed"]:
        raise RuntimeError("Independent source audit failed: " + str([c for c in checks if not c["passed"]]))
    print(f"Independent audit: {len(checks)} checks passed; raw FBref memberships match exactly", flush=True)
    return value
