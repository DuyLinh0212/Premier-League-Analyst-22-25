"""Merge all nine team CSV families without dropping source parameters."""
import csv
import hashlib
import json
from pathlib import Path

from .schema import SOURCE_METRICS, LEAKAGE_HEADERS, numeric, validate_stats

FAMILIES = ("score_fixtures", "shooting_for", "shooting_against", "miscellaneous_for",
            "miscellaneous_against", "gk_for", "gk_against", "goals_scores", "goals_against")

def build_dataset(root):
    from pl_pipeline.config import CLUBS, SEASONS
    from pl_pipeline.core import normalize
    aliases = {normalize(alias): cid for cid, name, _, names in CLUBS for alias in [name, *names]}
    clubs = {cid: name for cid, name, _, _ in CLUBS}
    teams, catalog = {}, {}
    for season in SEASONS:
        for directory in sorted((Path(root) / "data/bronze/fbref" / season / "04_teams").iterdir()):
            if not directory.is_dir():
                continue
            cid = aliases[normalize(directory.name)]
            for family in FAMILIES:
                file = directory / (family + ".csv")
                with file.open(encoding="utf-8-sig", newline="") as stream:
                    reader = csv.DictReader(stream)
                    for header in reader.fieldnames:
                        key = family + "." + header
                        feature = SOURCE_METRICS.get(family, {}).get(header)
                        if family == 'score_fixtures' and header == 'Formation':
                            feature = 'formation'
                        role = "feature" if feature else "excluded" if header in LEAKAGE_HEADERS else "metadata"
                        if family == "score_fixtures" and header == "Result":
                            role = "label"
                        catalog[key] = {"source": family, "column": header, "role": role,
                                        "feature": feature, "reason": "Thống kê an toàn của từng đội" if feature else
                                        "Nhãn kết quả" if role == "label" else
                                        "Kết quả hoặc thông tin có thể tái dựng tỉ số" if role == "excluded" else
                                        "Metadata / bản đối chiếu / sự kiện, không đưa vào model"}
                    for row in reader:
                        # FBref goal files include footer/blank rows, retained in raw CSV.
                        if not row.get("Date") or not row.get("Opponent"):
                            continue
                        opponent = aliases[normalize(row["Opponent"])]
                        key = (season, row["Date"], cid, opponent)
                        team = teams.setdefault(key, {"club_id": cid, "opponent_id": opponent, "sources": {}})
                        if family.startswith("goals_"):
                            team["sources"].setdefault(family, []).append(row)
                        elif family in team["sources"]:
                            raise ValueError(f"Duplicate {family}: {key}")
                        else:
                            team["sources"][family] = row
    matches = []
    for (season, date, cid, oid), home in sorted(teams.items()):
        fixture = home["sources"].get("score_fixtures", {})
        if fixture.get("Venue") != "Home":
            continue
        away = teams[(season, date, oid, cid)]
        if away["sources"]["score_fixtures"]["Venue"] != "Away":
            raise ValueError("Missing mirrored away fixture")
        hscore, ascore = int(fixture["GF_match"]), int(fixture["GA_match"])
        mirror = away["sources"]["score_fixtures"]
        if (hscore, ascore) != (int(mirror["GA_match"]), int(mirror["GF_match"])):
            raise ValueError("Score mismatch")
        label = 0 if hscore > ascore else 1 if hscore == ascore else 2
        if fixture["Result"] != ("W", "D", "L")[label]:
            raise ValueError("Result mismatch")
        record = {"match_id": f"{season}_{date}_{cid}_{oid}", "season": season, "match_date": date,
                  "home_team": cid, "away_team": oid, "home_name": clubs[cid], "away_name": clubs[oid],
                  "label": label, "is_simulated": False, "source": "FBref", "schema_version": 1,
                  "home_goals": hscore, "away_goals": ascore}
        for side, team in [("home", home), ("away", away)]:
            stats = {}
            for family, mapping in SOURCE_METRICS.items():
                stats.update({target: numeric(team["sources"][family].get(column)) for column, target in mapping.items()})
            stats["formation"] = team["sources"]["score_fixtures"].get("Formation")
            record[side + "_stats"] = validate_stats(stats)
            record[side + "_source_parameters"] = team["sources"]
        matches.append(record)
    if len(matches) != 1140 or len({r["match_id"] for r in matches}) != 1140:
        raise ValueError(f"Expected 1140 unique matches, got {len(matches)}")
    for season in SEASONS:
        if sum(r["season"] == season for r in matches) != 380:
            raise ValueError(f"Incomplete season: {season}")
    fingerprint = hashlib.sha256(json.dumps(matches, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    for record in matches:
        record["event_id"] = hashlib.sha256((fingerprint + record["match_id"]).encode()).hexdigest()
        record["kind"] = "historical"
    return matches, list(catalog.values()), fingerprint

def write_dataset(root, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    rows, catalog, fingerprint = build_dataset(root)
    with (output / "matches.jsonl").open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
    (output / "feature_catalog.json").write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "dataset_manifest.json").write_text(json.dumps({"matches": len(rows), "fingerprint": fingerprint,
        "source_parameters_preserved": True, "seasons": {s: sum(r["season"] == s for r in rows) for s in sorted({r["season"] for r in rows})}}, indent=2), encoding="utf-8")
    return rows, fingerprint

def reconcile_silver(spark, rows, silver, output):
    """Verify every modeling input/label against normalization shared with KNN."""
    normalized = {r.match_id: r.asDict() for r in spark.read.parquet(silver.rstrip('/') + '/facts/fact_match_enriched').collect()}
    if set(normalized) != {r['match_id'] for r in rows}:
        raise ValueError('Silver match keys differ from the 1140 historical matches')
    metric_columns = {'possession': 'possession', 'shots': 'shooting_shots', 'shots_on_target': 'shooting_shots_on_target',
                      'shot_accuracy': 'shooting_sot_pct', 'penalties_attempted': 'shooting_penalties_attempted',
                      **{k: 'misc_' + k for k in SOURCE_METRICS['miscellaneous_for'].values()}}
    checked = 0
    for row in rows:
        record = normalized[row['match_id']]
        if (row['home_goals'], row['away_goals']) != (record['home_goals'], record['away_goals']):
            raise ValueError('Silver labels differ: ' + row['match_id'])
        for side in ('home', 'away'):
            stats = {k: record[side + '_' + col] for k, col in metric_columns.items()}
            stats['formation'] = record[side + '_formation']
            stats = validate_stats(stats)
            for key, value in stats.items():
                original = row[side + '_stats'][key]
                if original != value and (original is None or value is None or isinstance(value, str) or abs(original-value) > 1e-9):
                    raise ValueError(f'Silver feature mismatch: {row["match_id"]}/{side}/{key}')
                checked += 1
            row[side + '_stats'] = stats
    Path(output, 'silver_reconciliation.json').write_text(json.dumps({'path': silver, 'matches': len(rows),
        'checked_fields': checked, 'all_features_and_labels_equal': True}, indent=2), encoding='utf-8')
    return rows
