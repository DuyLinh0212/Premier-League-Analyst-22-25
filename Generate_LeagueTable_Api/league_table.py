"""Deterministic, internally consistent Premier League season simulation."""

from __future__ import annotations

import csv
import hashlib
import io
import math
import random
import re
from pathlib import Path


CSV_COLUMNS = [
    "Rk", "Squad", "MP", "W", "D", "L", "GF", "GA", "GD", "Pts",
    "Pts/MP", "Attendance", "Top Team Scorer", "Goalkeeper", "Notes",
]
DEFAULT_BASELINE = (
    Path(__file__).resolve().parent.parent
    / "FBref_Data" / "2024-2025" / "01_league" / "league_table.csv"
)
SEASON_PATTERN = re.compile(r"^(\d{4})-(\d{4})$")


def validate_season(season: str) -> int:
    match = SEASON_PATTERN.fullmatch(season)
    if not match or int(match.group(2)) != int(match.group(1)) + 1:
        raise ValueError("season must have the form YYYY-YYYY with consecutive years")
    first_year = int(match.group(1))
    if first_year < 2025:
        raise ValueError("simulated seasons must start in 2025 or later")
    return first_year


def next_season(season: str) -> str:
    first_year = validate_season(season) + 1
    return f"{first_year}-{first_year + 1}"


def _read_baseline(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != CSV_COLUMNS:
            raise ValueError(f"Unexpected baseline CSV header: {reader.fieldnames}")
        rows = list(reader)
    if len(rows) != 20 or len({row["Squad"] for row in rows}) != 20:
        raise ValueError("Baseline must contain exactly 20 unique clubs")
    return rows


def _poisson(rng: random.Random, mean: float) -> int:
    # Knuth's algorithm is sufficient for the small goal rates used here.
    limit = math.exp(-mean)
    product = 1.0
    count = 0
    while product > limit:
        count += 1
        product *= rng.random()
    return count - 1


def _notes(rank: int) -> str:
    if rank <= 4:
        return "Champions League via league finish"
    if rank <= 6:
        return "Europa League via league finish"
    if rank == 7:
        return "Conference League via league finish"
    if rank >= 18:
        return "Relegated"
    return ""


def generate_csv(season: str, baseline_path: Path = DEFAULT_BASELINE) -> str:
    """Simulate 380 two-leg fixtures and return the historical 15-column CSV."""
    validate_season(season)
    baseline = _read_baseline(Path(baseline_path))
    seed = int.from_bytes(
        hashlib.sha256(f"league-table-v1:{season}".encode()).digest(), "big"
    )
    rng = random.Random(seed)
    stats = {
        row["Squad"]: {"MP": 0, "W": 0, "D": 0, "L": 0, "GF": 0, "GA": 0}
        for row in baseline
    }

    def play(home: dict[str, str], away: dict[str, str]) -> None:
        home_attack = float(home["GF"]) / 38 / 1.5
        away_attack = float(away["GF"]) / 38 / 1.5
        home_weakness = float(home["GA"]) / 38 / 1.5
        away_weakness = float(away["GA"]) / 38 / 1.5
        home_mean = max(0.2, min(3.8, 1.65 * home_attack * away_weakness))
        away_mean = max(0.2, min(3.8, 1.30 * away_attack * home_weakness))
        home_goals, away_goals = _poisson(rng, home_mean), _poisson(rng, away_mean)
        hs, vs = stats[home["Squad"]], stats[away["Squad"]]
        hs["MP"] += 1
        vs["MP"] += 1
        hs["GF"] += home_goals
        hs["GA"] += away_goals
        vs["GF"] += away_goals
        vs["GA"] += home_goals
        if home_goals > away_goals:
            hs["W"] += 1
            vs["L"] += 1
        elif home_goals < away_goals:
            vs["W"] += 1
            hs["L"] += 1
        else:
            hs["D"] += 1
            vs["D"] += 1

    for index, first in enumerate(baseline):
        for second in baseline[index + 1:]:
            play(first, second)
            play(second, first)

    ranked = sorted(
        baseline,
        key=lambda row: (
            -(3 * stats[row["Squad"]]["W"] + stats[row["Squad"]]["D"]),
            -(stats[row["Squad"]]["GF"] - stats[row["Squad"]]["GA"]),
            -stats[row["Squad"]]["GF"],
            row["Squad"],
        ),
    )
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=CSV_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for rank, source in enumerate(ranked, start=1):
        club = stats[source["Squad"]]
        points = 3 * club["W"] + club["D"]
        goal_diff = club["GF"] - club["GA"]
        writer.writerow({
            "Rk": rank, "Squad": source["Squad"], "MP": club["MP"],
            "W": club["W"], "D": club["D"], "L": club["L"],
            "GF": club["GF"], "GA": club["GA"],
            "GD": f"{goal_diff:+d}", "Pts": points,
            "Pts/MP": f"{points / club['MP']:.2f}",
            "Attendance": source["Attendance"],
            "Top Team Scorer": source["Top Team Scorer"],
            "Goalkeeper": source["Goalkeeper"], "Notes": _notes(rank),
        })
    return output.getvalue()


def parse_and_validate_csv(csv_text: str) -> list[dict[str, str]]:
    """Reject malformed or inconsistent snapshots before publishing/writing."""
    reader = csv.DictReader(io.StringIO(csv_text, newline=""))
    if reader.fieldnames != CSV_COLUMNS:
        raise ValueError("CSV header does not match league_table.csv")
    rows = list(reader)
    if len(rows) != 20 or len({row["Squad"] for row in rows}) != 20:
        raise ValueError("CSV must contain exactly 20 unique clubs")
    total_goals_for = total_goals_against = total_wins = total_losses = 0
    previous_key: tuple[int, int, int] | None = None
    for rank, row in enumerate(rows, start=1):
        if None in row or any(value is None for value in row.values()):
            raise ValueError("CSV contains missing or extra fields")
        try:
            actual_rank = int(row["Rk"])
            mp, wins, draws, losses = (int(row[name]) for name in ("MP", "W", "D", "L"))
            gf, ga, gd, points = (int(row[name]) for name in ("GF", "GA", "GD", "Pts"))
            points_per_mp = float(row["Pts/MP"])
        except (TypeError, ValueError) as exc:
            raise ValueError("CSV has an invalid numeric value") from exc
        if actual_rank != rank or mp != 38 or min(wins, draws, losses, gf, ga) < 0:
            raise ValueError("CSV has invalid rank, match count or negative statistics")
        if wins + draws + losses != mp or gd != gf - ga or points != 3 * wins + draws:
            raise ValueError("CSV has inconsistent team statistics")
        if not math.isfinite(points_per_mp) or abs(points_per_mp - round(points / mp, 2)) > 0.001:
            raise ValueError("CSV has inconsistent points per match")
        key = (points, gd, gf)
        if previous_key is not None and key > previous_key:
            raise ValueError("CSV is not sorted by points, goal difference and goals")
        previous_key = key
        total_goals_for += gf
        total_goals_against += ga
        total_wins += wins
        total_losses += losses
    if total_goals_for != total_goals_against or total_wins != total_losses:
        raise ValueError("CSV statistics are inconsistent across clubs")
    return rows
