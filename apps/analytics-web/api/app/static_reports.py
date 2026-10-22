from __future__ import annotations

import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Callable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from app.queries import age_distribution, attack_defense, head_to_head, spending_vs_achievement


LOGGER = logging.getLogger(__name__)
REPORTS_DIR = Path(os.getenv("PLCA_REPO_ROOT", Path(__file__).resolve().parents[4])) / "evidence"
REPORT_VERSION = 1
REPORTS: dict[str, tuple[str, Callable[..., dict[str, Any]]]] = {
    "age-distribution": ("Phân bố độ tuổi", age_distribution.run),
    "head-to-head": ("Ma trận đối đầu", head_to_head.run),
    "attack-defense": ("Tấn công và phòng ngự", attack_defense.run),
    "spending-vs-achievement": ("Chi tiêu và thành tích", spending_vs_achievement.run),
}
_LOCK = threading.Lock()

INK = "#17243b"
MUTED = "#778397"
GRID = "#e5eaf0"
PINK = "#e83267"
GREEN = "#247a5a"
BLUE = "#4f77a8"


def _figure() -> tuple[Any, Any]:
    fig, ax = plt.subplots(figsize=(12, 6), dpi=150, facecolor="white")
    ax.set_facecolor("white")
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(GRID)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=MUTED, labelsize=9)
    return fig, ax


def _draw_age(result: dict[str, Any], season: str, path: Path) -> None:
    labels = ["<23", "23–27", "28–32", ">32"]
    counts = {row["age_band"]: row["player_count"] for row in result["data"]}
    values = [counts.get(label, 0) for label in labels]
    colors = [GREEN, BLUE, PINK, INK]
    fig, ax = _figure()
    bars = ax.bar(labels, values, color=colors, width=0.58)
    ax.set_title("Độ tuổi cầu thủ theo nhóm", loc="left", color=INK, fontsize=15, weight="bold", pad=14)
    ax.set_ylabel("Số cầu thủ duy nhất", color=MUTED)
    ax.set_xlabel("Nhóm tuổi", color=MUTED, labelpad=10)
    ax.bar_label(bars, padding=4, color=INK, fontsize=10, weight="bold")
    ax.set_ylim(0, max(values or [0]) * 1.18 + 1)
    fig.text(0.99, 0.015, f"HuTube PL • {season}", ha="right", color=MUTED, fontsize=8)
    fig.tight_layout(rect=(0.02, 0.04, 0.98, 0.98))
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def _draw_head_to_head(result: dict[str, Any], season: str, path: Path) -> None:
    teams = result.get("meta", {}).get("teams", [])
    ids = [team["club_id"] for team in teams]
    names = [team["club_name"] for team in teams]
    short = [name.replace("Manchester ", "Man ").replace("United", "Utd") for name in names]
    index = {club_id: position for position, club_id in enumerate(ids)}
    size = len(ids)
    wins = np.full((size, size), np.nan, dtype=float)
    draws = np.full((size, size), np.nan, dtype=float)
    losses = np.full((size, size), np.nan, dtype=float)
    for row in result["data"]:
        i = index.get(row["row_club_id"])
        j = index.get(row["column_club_id"])
        if i is not None and j is not None:
            wins[i, j] = row["wins"]
            draws[i, j] = row["draws"]
            losses[i, j] = row["losses"]

    fig, ax = plt.subplots(figsize=(16, 14), dpi=150, facecolor="white")
    image = ax.imshow(np.nan_to_num(wins, nan=0.0), cmap="YlOrRd", vmin=0, vmax=2, aspect="auto")
    ax.set_xticks(range(size), short, rotation=55, ha="right", fontsize=7, color=MUTED)
    ax.set_yticks(range(size), short, fontsize=7, color=MUTED)
    ax.set_xlabel("Đội ở cột", color=MUTED, labelpad=12)
    ax.set_ylabel("Đội ở hàng", color=MUTED, labelpad=12)
    ax.set_title(
        "Đối đầu trực tiếp • ô ghi Thắng–Hòa–Thua",
        loc="left",
        color=INK,
        fontsize=15,
        weight="bold",
        pad=16,
    )
    for i in range(size):
        for j in range(size):
            if i == j or np.isnan(wins[i, j]):
                continue
            text = f"{int(wins[i, j])}–{int(draws[i, j])}–{int(losses[i, j])}"
            ax.text(j, i, text, ha="center", va="center", fontsize=5.8, color=INK)
    colorbar = fig.colorbar(image, ax=ax, fraction=0.025, pad=0.025)
    colorbar.set_label("Số trận thắng của đội ở hàng", color=MUTED)
    ax.set_xticks(np.arange(-0.5, size, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, size, 1), minor=True)
    ax.grid(which="minor", color="white", linewidth=0.4)
    ax.tick_params(which="minor", bottom=False, left=False)
    fig.text(0.99, 0.012, f"HuTube PL • {season}", ha="right", color=MUTED, fontsize=8)
    fig.tight_layout(rect=(0.04, 0.04, 0.98, 0.98))
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def _draw_attack_defense(result: dict[str, Any], season: str, path: Path) -> None:
    fig, ax = _figure()
    rows = result["data"]
    for row in rows:
        x = row.get("goals_for_per_match")
        y = row.get("goals_against_per_match")
        if x is None or y is None:
            continue
        ax.scatter(x, y, s=54, color=PINK, edgecolor="white", linewidth=0.8, zorder=3)
        ax.annotate(
            row["club_name"],
            (x, y),
            xytext=(5, 5),
            textcoords="offset points",
            fontsize=7,
            color=INK,
        )
    ax.invert_yaxis()
    ax.set_title("Bàn thắng và bàn thua mỗi trận", loc="left", color=INK, fontsize=15, weight="bold", pad=14)
    ax.set_xlabel("Bàn thắng / trận  ·  cao hơn về bên phải", color=MUTED, labelpad=10)
    ax.set_ylabel("Bàn thua / trận  ·  thấp hơn ở phía trên", color=MUTED, labelpad=10)
    fig.text(0.99, 0.015, f"HuTube PL • {season}", ha="right", color=MUTED, fontsize=8)
    fig.tight_layout(rect=(0.02, 0.04, 0.98, 0.98))
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def _draw_spending(result: dict[str, Any], season: str, path: Path) -> None:
    fig, ax = _figure()
    eligible = [
        row for row in result["data"]
        if row.get("reported_spend_eur") is not None and row.get("league_rank") is not None
    ]
    for row in eligible:
        x = row["reported_spend_eur"] / 1_000_000.0
        y = row["league_rank"]
        ax.scatter(x, y, s=56, color=GREEN, edgecolor="white", linewidth=0.8, zorder=3)
        ax.annotate(
            row["club_name"],
            (x, y),
            xytext=(5, 4),
            textcoords="offset points",
            fontsize=7,
            color=INK,
        )
    ax.invert_yaxis()
    ax.set_title("Phí mua vào được báo cáo và thứ hạng cuối mùa", loc="left", color=INK, fontsize=15, weight="bold", pad=14)
    ax.set_xlabel("Tổng phí mua vào (triệu EUR)", color=MUTED, labelpad=10)
    ax.set_ylabel("Thứ hạng Premier League  ·  1 là cao nhất", color=MUTED, labelpad=10)
    rho = result.get("insight", {}).get("finding", "")
    fig.text(0.02, 0.015, rho, ha="left", color=MUTED, fontsize=8)
    fig.text(0.99, 0.015, f"HuTube PL • {season}", ha="right", color=MUTED, fontsize=8)
    fig.tight_layout(rect=(0.02, 0.06, 0.98, 0.98))
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


DRAWERS: dict[str, Callable[[dict[str, Any], str, Path], None]] = {
    "age-distribution": _draw_age,
    "head-to-head": _draw_head_to_head,
    "attack-defense": _draw_attack_defense,
    "spending-vs-achievement": _draw_spending,
}


def ensure_report(spark: Any, season: str, slug: str, dataset_id: str) -> dict[str, Any]:
    if hasattr(spark, "report"):
        return spark.report(season, slug)
    if slug not in REPORTS:
        raise KeyError(f"Báo cáo tĩnh không tồn tại: {slug}")
    if not season or not season.replace("-", "").isdigit():
        raise ValueError("Mùa giải không hợp lệ.")
    directory = REPORTS_DIR / season
    image_path = directory / f"{slug}.png"
    json_path = directory / f"{slug}.json"
    with _LOCK:
        if image_path.is_file() and json_path.is_file():
            try:
                cached = json.loads(json_path.read_text(encoding="utf-8"))
                if cached.get("dataset_id") == dataset_id and cached.get("report_version") == REPORT_VERSION:
                    return cached["report"]
            except (OSError, json.JSONDecodeError, KeyError):
                pass

        directory.mkdir(parents=True, exist_ok=True)
        _, query = REPORTS[slug]
        result = query(spark, season)
        DRAWERS[slug](result, season, image_path)
        report = {
            **result,
            "meta": {**result.get("meta", {}), "season": season, "dataset_id": dataset_id},
            "title": REPORTS[slug][0],
            "image_url": f"/reports/{season}/{slug}.png",
        }
        json_path.write_text(
            json.dumps(
                {"dataset_id": dataset_id, "report_version": REPORT_VERSION, "report": report},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return report


def build_all(spark: Any, seasons: list[str], dataset_id: str) -> dict[str, int]:
    built = 0
    failed = 0
    for season in seasons:
        for slug in REPORTS:
            try:
                ensure_report(spark, season, slug, dataset_id)
                built += 1
            except Exception:
                LOGGER.exception("Không tạo được báo cáo %s cho mùa %s", slug, season)
                failed += 1
    return {"built": built, "failed": failed}
