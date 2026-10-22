"""Export reproducible figures, complete tabular results and a comparison workbook."""
from __future__ import annotations

from collections import defaultdict
import csv
from itertools import combinations
import json
import logging
import math
from pathlib import Path
from xml.sax.saxutils import escape

from src.ingest.runtime import atomic_json

LOG = logging.getLogger(__name__)
TITLES = {"player-radar": "So sánh cầu thủ", "player-contribution": "Đóng góp cầu thủ",
          "squad-value": "Giá trị đội hình", "age-distribution": "Độ tuổi", "head-to-head": "Đối đầu",
          "matchday-performance": "Theo vòng", "attack-defense": "Tấn công và phòng ngự",
          "points-progression": "Diễn biến điểm", "team-strength": "Sức mạnh đội",
          "spending-vs-achievement": "Chi tiêu và thành tích"}


def diagram(path, title, nodes, edges, footer=""):
    """SVG nodes and edges come from the actual configured pipeline, not a screenshot."""
    width, height, step = 1120, 125 + len(nodes) * 82, 82
    boxes = []
    for i, (label, detail, status) in enumerate(nodes):
        color = {"success": "#147d57", "failed": "#b52d43", "running": "#b47c12"}.get(status, "#43556c")
        y = 70 + i * step
        boxes.append(f'<rect x="70" y="{y}" width="980" height="58" rx="8" fill="#f4f7fa" stroke="{color}"/>'
                     f'<text x="92" y="{y+23}" font-weight="bold" fill="{color}">{escape(label)}</text>'
                     f'<text x="92" y="{y+44}" font-size="13">{escape(detail)}</text>')
    lines = []
    for source, target in edges:
        lines.append(f'<path d="M560,{128+source*step} V{70+target*step}" stroke="#718096" stroke-width="2" marker-end="url(#arrow)"/>')
    path.write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">'
                    '<defs><marker id="arrow" markerWidth="7" markerHeight="7" refX="6" refY="3" orient="auto"><path d="M0,0 L6,3 L0,6" fill="#718096"/></marker></defs>'
                    '<rect width="100%" height="100%" fill="white"/><g font-family="DejaVu Sans,Arial,sans-serif" font-size="17" fill="#24344a">'
                    f'<text x="70" y="37" font-size="22" font-weight="bold">{escape(title)}</text>' + "".join(lines + boxes) +
                    f'<text x="70" y="{height-18}" font-size="12">{escape(footer)}</text></g></svg>', encoding="utf-8")


def diagrams(ctx):
    from .stages import STAGES
    directory = ctx.local / "diagrams"
    directory.mkdir(exist_ok=True)
    nodes = [(stage, "Trạng thái: " + ctx.manifest["stages"].get(stage, {}).get("status", "đang chờ"),
              ctx.manifest["stages"].get(stage, {}).get("status")) for stage in STAGES]
    diagram(directory / "dag.svg", "DAG premier_league_reports", nodes,
            [(i, i+1) for i in range(len(nodes)-1)], "run_id=" + ctx.run_id + " · Trạng thái tại thời điểm xuất")
    nodes = [("Nguồn lịch sử", "586 file, FBref / Transfermarkt / DataMB, ba mùa 2022–2025", None),
             ("HDFS Bronze", ctx.bronze + " · SHA-256 đối chiếu từng file", None),
             ("HDFS Silver Parquet", ctx.silver + " · 21 bảng và tm_games", None),
             ("Spark SQL · 10 phân tích", "Giữ NULL; đối chiếu điểm chính thức và điểm thi đấu", None),
             ("HDFS golde theo ngày", ctx.gold, None),
             ("PostgreSQL", "Snapshot riêng theo run_id; công bố active_run_id trong giao dịch", None),
             ("Web và artifact CI/CD", "API chỉ đọc PostgreSQL · PNG, Excel, JSON, log và ảnh minh chứng", None)]
    diagram(directory / "architecture.svg", "Kiến trúc Airflow → HDFS → PostgreSQL → Web", nodes,
            [(i, i+1) for i in range(len(nodes)-1)], "Điều phối bằng Airflow scheduler trong WSL; không Docker")


def plot(result, slug, season, path, config):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    rows = result["data"]
    fig, ax = plt.subplots(figsize=(12, 6), dpi=150, facecolor="white")
    if slug in ("player-radar", "team-strength"):
        plt.close(fig)
        fig, ax = plt.subplots(figsize=(10, 7), dpi=150, subplot_kw={"projection": "polar"})
        axes = result["meta"]["axes"]
        angles = np.linspace(0, 2*np.pi, len(axes), endpoint=False).tolist()
        if slug == "player-radar":
            selected = sorted(rows, key=lambda row: (-(row.get("minutes") or 0), row["selection_id"]))[:2]
        else:
            selected = [row for row in rows if row["club_id"] in config["example_clubs"]]
        for row in selected:
            values = [row.get(("pct_" if slug == "player-radar" else "") + axis["key"]) for axis in axes]
            values = [np.nan if v is None else v for v in values]
            ax.plot(angles + angles[:1], values + values[:1], label=row.get("player_name", row.get("club_name")))
        ax.set_xticks(angles, [axis["label"] for axis in axes])
        ax.tick_params(axis="x", pad=14, labelsize=10)
        ax.set_ylim(0, 100)
        ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.24), ncol=2)
    elif slug in ("matchday-performance", "points-progression"):
        groups = defaultdict(list)
        for row in rows:
            groups[row.get("season", season)].append(row)
        for label, group in sorted(groups.items()):
            group.sort(key=lambda row: row["matchday"])
            ax.plot([row["matchday"] for row in group], [row["cumulative_points"] for row in group], label=label)
        ax.set_xlabel("Vòng đấu"); ax.set_ylabel("Điểm tích lũy từ kết quả thi đấu")
        ax.legend()
    elif slug == "head-to-head":
        group = sorted(rows, key=lambda row: row["opponent_name"])
        labels = [row["opponent_name"] for row in group]
        left = np.zeros(len(group))
        for field, label, color in [("wins", "Thắng", "#247a5a"), ("draws", "Hòa", "#b4becb"), ("losses", "Thua", "#e83267")]:
            values = [row[field] for row in group]
            ax.barh(labels, values, left=left, label=label, color=color)
            left += np.array(values)
        ax.legend(); ax.tick_params(axis="y", labelsize=8)
    else:
        metric = "goal_contributions" if slug == "player-contribution" else "market_value_eur"
        group = [row for row in rows if row.get(metric) is not None and
                 (slug != "squad-value" or row.get("club_id") == config["example_club"])]
        group = sorted(group, key=lambda row: (-row[metric], row["player_name"]))[:25]
        ax.barh([row["player_name"] for row in reversed(group)], [row[metric] for row in reversed(group)], color="#247a5a")
        ax.set_xlabel("Bàn thắng + kiến tạo" if metric == "goal_contributions" else "Định giá được báo cáo (EUR)")
        ax.tick_params(axis="y", labelsize=8)
    subtitle = "\nPercentile 0–100 trong cùng mùa và cùng nhóm tham chiếu" if slug in ("player-radar", "team-strength") else ""
    if slug == "points-progression":
        heading = TITLES[slug] + " · " + result["meta"].get("club_name", config["example_club"]) + " · so sánh ba mùa"
    else:
        heading = TITLES[slug] + " · " + season
    ax.set_title(heading + subtitle, pad=25)
    fig.text(0.02, 0.015, "Nguồn: Silver Parquet trên HDFS · NULL thể hiện dữ liệu thiếu · lựa chọn minh họa tái lập", fontsize=8)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    if not path.is_file() or path.stat().st_size < 5000:
        raise RuntimeError("Biểu đồ không xuất được: " + str(path))


def workbook(ctx, index, tables):
    import xlsxwriter
    from xlsxwriter.utility import xl_rowcol_to_cell
    directory = ctx.local / "excel"
    directory.mkdir(exist_ok=True)
    path = directory / "so_sanh_3_mua.xlsx"
    with xlsxwriter.Workbook(path, {"strings_to_formulas": False, "strings_to_urls": False}) as book:
        book.set_properties({"title": "Phân tích Premier League 2022–2025", "comments": "run_id=" + ctx.run_id})
        title = book.add_format({"font_name": "Arial", "font_size": 15, "bold": True, "font_color": "#173c55"})
        head = book.add_format({"font_name": "Arial", "bold": True, "bg_color": "#173c55", "font_color": "white", "text_wrap": True})
        cell = book.add_format({"font_name": "Arial", "font_size": 10, "valign": "top"})
        number = book.add_format({"font_name": "Arial", "font_size": 10, "num_format": "#,##0.00;[Red](#,##0.00);0"})
        sheets = {name: book.add_worksheet(name) for name in
                  ("Tổng quan", "Đội bóng", "Cầu thủ", "Giá trị", "Theo vòng", "Đối đầu", "Độ tuổi", "Chi tiêu", "Chỉ số radar", "Đối soát")}
        pointers = defaultdict(int)
        for name, sheet in sheets.items():
            sheet.write(0, 0, name + " · " + ctx.run_id, title)
            sheet.write(1, 0, "Nguồn: " + ctx.gold, cell)
            sheet.write(2, 0, "Ô trống = thiếu dữ liệu hoặc CLB không tham gia mùa đó; không thay bằng 0.", cell)
            sheet.freeze_panes(5, 2); sheet.set_column(0, 0, 26); sheet.set_column(1, 250, 20)
            sheet.set_landscape(); sheet.fit_to_pages(1, 0)
            pointers[name] = 4

        def section(sheet_name, caption, rows):
            sheet = sheets[sheet_name]
            start = pointers[sheet_name]
            sheet.write(start, 0, caption, title)
            rows = list(rows)
            columns = list(dict.fromkeys(key for row in rows for key in row)) or ["Ghi chú"]
            sheet.write_row(start + 1, 0, columns, head)
            for i, row in enumerate(rows, start + 2):
                for j, name in enumerate(columns):
                    value = row.get(name)
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value, ensure_ascii=False)
                    if value is None:
                        sheet.write_blank(i, j, None, cell)
                    elif isinstance(value, (float, int)) and not isinstance(value, bool):
                        sheet.write_number(i, j, value, number)
                    else:
                        sheet.write(i, j, value, cell)
            pointers[sheet_name] = start + 4 + len(rows)
            return start + 2, columns

        section("Tổng quan", "Thông tin lần chạy", [{"run_id": ctx.run_id, "ngày_báo_cáo": ctx.manifest["report_date"],
                "commit_sha": ctx.manifest["commit_sha"], "số_file": len(json.loads((ctx.local / "source_manifest.json").read_text())),
                "mùa": ", ".join(ctx.config["seasons"])}])
        section("Tổng quan", "Phạm vi kết quả", [{"phân_tích": slug, "số_bộ_kết_quả": sum(e["slug"] == slug for e in index)} for slug in sorted({e["slug"] for e in index})])
        section("Đội bóng", "Thống kê chi tiết 20 đội mỗi mùa", tables["fact_team_season"])
        section("Cầu thủ", "Toàn bộ cầu thủ–CLB–mùa hợp lệ", tables["fact_player_season"])
        section("Cầu thủ", "Đối chiếu danh tính chưa ghép được nguồn", tables["identity_review"])
        mapping = {"player-contribution": "Cầu thủ", "squad-value": "Giá trị", "matchday-performance": "Theo vòng",
                   "points-progression": "Theo vòng", "head-to-head-matrix": "Đối đầu", "head-to-head": "Đối đầu",
                   "age-distribution": "Độ tuổi", "spending-vs-achievement": "Chi tiêu", "attack-defense": "Đội bóng",
                   "player-radar": "Chỉ số radar", "team-strength": "Chỉ số radar"}
        grouped = defaultdict(list)
        for entry in index:
            value = json.loads((ctx.local / entry["file"]).read_text())
            for row in value["data"]:
                grouped[entry["slug"]].append({"phạm_vi": entry["scope"], "mùa": entry["params"].get("season"),
                                                "CLB_chọn": entry["params"].get("club_id"), **row})
        for slug, rows in grouped.items():
            section(mapping[slug], "Kết quả: " + slug, rows)
        # All three pairwise comparisons, preserving clubs absent from a season.
        teams = tables["fact_team_season"]
        lookup = {(row["club_id"], row["season"]): row for row in teams}
        comparison = []
        seasons = ctx.config["seasons"]
        metrics = ["league_rank", "points", "earned_points", "points_adjustment", "goals_for", "goals_against", "goal_diff", "wins", "draws", "losses"]
        for club in sorted({row["club_id"] for row in teams}):
            for metric in metrics:
                comparison.append({"club_id": club, "chỉ_số": metric, **{season: lookup.get((club, season), {}).get(metric) for season in seasons},
                                   **{b + " trừ " + a: None for a, b in combinations(seasons, 2)}})
        start, columns = section("Đội bóng", "So sánh ba mùa và từng cặp mùa (công thức Excel)", comparison)
        for offset, row in enumerate(comparison):
            i = start + offset
            for a, b in combinations(seasons, 2):
                col = columns.index(b + " trừ " + a)
                first, last = (xl_rowcol_to_cell(i, columns.index(s)) for s in (a, b))
                formula = f'=IF(COUNT({first},{last})=2,{last}-{first},"")'
                cached = row[b] - row[a] if row[a] is not None and row[b] is not None else ""
                sheets["Đội bóng"].write_formula(i, col, formula, number, cached)
        quality = json.loads((ctx.local / "silver_quality.json").read_text())
        section("Đối soát", "Kiểm tra Silver", quality)
        section("Đối soát", "Dòng và schema các bảng Parquet", [{"bảng": key, "số_dòng": value["row_count"], "đường_dẫn_HDFS": value["path"]} for key, value in ctx.manifest["tables"].items()])
        section("Đối soát", "Checksum nguồn đã đối chiếu Bronze", json.loads((ctx.local / "source_manifest.json").read_text()))
    # Reopen the delivered workbook including formula caches, checking all sheets.
    from openpyxl import load_workbook
    opened = load_workbook(path, read_only=True, data_only=True)
    errors = []
    for sheet in opened:
        for row in sheet:
            errors.extend(f"{sheet.title}!{item.coordinate}" for item in row if item.data_type == "e")
    names = opened.sheetnames
    opened.close()
    if len(names) != 10 or errors:
        raise RuntimeError(f"Excel không hợp lệ: {errors}")
    atomic_json(ctx.local / "excel/validation.json", {"passed": True, "sheets": names, "formula_errors": errors,
                "missing_values": "blank", "formula_cache": "pairwise difference validated from exported Spark results"})


def export(ctx):
    from .stages import api_imports, clean
    api_imports()
    from app.static_reports import DRAWERS, REPORTS, REPORT_VERSION
    index = json.loads((ctx.local / "analysis/index.json").read_text())
    values = {entry["id"]: json.loads((ctx.local / entry["file"]).read_text()) for entry in index}
    reports = ctx.local / "reports"
    reports.mkdir(exist_ok=True)
    for season in ctx.config["seasons"]:
        directory = reports / season
        directory.mkdir(exist_ok=True)
        for slug in TITLES:
            source_slug = "head-to-head-matrix" if slug == "head-to-head" else slug
            if slug in ("player-contribution", "matchday-performance"):
                key = slug + "__" + season + "_" + ctx.config["example_club"]
            elif slug == "player-radar":
                key = slug + "__" + season + "_FW"
            elif slug == "points-progression":
                key = slug + "__all_" + ctx.config["example_club"]
            else:
                key = source_slug + "__" + season
            result = values[key]
            image = directory / (slug + ".png")
            if slug in DRAWERS:
                DRAWERS[slug](result, season, image)
            else:
                plot(result, slug, season, image, ctx.config)
            report = {**result, "title": TITLES[slug], "image_url": f"/reports/{season}/{slug}.png",
                      "meta": {**result["meta"], "season": season, "dataset_id": ctx.run_id}}
            atomic_json(directory / (slug + ".json"), {"dataset_id": ctx.run_id, "report_version": REPORT_VERSION, "report": report})
            LOG.info("Đã xuất biểu đồ %s mùa %s.", slug, season)
    # Detailed exports also cover players below the radar cohort threshold.
    spark = ctx.session("export-detailed-tables")
    tables = {}
    detail = ctx.local / "tables"
    detail.mkdir(exist_ok=True)
    try:
        for key, entry in ctx.manifest["tables"].items():
            name = key.split("/")[-1]
            frame = spark.read.parquet(entry["path"])
            rows = [clean(row.asDict(recursive=True)) for row in frame.toLocalIterator()]
            if len(rows) != entry["row_count"]:
                raise RuntimeError("Chi tiết xuất không khớp Silver: " + name)
            if name in ("fact_team_season", "fact_player_season", "identity_review"):
                tables[name] = rows
            with (detail / (name + ".jsonl")).open("w", encoding="utf-8") as target:
                for row in rows:
                    target.write(json.dumps(row, ensure_ascii=False) + "\n")
    finally:
        spark.stop()
    workbook(ctx, index, tables)
    diagrams(ctx)
