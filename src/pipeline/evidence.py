"""Collect actual logs and browser/HTTP evidence, including on failed runs."""
from __future__ import annotations

import hashlib
from html import escape
import json
import logging
from pathlib import Path
import re
import shutil
import time
from urllib.parse import urlencode
import uuid

import httpx

from src.ingest.runtime import atomic_json
from .config import ROOT

LOG = logging.getLogger(__name__)
TABS = ("player-radar", "player-contribution", "squad-value", "age-distribution", "head-to-head",
        "matchday-performance", "attack-defense", "points-progression", "team-strength", "spending-vs-achievement")


def redact(text, config):
    values = []
    try:
        credentials = config.credentials()
        values.extend(credentials[role]["password"] for role in ("reader", "writer", "airflow"))
    except (OSError, ValueError, RuntimeError, KeyError):
        pass
    private = config.state / "service-secrets.json"
    if private.exists():
        settings = json.loads(private.read_text())
        values.extend(settings[key] for key in ("password", "jwt", "fernet"))
    for value in values:
        text = text.replace(value, "[ĐÃ ẨN]")
    text = re.sub(r"(?i)(PASSWORD\s+)[^\s;]+", r"\1[ĐÃ ẨN]", text)
    text = re.sub(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", "[TOKEN ĐÃ ẨN]", text)
    text = re.sub(r"(?i)(authorization[\"']?\s*[:=]\s*[\"']?Bearer\s+)[^\s\"']+", r"\1[ĐÃ ẨN]", text)
    return text


def capture(ctx):
    from playwright.sync_api import sync_playwright
    directory = ctx.local / "screenshots/web"
    directory.mkdir(parents=True, exist_ok=True)
    network, errors = [], []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1540, "height": 1050}, locale="vi-VN", device_scale_factor=1)
        page = context.new_page()

        def response_evidence(response):
            if response.url.startswith(ctx.config["api_url"] + "/api/"):
                try:
                    value = response.json()
                    network.append({"url": response.url, "status": response.status, "run_id": response.headers.get("x-plca-run-id"),
                                    "source": response.headers.get("x-plca-source"), "body": value})
                except Exception:
                    network.append({"url": response.url, "status": response.status, "error": "Response is not JSON"})
        page.on("response", response_evidence)
        page.on("pageerror", lambda error: errors.append(str(error)))
        try:
            health = httpx.get(ctx.config["api_url"] + "/health", timeout=20)
            health.raise_for_status()
            body = health.json()
            if body.get("run_id") != ctx.run_id or body.get("source") != "postgresql" or not body.get("database"):
                raise AssertionError("Web không phục vụ PostgreSQL của run_id đang nghiệm thu")
            atomic_json(ctx.local / "api-health.json", body)
            page.goto(ctx.config["web_url"], wait_until="networkidle", timeout=60000)
            for season in ctx.config["seasons"]:
                page.get_by_test_id("season").select_option(season)
                for tab in TABS:
                    page.get_by_test_id("tab-" + tab).click()
                    if tab == "player-radar":
                        metadata = httpx.get(ctx.config["api_url"] + "/api/metadata", params={"season": season, "position": "FW", "min_minutes": 900}, timeout=20).json()
                        chosen = sorted(metadata["players"], key=lambda item: (-item["minutes"], item["selection_id"]))[:2]
                        while page.locator(".picker-selected .picker-chip").count():
                            page.locator(".picker-selected .picker-chip").first.click()
                        for player in chosen:
                            page.get_by_test_id("choice-" + player["selection_id"]).click()
                    report = page.get_by_test_id("report")
                    page.wait_for_function("""([run,tab]) => {
                        const box=document.querySelector('[data-testid="report"]');
                        return box && box.dataset.ready==='true' && box.dataset.runId===run &&
                               box.dataset.source==='postgresql' && box.dataset.analysis===tab;
                    }""", arg=[ctx.run_id, tab], timeout=60000)
                    page.wait_for_function("""() => [...document.querySelectorAll('.report-card img')].every(i => i.complete && i.naturalWidth > 20)""", timeout=30000)
                    if report.locator("canvas").count():
                        page.wait_for_function("""() => document.querySelector('.report-card [data-chart-ready="true"]') !== null""", timeout=30000)
                    page.evaluate("document.fonts.ready")
                    # ECharts has a short transition after updating an existing chart.
                    page.wait_for_timeout(600)
                    page.screenshot(path=str(directory / f"{season}__{tab}.png"), full_page=True, animations="disabled")
                    if page.get_by_role("alert").count():
                        raise AssertionError(f"Web báo lỗi ở {tab}/{season}")
                    LOG.info("Đã kiểm tra và chụp Web: %s, mùa %s, nguồn PostgreSQL.", tab, season)
            for record in network:
                if record["status"] != 200 or record.get("run_id") != ctx.run_id or record.get("source") != "postgresql":
                    raise AssertionError("Phản hồi mạng Web không khớp snapshot")
            if not network or not any("player-radar" in record["url"] for record in network):
                raise AssertionError("Chưa thu được phản hồi mạng thực của Web")
            if errors:
                raise AssertionError("JavaScript Web báo lỗi: " + "; ".join(errors))
            # Verify each pair and all three seasons directly through the same public API.
            from itertools import combinations
            comparisons = []
            with httpx.Client(base_url=ctx.config["api_url"], timeout=30) as client:
                for length in (2, 3):
                    for seasons in combinations(ctx.config["seasons"], length):
                        for endpoint in ("head-to-head", "points-progression"):
                            params = [("club_id", ctx.config["example_club"]), *[("seasons", season) for season in seasons]]
                            response = client.get("/api/" + endpoint, params=params)
                            response.raise_for_status()
                            value = response.json()
                            if value["meta"].get("run_id") != ctx.run_id or not value["data"]:
                                raise AssertionError("So sánh mùa không có dữ liệu đúng run_id")
                            comparisons.append({"analysis": endpoint, "seasons": seasons, "body": value})
            atomic_json(ctx.local / "season-comparisons.json", comparisons)
            capture_airflow(ctx, context)
            atomic_json(ctx.local / "web-validation.json", {"passed": True, "tabs": list(TABS), "seasons": ctx.config["seasons"],
                        "screenshots": 30, "run_id": ctx.run_id, "source": "postgresql", "javascript_errors": errors})
        except Exception:
            try:
                page.screenshot(path=str(directory / "ERROR.png"), full_page=True)
            except Exception:
                pass
            atomic_json(ctx.local / "web-validation.json", {"passed": False, "run_id": ctx.run_id, "javascript_errors": errors})
            raise
        finally:
            atomic_json(ctx.local / "network-responses.json", network)
            browser.close()


def capture_airflow(ctx, browser_context):
    from .ops import airflow_client
    from playwright.sync_api import expect
    directory = ctx.local / "screenshots/airflow"
    directory.mkdir(parents=True, exist_ok=True)
    with airflow_client(ctx.config) as client:
        token = client.headers["Authorization"].split(" ", 1)[1]
    browser_context.add_cookies([{"name": "_token", "value": token, "url": ctx.config["airflow_url"], "httpOnly": True, "sameSite": "Lax"}])
    page = browser_context.new_page()
    base = ctx.config["airflow_url"] + "/dags/premier_league_reports"
    page.goto(base, wait_until="networkidle", timeout=60000)
    expect(page.get_by_text("premier_league_reports", exact=True).first).to_be_visible(timeout=30000)
    page.screenshot(path=str(directory / "dag.png"), full_page=True)
    page.goto(base + "/runs/" + ctx.run_id, wait_until="networkidle", timeout=60000)
    expect(page.get_by_text(ctx.run_id, exact=True).first).to_be_visible(timeout=30000)
    page.screenshot(path=str(directory / "run.png"), full_page=True)
    page.goto(base + "/runs/" + ctx.run_id + "/tasks/publish?tab=logs", wait_until="networkidle", timeout=60000)
    # A log screenshot must contain actual task output, not just a navigation shell.
    expect(page.get_by_text("Hoàn tất bước publish.", exact=False).first).to_be_visible(timeout=30000)
    page.screenshot(path=str(directory / "task-log.png"), full_page=True)
    page.close()


def log_page(ctx, destination):
    directory = destination / "log-viewer"
    directory.mkdir(exist_ok=True)
    manifest = ctx.manifest
    style = """body{font-family:'Segoe UI',Arial,sans-serif;margin:0;background:#101a2a;color:#e4eef7}header{padding:24px 32px;background:#19283f}main{padding:24px 32px}h1{font-size:24px}a{color:#85d7b1}pre{white-space:pre-wrap;overflow-wrap:anywhere;line-height:1.55;font:14px Consolas,monospace;background:#091322;padding:22px;border-radius:10px}table{width:100%;border-collapse:collapse}td,th{padding:9px;text-align:left;border-bottom:1px solid #334157}.success{color:#86e7b6}.failed{color:#ff9f9f}"""
    rows, links = [], []
    for stage, status in manifest.get("stages", {}).items():
        rows.append(f'<tr class="{escape(status["status"])}"><td>{escape(stage)}</td><td>{escape(status["status"])}</td><td>{escape(str(status.get("duration_seconds", "")))}</td></tr>')
    for source in sorted((destination / "logs").rglob("*")) if (destination / "logs").exists() else []:
        if not source.is_file():
            continue
        relative = source.relative_to(destination).as_posix()
        name = hashlib.sha256(relative.encode()).hexdigest()[:12] + ".html"
        text = source.read_text(encoding="utf-8", errors="replace")
        # Full original text remains attached; the browser view shows the actual last 100 lines.
        excerpt = "\n".join(text.splitlines()[-100:])
        document = f'<html lang="vi"><meta charset="utf-8"><title>{escape(relative)}</title><style>{style}</style><header><h1>Log thực tế · {escape(relative)}</h1><p>Lần chạy: {escape(ctx.run_id)}</p></header><main><p>100 dòng cuối. Toàn bộ log nằm trong artifact.</p><pre>{escape(excerpt)}</pre></main></html>'
        (directory / name).write_text(document, encoding="utf-8")
        links.append(f'<li><a href="{name}">{escape(relative)}</a></li>')
    (directory / "index.html").write_text(f'<html lang="vi"><meta charset="utf-8"><title>Nhật ký pipeline</title><style>{style}</style><header><h1>Nhật ký pipeline Premier League</h1><p>{escape(ctx.run_id)} · {escape(manifest["report_date"])} · Trạng thái: {escape(manifest.get("status", "đang chạy"))}</p></header><main><table><tr><th>Tác vụ</th><th>Trạng thái thực tế</th><th>Thời gian (giây)</th></tr>{"".join(rows)}</table><h2>Log theo thành phần và tác vụ</h2><ul>{"".join(links)}</ul></main></html>', encoding="utf-8")
    return directory


def collect_artifacts(ctx, destination, success):
    from .reports import diagrams
    diagrams(ctx)
    destination.mkdir(parents=True, exist_ok=True)
    shutil.copytree(ctx.local, destination, dirs_exist_ok=True)
    logs = destination / "logs"
    logs.mkdir(exist_ok=True)
    global_logs = ctx.config.state / "logs"
    if global_logs.exists():
        shutil.copytree(global_logs, logs / "services", dirs_exist_ok=True)
    airflow_logs = Path(ctx.config["airflow_home"]).expanduser() / "logs" / "dag_id=premier_league_reports" / ("run_id=" + ctx.run_id)
    if airflow_logs.exists():
        shutil.copytree(airflow_logs, logs / "airflow", dirs_exist_ok=True)
    for path in logs.rglob("*"):
        if path.is_file():
            path.write_text(redact(path.read_text(encoding="utf-8", errors="replace"), ctx.config), encoding="utf-8")
    directory = log_page(ctx, destination)
    from playwright.sync_api import sync_playwright
    captures = destination / "screenshots/logs"
    captures.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1500, "height": 1000}, locale="vi-VN")
        for path in [directory / "index.html", *sorted(directory.glob("*.html"))]:
            page.goto(path.as_uri(), wait_until="load")
            page.screenshot(path=str(captures / (path.stem + ".png")), full_page=True)
        browser.close()
    entries = []
    for path in sorted(destination.rglob("*")):
        if path.is_file() and path.name != "checksums.json":
            with path.open("rb") as source:
                checksum = hashlib.file_digest(source, "sha256").hexdigest()
            entries.append({"path": path.relative_to(destination).as_posix(), "size": path.stat().st_size, "sha256": checksum})
    atomic_json(destination / "checksums.json", entries)
    # Each evidence upload is immutable; retries keep prior failure evidence as well.
    attempt = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "_" + uuid.uuid4().hex[:8]
    final = (ctx.config.hdfs_root + "/golde/reports/report_date=" + ctx.manifest["report_date"] +
             "/evidence/run_id=" + ctx.run_id + "/attempt=" + attempt)
    staging = final + ".staging"
    ctx.hdfs.hdfs("dfs", "-mkdir", "-p", staging)
    ctx.hdfs.hdfs("dfs", "-put", *map(str, sorted(destination.iterdir())), staging)
    ctx.hdfs.hdfs("dfs", "-mv", staging, final)
    atomic_json(destination.with_name(destination.name + ".hdfs-complete.json"),
                {"run_id": ctx.run_id, "hdfs_path": final, "pipeline_success": success})
    LOG.info("Đã lưu báo cáo và minh chứng thực tế lên HDFS: %s", final)


def main():
    import argparse
    from src.ingest.runtime import configure_logging
    from .context import Context
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    configure_logging()
    ctx = Context(args.run_id)
    destination = ctx.config.artifacts / ctx.manifest["report_date"] / ctx.run_id
    if (destination / "checksums.json").is_file() and destination.with_name(destination.name + ".hdfs-complete.json").is_file():
        LOG.info("Artifact đã được thu thập: %s", destination)
        return
    if "status" not in ctx.manifest:
        ctx.manifest["status"] = "failed"
        ctx.save()
    collect_artifacts(ctx, destination, ctx.manifest["status"] == "success")


if __name__ == "__main__":
    main()
