"""Local and CI launcher. Airflow's scheduler, never `dags test`, executes jobs."""
from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import secrets
import shutil
import signal
import subprocess
import sys
import time
from urllib.parse import quote, urlparse
from zoneinfo import ZoneInfo

import httpx

from src.ingest.runtime import atomic_json, configure_logging, process_lock, run_logged
from .config import ROOT, Config
from .context import Context, safe_id

LOG = logging.getLogger(__name__)
DAG_ID = "premier_league_reports"


def protected_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_json(path, value)
    path.chmod(0o600)


def private_settings(config):
    path = config.state / "service-secrets.json"
    if not path.exists():
        protected_json(path, {"fernet": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(), "jwt": secrets.token_urlsafe(48),
                              "username": "plca", "password": secrets.token_urlsafe(32)})
    return json.loads(path.read_text())


def airflow_environment(config):
    credential = config.connection("airflow")
    settings = private_settings(config)
    home = Path(config["airflow_home"]).expanduser()
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    passwords = home / "passwords.json"
    protected_json(passwords, {settings["username"]: settings["password"]})
    connection = ("postgresql+psycopg://" + quote(credential["user"], safe="") + ":" +
                  quote(credential["password"], safe="") + "@" + credential["host"] + ":" +
                  str(credential["port"]) + "/" + credential["dbname"])
    return {**os.environ, "PYTHONUTF8": "1", "PYTHONUNBUFFERED": "1", "PYTHONPATH": str(ROOT),
            "PLCA_PROJECT_ROOT": str(ROOT), "PLCA_SPARK_PYTHON": config.python(),
            "AIRFLOW_HOME": str(home), "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": connection,
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN_ASYNC": connection,
            "AIRFLOW__CORE__EXECUTOR": "LocalExecutor", "AIRFLOW__CORE__PARALLELISM": "2",
            "AIRFLOW__CORE__DAGS_FOLDER": str(ROOT / "airflow/dags"), "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__FERNET_KEY": settings["fernet"], "AIRFLOW__API_AUTH__JWT_SECRET": settings["jwt"],
            "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": config["airflow_url"].rstrip("/") + "/execution/",
            "AIRFLOW__API__BASE_URL": config["airflow_url"],
            "AIRFLOW__CORE__AUTH_MANAGER": "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS": settings["username"] + ":admin",
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE": str(passwords),
            "AIRFLOW__LOGGING__BASE_LOG_FOLDER": str(home / "logs"),
            "AIRFLOW__LOGGING__COLORED_CONSOLE_LOG": "False",
            "AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL": "15",
            "AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL": "10"}


def command(config, args, log, env=None):
    # run_logged keeps full native output in the technical log, with short Vietnamese progress.
    return run_logged(list(map(str, args)), log, cwd=ROOT, env=env)


def setup(config):
    config.credentials()  # Must first be created by the Windows setup without sharing secrets.
    config.state.mkdir(parents=True, exist_ok=True, mode=0o700)
    logs = config.state / "logs"
    logs.mkdir(exist_ok=True)
    LOG.info("Đang kiểm tra môi trường Spark và cài thư viện của pipeline...")
    command(config, [config.python(), "-m", "pip", "install", "-r", ROOT / "src/pipeline/requirements.txt"], logs / "dependencies.log")
    command(config, [config.python("airflow"), "-m", "pip", "install", "psycopg[binary]>=3.2,<4"], logs / "airflow-driver.log")
    env = airflow_environment(config)
    LOG.info("Đang tạo metadata Airflow trên PostgreSQL; giữ nguyên Airflow home cũ...")
    command(config, [config.python("airflow"), "-m", "airflow", "db", "migrate"], logs / "airflow-migrate.log", env)
    command(config, [config.python("airflow"), "-m", "airflow", "pools", "set", "plca_spark", "1", "Một tác vụ Spark nặng"], logs / "airflow-pool.log", env)
    from .database import initialize
    initialize(config)
    command(config, [config.python(), "-m", "playwright", "install", "chromium"], logs / "browser-install.log")
    LOG.info("Thiết lập hoàn tất. Tài khoản Airflow lưu riêng trong %s; không đưa file này vào artifact.", config.state / "service-secrets.json")


def process_start(pid):
    try:
        # /proc/PID/stat may contain spaces in the process name.
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return None if fields[0] == "Z" else fields[19]
    except (OSError, IndexError):
        return None


def live(record):
    return bool(record and process_start(record["pid"]) == record["start"])


def services(config):
    path = config.state / "services.json"
    return json.loads(path.read_text()) if path.exists() else {}


def start_service(config, name, arguments, env=None, cwd=None):
    records = services(config)
    previous = records.get(name)
    if live(previous):
        if previous.get("project") != str(ROOT):
            raise RuntimeError(f"{name} đang dùng bản triển khai khác; dừng bằng launcher trước khi chuyển commit")
        LOG.info("%s đã chạy, PID %d.", name, previous["pid"])
        return
    directory = config.state / "logs"
    directory.mkdir(parents=True, exist_ok=True)
    log = directory / (name + ".log")
    LOG.info("Đang khởi động %s...", name)
    with log.open("ab", buffering=0) as target:
        child = subprocess.Popen(list(map(str, arguments)), cwd=cwd or ROOT, env=env or os.environ.copy(),
                                 stdin=subprocess.DEVNULL, stdout=target, stderr=subprocess.STDOUT, start_new_session=True)
    time.sleep(0.5)
    if child.poll() is not None:
        raise RuntimeError(f"Không khởi động được {name}; xem {log}")
    records[name] = {"pid": child.pid, "start": process_start(child.pid), "project": str(ROOT), "log": str(log)}
    atomic_json(config.state / "services.json", records)


def wait_http(url, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            response = httpx.get(url, timeout=5)
            if response.status_code < 400:
                return
        except httpx.HTTPError:
            pass
        time.sleep(2)
    raise RuntimeError(f"Hết thời gian chờ dịch vụ: {url}")


def port(url):
    parsed = urlparse(url)
    if parsed.hostname not in ("127.0.0.1", "localhost") or not parsed.port:
        raise ValueError("Launcher chỉ khởi động dịch vụ trên localhost với cổng cấu hình rõ ràng")
    return str(parsed.port)


def start(config, web=False):
    config.state.mkdir(parents=True, exist_ok=True, mode=0o700)
    logs = config.state / "logs"
    logs.mkdir(exist_ok=True)
    command(config, [config.python(), ROOT / "scripts/start_hdfs.py"], logs / "hdfs-start.log")
    from .database import connect
    with connect(config) as db:
        db.execute("SELECT 1")
    from .database import recover_rollbacks
    recover_rollbacks(config)
    LOG.info("HDFS và PostgreSQL đã sẵn sàng.")
    env = airflow_environment(config)
    airflow = [config.python("airflow"), "-m", "airflow"]
    start_service(config, "Airflow API", airflow + ["api-server", "--host", "127.0.0.1", "--port", port(config["airflow_url"]), "--workers", "1"], env)
    wait_http(config["airflow_url"] + "/api/v2/monitor/health")
    start_service(config, "Airflow DAG processor", airflow + ["dag-processor"], env)
    start_service(config, "Airflow scheduler", airflow + ["scheduler"], env)
    if web:
        start_web(config)
    LOG.info("Airflow: %s. DAG chỉ chạy khi kích hoạt thủ công hoặc từ CI/CD.", config["airflow_url"])


def stop(config):
    records = services(config)
    # Refuse to interrupt an active publication/run.
    try:
        with airflow_client(config) as client:
            response = client.get(f"/api/v2/dags/{DAG_ID}/dagRuns", params={"state": "running", "limit": 1})
            if response.is_success and response.json().get("dag_runs"):
                raise RuntimeError("DAG đang chạy; chờ hoàn tất hoặc dừng lần chạy trong Airflow trước khi stop")
    except httpx.HTTPError:
        pass
    for name, record in reversed(list(records.items())):
        if live(record):
            LOG.info("Đang dừng %s, giữ nguyên dữ liệu và checkpoint...", name)
            os.killpg(record["pid"], signal.SIGTERM)
            deadline = time.monotonic() + 30
            while live(record) and time.monotonic() < deadline:
                time.sleep(0.5)
            if live(record):
                raise RuntimeError(f"{name} chưa dừng; xem log, không xóa PID đang sống")
    atomic_json(config.state / "services.json", {})


def status(config):
    records = services(config)
    for name, record in records.items():
        LOG.info("%s: %s (PID %d)", name, "ĐANG CHẠY" if live(record) else "ĐÃ DỪNG", record["pid"])
    if not records:
        LOG.info("Chưa có tiến trình được launcher này quản lý.")


def authenticate(client, config):
    settings = private_settings(config)
    response = client.post("/auth/token", json={"username": settings["username"], "password": settings["password"]})
    response.raise_for_status()
    client.headers["Authorization"] = "Bearer " + response.json()["access_token"]


@contextmanager
def airflow_client(config):
    with httpx.Client(base_url=config["airflow_url"], timeout=30) as client:
        authenticate(client, config)
        yield client


def build_web(ctx):
    script = subprocess.check_output(["wslpath", "-w", str(ROOT / "scripts/build_pipeline_web.ps1")], text=True).strip()
    LOG.info("Đang kiểm thử và build giao diện Web...")
    command(ctx.config, ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script, "-ApiBaseUrl", ctx.config["api_url"]], ctx.local / "logs/web-build.log")


def start_web(config):
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(ROOT), str(ROOT / "apps/analytics-web/api")]), "PYTHONUTF8": "1"}
    start_service(config, "API phân tích", [config.python(), "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", port(config["api_url"])], env)
    wait_http(config["api_url"] + "/health")
    dist = ROOT / "apps/analytics-web/web/dist"
    if not (dist / "index.html").is_file():
        raise RuntimeError("Chưa build Web; chạy launcher -Action run")
    start_service(config, "Web", [config.python(), "-m", "http.server", port(config["web_url"]), "--bind", "127.0.0.1", "--directory", str(dist)])
    wait_http(config["web_url"])


def run(config, run_id):
    ctx = Context(run_id, config)
    (ctx.local / "logs").mkdir(exist_ok=True)
    from src.ingest.runtime import VietnameseFormatter
    handler = logging.FileHandler(ctx.local / "logs/pipeline.log", encoding="utf-8")
    handler.setFormatter(VietnameseFormatter())
    logging.getLogger().addHandler(handler)
    config.artifacts.mkdir(parents=True, exist_ok=True)
    destination = config.artifacts / ctx.manifest["report_date"] / ctx.run_id
    LOG.info("Đang chạy pipeline %s; ngày báo cáo %s.", ctx.run_id, ctx.manifest["report_date"])
    failed = None
    try:
        command(config, [config.python(), "-m", "pytest", "src/pipeline/tests", "-q", "--junitxml=" + str(ctx.local / "tests.xml")], ctx.local / "logs/tests.log")
        build_web(ctx)
        start(config)
        deadline = time.monotonic() + 180
        with airflow_client(config) as client:
            while time.monotonic() < deadline:
                dag = client.get(f"/api/v2/dags/{DAG_ID}")
                if dag.is_success and dag.json().get("fileloc") == str(ROOT / "airflow/dags/report_dag.py"):
                    atomic_json(ctx.local / "airflow-deployment.json", {"fileloc": dag.json()["fileloc"],
                                "commit_sha": ctx.manifest["commit_sha"], "project": str(ROOT)})
                    break
                time.sleep(3)
            else:
                raise RuntimeError("DAG chưa import đúng bản mã đang triển khai; xem log DAG processor")
            client.patch(f"/api/v2/dags/{DAG_ID}", json={"is_paused": False}).raise_for_status()
            endpoint = f"/api/v2/dags/{DAG_ID}/dagRuns/{ctx.run_id}"
            existing = client.get(endpoint)
            if existing.status_code == 404:
                response = client.post(f"/api/v2/dags/{DAG_ID}/dagRuns",
                                       json={"dag_run_id": ctx.run_id, "logical_date": None, "conf": {"run_id": ctx.run_id}})
                response.raise_for_status()
            elif not existing.is_success:
                existing.raise_for_status()
            elif existing.json()["state"] == "failed":
                LOG.info("Đang tiếp tục tác vụ lỗi của cùng run_id; giữ nguyên các bước đã thành công.")
                client.post(endpoint + "/clear", json={"dry_run": False, "only_failed": True,
                            "note": "Tiếp tục bằng launcher sau khi kiểm tra nguyên nhân lỗi"}).raise_for_status()
            deadline = time.monotonic() + config["run_timeout_seconds"]
            previous = {}
            while time.monotonic() < deadline:
                response = client.get(endpoint)
                if response.status_code == 401:
                    authenticate(client, config)
                    continue
                response.raise_for_status()
                dag_run = response.json()
                tasks = client.get(endpoint + "/taskInstances")
                tasks.raise_for_status()
                task_state = tasks.json()
                atomic_json(ctx.local / "airflow-run.json", dag_run)
                atomic_json(ctx.local / "airflow-tasks.json", task_state)
                for task in task_state.get("task_instances", []):
                    name, state = task["task_id"], task.get("state")
                    if previous.get(name) != state:
                        label = {"queued": "ĐANG CHỜ", "running": "ĐANG XỬ LÝ", "success": "THÀNH CÔNG",
                                 "failed": "THẤT BẠI", "up_for_retry": "CHỜ THỬ LẠI", "skipped": "BỎ QUA",
                                 "upstream_failed": "DỪNG DO BƯỚC TRƯỚC LỖI"}.get(state, state or "ĐANG CHỜ")
                        LOG.info("Airflow · %s: %s", name, label)
                        previous[name] = state
                if dag_run["state"] == "success":
                    break
                if dag_run["state"] == "failed":
                    raise RuntimeError("Airflow báo thất bại; không công bố kết quả chưa đối soát")
                time.sleep(5)
            else:
                raise RuntimeError("Hết thời gian chờ DAG; lần chạy vẫn được giữ trong Airflow để kiểm tra")
        ctx = Context(run_id, config)  # Reload the manifest written by scheduler task processes.
        from .evidence import capture
        try:
            start_web(config)
            capture(ctx)
            ctx.manifest["status"] = "success"
            ctx.save()
            from .database import finalize_manifest
            finalize_manifest(ctx)
        except Exception:
            from .database import rollback
            rollback(ctx)
            raise
        ctx.manifest["status"] = "success"
        LOG.info("Nghiệm thu hoàn tất: HDFS, PostgreSQL và đủ 10 phân tích trên Web cùng run_id %s.", run_id)
    except Exception as error:
        failed = error
        ctx = Context(run_id, config)
        ctx.manifest["status"] = "failed"
        LOG.exception("Pipeline thất bại; đang thu thập log và minh chứng thực tế.")
    finally:
        ctx.save()
        from .evidence import collect_artifacts
        try:
            collect_artifacts(ctx, destination, failed is None)
        except Exception:
            LOG.exception("Không thu thập đủ artifact; trạng thái lần chạy là thất bại.")
            ctx.manifest["status"] = "failed"
            ctx.manifest["artifact_collection_failed"] = True
            ctx.save()
            if destination.is_dir():
                atomic_json(destination / "manifest.json", ctx.manifest)
            if failed is None:
                failed = RuntimeError("Thu thập artifact thất bại")
        LOG.info("Artifact: %s", destination)
    if failed is not None:
        raise failed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("setup", "start", "stop", "status", "run", "dry-run"))
    parser.add_argument("--run-id")
    args = parser.parse_args()
    configure_logging()
    config = Config()
    config.state.mkdir(parents=True, exist_ok=True, mode=0o700)
    run_id = safe_id(args.run_id or datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("local_%Y%m%d_%H%M%S"))
    if args.action == "dry-run":
        LOG.info("Dự án: %s; nguồn: %s; số file: %d", ROOT, config.source, sum(p.is_file() for p in config.source.rglob("*")))
        LOG.info("HDFS: %s; Airflow: %s; ba mùa: %s", config.hdfs_root, config["airflow_url"], ", ".join(config["seasons"]))
        LOG.info("Run ID: %s. Dry-run chỉ đọc cấu hình, không khởi động hoặc nạp dữ liệu.", run_id)
        return
    if args.action == "status":
        status(config)
        return
    with process_lock(config.state / "launcher.lock"):
        if args.action == "run":
            run(config, run_id)
        elif args.action == "start":
            start(config)
            from .database import connect
            with connect(config) as db:
                active = db.execute("SELECT run_id FROM plca.active WHERE id=1").fetchone()
            if active and active["run_id"]:
                start_web(config)
            else:
                LOG.info("Chưa có snapshot đã công bố; chạy -Action run để tạo dữ liệu và mở Web.")
        else:
            globals()[args.action](config)


if __name__ == "__main__":
    main()
