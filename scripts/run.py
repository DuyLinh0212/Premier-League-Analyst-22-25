"""Run local apps and start existing infrastructure; never create Docker resources."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / "Premier Leauge Club Analyst" / "web-next"
AIRFLOW = (
    "airflow-docker-postgres-1", "airflow-docker-redis-1",
    "airflow-docker-airflow-apiserver-1", "airflow-docker-airflow-scheduler-1",
    "airflow-docker-airflow-dag-processor-1", "airflow-docker-airflow-triggerer-1",
    "airflow-docker-airflow-worker-1",
)


def environment(path):
    if not path.is_file():
        raise RuntimeError("Missing env file. Copy portable/.env.example to portable/.env and configure it.")
    values = {}
    for number, line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        key, separator, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise RuntimeError(f"Invalid env entry on line {number}; values are not displayed.")
        if value.startswith(("'", '"')):
            quote = value[0]
            end = value.find(quote, 1)
            if end < 0 or value[end + 1:].strip() and not value[end + 1:].strip().startswith("#"):
                raise RuntimeError(f"Invalid quoted env entry on line {number}.")
            value = value[1:end]
        else:
            value = re.split(r"\s+#", value, maxsplit=1)[0].rstrip()
        values[key] = value  # Literal values; no shell execution or variable expansion.
    values.update(os.environ)  # Explicit shell configuration takes precedence.
    for key in ("NEO4J_HTTP_URI", "KAFKA_BOOTSTRAP_SERVERS", "HDFS_URI", "NAMENODE_HTTP_URI"):
        values[key] = values.get("LOCAL_" + key) or values.get(key, "").replace("host.docker.internal", "127.0.0.1")
    paths = [str(ROOT), str(ROOT / "jobs"), str(ROOT / "Generate_LeagueTable_Api" / "analystic")]
    if values.get("PYTHONPATH"):
        paths.append(values["PYTHONPATH"])
    values["PYTHONPATH"] = os.pathsep.join(paths)
    return values


def checked(command):
    result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.returncode:
        raise RuntimeError(f"Command failed: {command[0]} {command[1]}. Check Docker/service configuration.")
    return result.stdout.strip()


def names(service, env):
    value = env.get("KAFKA_CONTAINER", "plca-kafka") if service == "kafka" else env.get("AIRFLOW_CONTAINERS", ",".join(AIRFLOW))
    result = list(dict.fromkeys(item.strip() for item in value.split(",") if item.strip()))
    if not result or any(not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", item) for item in result):
        raise RuntimeError(f"Invalid existing {service} container names in env file.")
    return result


def container_state(name):
    try:
        return json.loads(checked(["docker", "inspect", "--format", "{{json .State}}", name]))
    except RuntimeError as exc:
        raise RuntimeError(f"Existing container not found: {name}. Configure its name; no container will be created.") from exc


def infrastructure(service, env):
    if not shutil.which("docker"):
        raise RuntimeError("Docker CLI is missing. Open Docker Desktop / start the existing Docker engine.")
    checked(["docker", "info", "--format", "{{.ServerVersion}}"])
    # Inspect the complete group before starting any member.
    return [(name, container_state(name)) for name in names(service, env)]


def port(value):
    number = int(value)
    if not 1 <= number <= 65535:
        raise ValueError("Ports must be between 1 and 65535.")
    return number


def app_command(service, env, args):
    if service == "backend":
        missing = [key for key in ("NEO4J_HTTP_URI", "NEO4J_PASSWORD", "KAFKA_BOOTSTRAP_SERVERS") if not env.get(key)]
        if missing or env.get("NEO4J_PASSWORD") == "replace-with-existing-password":
            raise RuntimeError("Configure NEO4J_HTTP_URI, NEO4J_PASSWORD and KAFKA_BOOTSTRAP_SERVERS in the private env file.")
        missing_modules = [module for module in ("fastapi", "uvicorn", "pydantic", "numpy", "kafka") if not importlib.util.find_spec(module)]
        if missing_modules:
            raise RuntimeError("Missing API dependencies. Activate Python 3.11 venv and install portable/requirements-api.txt.")
        host = env.get("LOCAL_API_HOST", "127.0.0.1")
        number = port(args.api_port if args.api_port is not None else env.get("API_PORT", "8000"))
        command = [sys.executable, "-m", "uvicorn", "match_outcome.api:app", "--host", host, "--port", str(number)]
        directory = ROOT
    else:
        node = shutil.which("node")
        if not node:
            raise RuntimeError("Node.js is missing. Install Node.js >=20.9 and run npm ci in web-next.")
        version = tuple(int(item) for item in checked([node, "--version"]).lstrip("v").split(".")[:2])
        if version < (20, 9):
            raise RuntimeError("Next.js requires Node.js >=20.9.")
        cli = WEB / "node_modules" / "next" / "dist" / "bin" / "next"
        if not cli.is_file():
            raise RuntimeError("Frontend dependencies are missing. Run npm ci in web-next first.")
        standalone = WEB / ".next" / "standalone"
        if args.production and not (standalone / "server.js").is_file():
            raise RuntimeError("Production Web build is missing. Run npm run build in web-next first.")
        host = env.get("LOCAL_WEB_HOST", "127.0.0.1")
        number = port(args.web_port if args.web_port is not None else env.get("WEB_PORT", "3000"))
        api_port = port(args.api_port if args.api_port is not None else env.get("API_PORT", "8000"))
        env["FOOTBALL_API_URL"] = env.get("LOCAL_FOOTBALL_API_URL") or env.get("FOOTBALL_API_URL") or f"http://127.0.0.1:{api_port}"
        if args.production:
            env.update({"HOSTNAME": host, "PORT": str(number), "NODE_ENV": "production"})
            command, directory = [node, str(standalone / "server.js")], standalone
        else:
            command = [node, str(cli), "dev", "--hostname", host, "--port", str(number)]
            directory = WEB
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind((host, number))
    except OSError as exc:
        raise RuntimeError(f"Cannot bind {service} at {host}:{number}. Stop the existing app or choose another port.") from exc
    return command, directory, host, number


def start_infrastructure(group, timeout):
    for name, initial in group:
        if not initial.get("Running"):
            checked(["docker", "start", name])
        deadline = time.monotonic() + timeout
        while True:
            state = container_state(name)
            health = state.get("Health", {}).get("Status")
            if state.get("Running") and health in (None, "healthy"):
                print(f"{name}: {health or 'running (no healthcheck)'}", flush=True)
                break
            if not state.get("Running") or time.monotonic() >= deadline:
                raise RuntimeError(f"Container {name} is not ready. Inspect docker logs {name}; already started infrastructure is kept running.")
            time.sleep(2)


def stop_apps(processes):
    for process in processes:
        if process.poll() is None:
            try:
                if os.name == "nt":
                    process.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                if process.poll() is None:
                    process.kill()
                    process.wait()


def run(args):
    env_path = args.env_file if args.env_file.is_absolute() else ROOT / args.env_file
    env = environment(env_path)
    services = ("kafka", "airflow", "backend", "frontend") if args.service == "all" else (args.service,)
    apps, groups = {}, {}
    # Complete preflight first; --check never starts an app or a container.
    for service in services:
        if service in ("backend", "frontend"):
            apps[service] = app_command(service, env, args)
        else:
            groups[service] = infrastructure(service, env)
    if args.service == "all" and apps["backend"][3] == apps["frontend"][3]:
        raise RuntimeError("API and Web must use different ports.")
    for service, group in groups.items():
        for name, state in group:
            print(f"{service}: existing {name}, {state.get('Status', 'unknown')}", flush=True)
    for service, (_, _, host, number) in apps.items():
        print(f"{service}: http://{host}:{number}", flush=True)
    if args.check:
        print("Preflight passed. No processes or containers were started; Database authentication was not tested.")
        return 0
    timeout = int(env.get("STARTUP_TIMEOUT_SECONDS", "180"))
    if timeout <= 0:
        raise RuntimeError("STARTUP_TIMEOUT_SECONDS must be positive.")
    for group in groups.values():
        start_infrastructure(group, timeout)
    processes = []
    try:
        for command, directory, _, _ in apps.values():
            if directory == WEB / ".next" / "standalone":
                # Standalone builds omit these assets; copy only generated/public files.
                shutil.copytree(WEB / ".next" / "static", directory / ".next" / "static", dirs_exist_ok=True)
                if (WEB / "public").is_dir():
                    shutil.copytree(WEB / "public", directory / "public", dirs_exist_ok=True)
            options = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == "nt" else {"start_new_session": True}
            processes.append(subprocess.Popen(command, cwd=directory, env=env, **options))
        if processes:
            print("Apps run in this terminal. Ctrl+C stops these apps and leaves existing infrastructure running.", flush=True)
        while processes:
            for process in processes:
                code = process.poll()
                if code is not None:
                    return code if len(processes) == 1 or code else 1
            time.sleep(0.3)
        return 0
    except KeyboardInterrupt:
        return 0
    finally:
        stop_apps(processes)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("service", choices=("frontend", "backend", "kafka", "airflow", "all"))
    parser.add_argument("--env-file", type=Path, default=Path("portable/.env"), help="Relative to project root, or an absolute path")
    parser.add_argument("--check", action="store_true", help="Read-only dependency/configuration/container preflight")
    parser.add_argument("--production", action="store_true", help="Use the existing npm run build output")
    parser.add_argument("--api-port", type=int)
    parser.add_argument("--web-port", type=int)
    args = parser.parse_args()
    try:
        return run(args)
    except (RuntimeError, ValueError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
