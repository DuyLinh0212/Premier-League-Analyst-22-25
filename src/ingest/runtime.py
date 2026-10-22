"""Process locks, graceful shutdown, and durable local file writes."""
from contextlib import contextmanager
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import threading
from zoneinfo import ZoneInfo

LOG_MARKERS = ("[THÔNG TIN]", "[LƯU Ý]", "[LỖI]")


class VietnameseFormatter(logging.Formatter):
    def format(self, record):
        label = "LỖI" if record.levelno >= logging.ERROR else "LƯU Ý" if record.levelno >= logging.WARNING else "THÔNG TIN"
        timestamp = datetime.fromtimestamp(record.created, ZoneInfo("Asia/Bangkok")).strftime("%H:%M:%S")
        message = f"{timestamp} [{label}] {record.getMessage()}"
        if record.exc_info:
            message += "\n" + self.formatException(record.exc_info)
        return message


def configure_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(VietnameseFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    for name in ("httpx", "httpcore", "py4j", "pyspark", "uvicorn.access"):
        logging.getLogger(name).setLevel(logging.ERROR)


def run_logged(command, log_path: Path, *, cwd=None, env=None):
    """Keep JVM/CLI diagnostics in a file; show Vietnamese progress on the console."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as detail:
        with subprocess.Popen(command, cwd=cwd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, encoding="utf-8", errors="replace", bufsize=1) as process:
            for line in process.stdout:
                detail.write(line)
                detail.flush()
                if any(marker in line for marker in LOG_MARKERS):
                    print(line.rstrip(), flush=True)
            result = process.wait()
    if result:
        logging.getLogger(__name__).error("Tác vụ thất bại (mã %s). Xem log chi tiết: %s", result, log_path)
        raise subprocess.CalledProcessError(result, command)


def shutdown_event():
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    return stop


@contextmanager
def process_lock(path: Path):
    import fcntl  # All runtime processes are launched inside Ubuntu/WSL.
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(f"Another process owns {path}") from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def atomic_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
