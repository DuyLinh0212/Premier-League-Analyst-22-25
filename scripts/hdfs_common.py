"""Use the Hadoop installation already configured in Ubuntu/WSL."""
from __future__ import annotations

import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import xml.etree.ElementTree as ET

PROJECT_ROOT = Path(__file__).resolve().parents[1]
LAYERS = ("bronze", "silver", "golde", "ingestion")
BLOCK_SIZE = 64 * 1024 * 1024
REPLICATION = 2


def setting(name: str, default: str) -> str:
    if name in os.environ:
        return os.environ[name]
    env_file = PROJECT_ROOT / ".env"
    if env_file.is_file():
        for line in env_file.read_text(encoding="utf-8-sig").splitlines():
            key, separator, value = line.strip().removeprefix("export ").partition("=")
            if separator and key.strip() == name:
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                    return value[1:-1]
                return value.split(" #", 1)[0].rstrip()
    return default


def hdfs_root(value: str | None = None) -> str:
    value = (value if value is not None else setting("HDFS_ROOT", "/PremierLeagueAnalysis")).rstrip("/")
    if not re.fullmatch(r"(?:/[A-Za-z0-9_-]+)+", value):
            raise ValueError("Thư mục gốc HDFS phải là đường dẫn tuyệt đối và không chứa thành phần . hoặc ..")
    return value


def find_hdfs(explicit: str | None = None, dry_run: bool = False) -> str:
    specified = explicit or setting("HDFS_BIN", "")
    if specified:
        binary = shutil.which(specified) or str(Path(specified).expanduser())
        if not dry_run and not (Path(binary).is_file() and os.access(binary, os.X_OK)):
            raise RuntimeError(f"Không tìm thấy chương trình hdfs tại: {binary}")
        return binary
    binary = shutil.which("hdfs")
    if binary:
        return binary
    hadoop_home = os.environ.get("HADOOP_HOME")
    candidates = [Path(hadoop_home) / "bin/hdfs"] if hadoop_home else []
    for parent in (Path.home(), Path("/opt"), Path("/usr/local")):
        candidates.extend(sorted(parent.glob("hadoop*/bin/hdfs")))
    for user_home in sorted(Path("/home").glob("*")):
        if user_home.is_dir():
            candidates.extend(sorted(user_home.glob("hadoop*/bin/hdfs")))
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    if dry_run:
        return "hdfs"
    raise RuntimeError(
        "Không tìm thấy lệnh hdfs trong Ubuntu/WSL. Hãy thêm $HADOOP_HOME/bin vào PATH "
        "hoặc truyền --hdfs-bin /đường/dẫn/tới/hadoop/bin/hdfs."
    )


def hadoop_environment(binary: str) -> dict[str, str]:
    environment = os.environ.copy()
    home = Path(binary).resolve().parents[1]
    environment["HADOOP_HOME"] = str(home)
    if not environment.get("JAVA_HOME"):
        java = shutil.which("java")
        if java:
            environment["JAVA_HOME"] = str(Path(java).resolve().parents[1])
    if not environment.get("HADOOP_CONF_DIR"):
        installed_conf = home / "etc/hadoop"
        configured = False
        core = installed_conf / "core-site.xml"
        if core.is_file():
            props = {p.findtext("name"): p.findtext("value", "") for p in ET.parse(core).findall("property")}
            configured = props.get("fs.defaultFS", "").startswith(("hdfs://", "viewfs://"))
        project_state = Path(setting("HDFS_STATE_DIR", str(Path.home() / ".local/share/premier-league-hdfs")))
        project_conf = project_state / "conf/namenode"
        fallback = project_conf if project_conf.is_dir() else PROJECT_ROOT / "config/hadoop"
        environment["HADOOP_CONF_DIR"] = str(installed_conf if configured else fallback)
    return environment


class HdfsClient:
    def __init__(self, dry_run: bool = False, hdfs_bin: str | None = None):
        self.dry_run = dry_run
        self.binary = find_hdfs(hdfs_bin, dry_run)
        self.environment = hadoop_environment(self.binary)

    def hdfs(self, *arguments: str, capture: bool = False, timeout: int | None = None) -> str:
        command = [self.binary, *arguments]
        print("[LỆNH HDFS] " + shlex.join(command), flush=True)
        if self.dry_run:
            return ""
        result = subprocess.run(
            command, check=True, text=True, encoding="utf-8", errors="replace",
            capture_output=capture, timeout=timeout,
            env=self.environment,
        )
        return result.stdout.strip() if capture else ""

    def require_ready(self, min_datanodes: int = 1) -> None:
        if self.dry_run:
            return
        uri = self.hdfs("getconf", "-confKey", "fs.defaultFS", capture=True, timeout=30)
        if not uri.startswith(("hdfs://", "viewfs://")):
            raise RuntimeError(
                f"fs.defaultFS={uri!r} không trỏ tới HDFS. Hãy đặt HADOOP_CONF_DIR tới cấu hình cụm đang dùng; "
                "chưa có dữ liệu nào được tải lên filesystem cục bộ."
            )
        report = self.hdfs("dfsadmin", "-report", capture=True, timeout=30)
        match = re.search(r"Live datanodes \((\d+)\):", report)
        if not match or int(match.group(1)) < min_datanodes:
            raise RuntimeError(
                f"Cần ít nhất {min_datanodes} DataNode đang hoạt động. "
                "Kiểm tra bằng hdfs dfsadmin -report; chờ đủ 2 bản sao cần ít nhất 2 DataNode."
            )
        safemode = self.hdfs("dfsadmin", "-safemode", "get", capture=True, timeout=30)
        if "Safe mode is OFF" not in safemode:
            raise RuntimeError("NameNode đang ở chế độ an toàn; hãy đợi HDFS sẵn sàng.")

    def init_layout(self, root: str) -> None:
        self.hdfs("dfs", "-mkdir", "-p", *(f"{root}/{layer}" for layer in LAYERS))


def command_error(error: Exception) -> str:
    if isinstance(error, subprocess.CalledProcessError):
        return f"Lệnh thất bại (mã thoát {error.returncode}). {(error.stderr or error.stdout or '').strip()}"
    return str(error)
