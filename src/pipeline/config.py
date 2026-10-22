from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import yaml

ROOT = Path(__file__).resolve().parents[2]


class Config:
    def __init__(self, path=None):
        self.path = Path(path or os.getenv("PLCA_PIPELINE_CONFIG", ROOT / "config/pipeline.yaml")).resolve()
        self.values = yaml.safe_load(self.path.read_text(encoding="utf-8-sig"))
        if self.values.get("version") != 1:
            raise ValueError("Phiên bản cấu hình không được hỗ trợ")
        if self.values["seasons"] != ["2022-2023", "2023-2024", "2024-2025"]:
            raise ValueError("Pipeline lịch sử yêu cầu ba mùa 2022–2025")
        self.state = Path(os.getenv("PLCA_PIPELINE_STATE", self.values["state_dir"])).expanduser().resolve()
        self.artifacts = ROOT / self.values["artifact_dir"]
        self.source = (ROOT / self.values["source"]).resolve()
        if not self.source.is_relative_to((ROOT / "data").resolve()):
            raise ValueError("Nguồn phải nằm trong data/ của dự án")
        if str(self.state).startswith("/mnt/"):
            raise ValueError("State phải nằm trên filesystem Linux của WSL")
        self.hdfs_root = self.values["hdfs_root"].rstrip("/")
        if not re.fullmatch(r"(?:/[A-Za-z0-9_-]+)+", self.hdfs_root):
            raise ValueError("HDFS root không hợp lệ")

    def __getitem__(self, name):
        return self.values[name]

    def python(self, service="spark"):
        return str(Path(self.values[service + "_python"]).expanduser())

    def credentials(self):
        path = Path(os.getenv("PLCA_CREDENTIALS_FILE", self.values["credentials_file"])).expanduser()
        if not path.is_file():
            raise RuntimeError("Chưa thiết lập PostgreSQL. Chạy scripts/run_pipeline.ps1 -Action setup để nhập mật khẩu kín.")
        return json.loads(path.read_text(encoding="utf-8"))

    def connection(self, role="writer"):
        value = self.credentials()
        route = subprocess.check_output(["ip", "-j", "route", "show", "default"], text=True)
        gateway = json.loads(route)[0]["gateway"]
        return {"host": os.getenv("PLCA_POSTGRES_HOST", gateway), "port": value.get("port", 5432),
                "dbname": "premier_league_airflow" if role == "airflow" else "premier_league_analytics",
                "user": value[role]["user"], "password": value[role]["password"], "connect_timeout": 10}
