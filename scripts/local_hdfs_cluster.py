"""One NameNode and three DataNodes on this WSL host, without SSH."""
from __future__ import annotations

import os
from pathlib import Path
import socket
import shutil
import subprocess
import xml.etree.ElementTree as ET

from scripts.hdfs_common import HdfsClient, PROJECT_ROOT, setting

NODES = ("namenode", "datanode1", "datanode2", "datanode3")
PORTS = {
    "namenode": (9000, 9870),
    "datanode1": (9864, 9867, 9871),
    "datanode2": (9865, 9868, 9872),
    "datanode3": (9866, 9869, 9873),
}


def write_xml(path: Path, properties: dict[str, str]) -> None:
    root = ET.Element("configuration")
    for name, value in properties.items():
        entry = ET.SubElement(root, "property")
        ET.SubElement(entry, "name").text = name
        ET.SubElement(entry, "value").text = value
    ET.indent(root, space="  ")
    ET.ElementTree(root).write(path, encoding="utf-8", xml_declaration=True)


class LocalCluster:
    def __init__(self, client: HdfsClient):
        self.client = client
        self.state = Path(setting("HDFS_STATE_DIR", str(Path.home() / ".local/share/premier-league-hdfs"))).expanduser().resolve()
        if str(self.state).startswith("/mnt/"):
            raise ValueError("HDFS_STATE_DIR phải nằm trên filesystem Linux, không được đặt trong /mnt/.")

    def environment(self, node: str) -> dict[str, str]:
        environment = self.client.environment.copy()
        environment.update({
            "HADOOP_CONF_DIR": str(self.state / "conf" / node),
            "HADOOP_PID_DIR": str(self.state / "pids" / node),
            "HADOOP_LOG_DIR": str(self.state / "logs" / node),
            "HADOOP_IDENT_STRING": f"premier-league-{node}",
            "HADOOP_HEAPSIZE_MAX": "512" if node == "namenode" else "256",
        })
        # Hadoop's daemon wrapper requires explicit service users when invoked as root.
        if os.geteuid() == 0:
            environment["HDFS_NAMENODE_USER"] = "root"
            environment["HDFS_DATANODE_USER"] = "root"
        return environment

    def command(self, node: str, *arguments: str, capture: bool = False, check: bool = True):
        command = [
            self.client.binary, "--config", str(self.state / "conf" / node), *arguments,
        ]
        daemon_start = len(arguments) >= 2 and arguments[:2] == ("--daemon", "start")
        return subprocess.run(command, env=self.environment(node), check=check,
                              capture_output=capture, text=True, timeout=60,
                              stdin=subprocess.DEVNULL if daemon_start else None,
                              start_new_session=daemon_start)

    def running(self, node: str) -> bool:
        expected = "org.apache.hadoop.hdfs.server." + (
            "namenode.NameNode" if node == "namenode" else "datanode.DataNode"
        )
        for pidfile in (self.state / "pids" / node).glob("*.pid"):
            try:
                pid = int(pidfile.read_text().strip())
                command_line = Path(f"/proc/{pid}/cmdline").read_bytes().decode(errors="replace")
                if expected in command_line:
                    return True
            except (OSError, ValueError):
                continue
        return False

    def configure(self) -> None:
        base = PROJECT_ROOT / "config/hadoop"
        core = {p.findtext("name"): p.findtext("value", "") for p in ET.parse(base / "core-site.xml").findall("property")}
        core["fs.defaultFS"] = "hdfs://localhost:9000"
        hdfs = {
            "dfs.blocksize": "67108864", "dfs.replication": "2",
            "dfs.namenode.name.dir": (self.state / "storage/namenode").as_uri(),
            "dfs.namenode.rpc-address": "localhost:9000",
            "dfs.namenode.rpc-bind-host": "127.0.0.1",
            "dfs.namenode.http-address": "127.0.0.1:9870",
            "dfs.namenode.safemode.min.datanodes": "3",
            "dfs.namenode.safemode.extension": "3000",
            "dfs.namenode.datanode.registration.ip-hostname-check": "false",
            "dfs.datanode.hostname": "localhost", "dfs.client.use.datanode.hostname": "true",
            "dfs.datanode.use.datanode.hostname": "true",
            "dfs.webhdfs.enabled": "true", "dfs.permissions.enabled": "false",
        }
        for node in NODES:
            conf = self.state / "conf" / node
            for directory in (conf, self.state / "logs" / node, self.state / "pids" / node, self.state / "storage" / node):
                directory.mkdir(parents=True, exist_ok=True)
            properties = hdfs.copy()
            if node != "namenode":
                http, transfer, ipc = PORTS[node]
                properties.update({
                    "dfs.datanode.data.dir": (self.state / "storage" / node).as_uri(),
                    "dfs.datanode.http.address": f"127.0.0.1:{http}",
                    "dfs.datanode.address": f"127.0.0.1:{transfer}",
                    "dfs.datanode.ipc.address": f"127.0.0.1:{ipc}",
                })
            write_xml(conf / "core-site.xml", core)
            write_xml(conf / "hdfs-site.xml", properties)
            installed_conf = Path(self.client.binary).resolve().parents[1] / "etc/hadoop"
            for support_file in ("log4j.properties", "hadoop-metrics2.properties"):
                original = installed_conf / support_file
                if original.is_file():
                    shutil.copy2(original, conf / support_file)
        self.client.environment["HADOOP_CONF_DIR"] = str(self.state / "conf/namenode")

    def start(self) -> None:
        # Check for unrelated services before formatting or starting anything.
        for node in NODES:
            if self.running(node):
                continue
            for port in PORTS[node]:
                with socket.socket() as probe:
                    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    try:
                        probe.bind(("127.0.0.1", port))
                    except OSError as error:
                        raise RuntimeError(f"Cổng {port} đang được chương trình khác sử dụng; chưa khởi động {node}.") from error
        self.configure()
        name_dir = self.state / "storage/namenode"
        if not (name_dir / "current/VERSION").is_file():
            if any(name_dir.iterdir()):
                raise RuntimeError(f"Từ chối format thư mục NameNode không rỗng: {name_dir}")
            print(f"[KHỞI TẠO] Tạo metadata NameNode mới cho dự án tại: {name_dir}", flush=True)
            result = self.command("namenode", "namenode", "-format", "-nonInteractive",
                                  "-clusterid", "CID-premier-league-analysis",
                                  capture=True, check=False)
            log = self.state / "logs/namenode/format.log"
            log.write_text(result.stdout + result.stderr)
            if result.returncode:
                raise RuntimeError(f"Khởi tạo NameNode thất bại; xem log tại {log}")
        for node in NODES:
            role = "namenode" if node == "namenode" else "datanode"
            if self.running(node):
                print(f"[ĐANG CHẠY] {node} đã chạy, giữ nguyên tiến trình hiện tại.", flush=True)
            else:
                print(f"[KHỞI ĐỘNG] Đang khởi động {node}...", flush=True)
                self.command(node, "--daemon", "start", role)
        print(f"[THÔNG TIN] Thư mục dữ liệu và log: {self.state}", flush=True)

    def stop(self) -> None:
        for node in reversed(NODES):
            if self.running(node):
                role = "namenode" if node == "namenode" else "datanode"
                self.command(node, "--daemon", "stop", role)
        print("[ĐÃ DỪNG] HDFS của dự án đã dừng; dữ liệu vẫn được giữ nguyên.", flush=True)
