"""Start this project's NameNode and three DataNodes directly on WSL."""
from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
import time

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from scripts.hdfs_common import HdfsClient, command_error, hdfs_root  # noqa: E402
from scripts.local_hdfs_cluster import LocalCluster  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Khởi động NameNode và 3 DataNode của dự án trực tiếp trên WSL.")
    parser.add_argument("--hdfs-bin", help="Đường dẫn tới lệnh hdfs nếu lệnh này chưa có trong PATH.")
    parser.add_argument("--wait-timeout", type=int, default=180, help="Thời gian tối đa chờ HDFS sẵn sàng (giây).")
    parser.add_argument("--min-datanodes", type=int, default=3, help="Số DataNode tối thiểu cần chờ.")
    parser.add_argument("--dry-run", action="store_true", help="Chỉ in kế hoạch, không khởi động dịch vụ.")
    parser.add_argument("--stop", action="store_true", help="Dừng các dịch vụ HDFS của dự án, giữ nguyên dữ liệu.")
    args = parser.parse_args()
    if args.wait_timeout <= 0 or args.min_datanodes <= 0:
        parser.error("Thời gian chờ và số DataNode tối thiểu phải lớn hơn 0.")
    try:
        client = HdfsClient(dry_run=args.dry_run, hdfs_bin=args.hdfs_bin)
        root = hdfs_root()
        cluster = LocalCluster(client)
        if args.stop:
            if args.dry_run:
                print("[KẾ HOẠCH] Sẽ dừng các dịch vụ HDFS của dự án và giữ nguyên dữ liệu.")
            else:
                cluster.stop()
            return 0
        if args.dry_run:
            print(f"[KẾ HOẠCH] Sẽ khởi động 1 NameNode và 3 DataNode; dữ liệu dịch vụ: {cluster.state}")
        else:
            cluster.start()
            deadline = time.monotonic() + args.wait_timeout
            while True:
                try:
                    client.require_ready(min_datanodes=max(3, args.min_datanodes))
                    break
                except (RuntimeError, subprocess.SubprocessError) as error:
                    if time.monotonic() >= deadline:
                        raise RuntimeError(f"HDFS chưa sẵn sàng: {command_error(error)}") from error
                    time.sleep(3)
        client.init_layout(root)
        client.hdfs("dfsadmin", "-report")
        print("[XONG] Đây là lần chạy thử; chưa khởi động HDFS." if args.dry_run
              else "[SẴN SÀNG] HDFS có 3 DataNode hoạt động, block 64 MiB, replication 2.", flush=True)
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"[LỖI] {command_error(error)}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
