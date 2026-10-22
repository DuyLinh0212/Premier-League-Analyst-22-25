"""Standard-library bridge for PowerShell 5.1: no inline Python quoting."""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def read_credentials(path):
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_credentials(path, value):
    if set(value) != {"port", "writer", "reader", "airflow"}:
        raise ValueError("Invalid credential fields")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    handle, temporary = tempfile.mkstemp(prefix=".plca-credentials-", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as target:
            json.dump(value, target, ensure_ascii=True)
            target.flush()
            os.fsync(target.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def wsl_network():
    addresses = json.loads(subprocess.check_output(["ip", "-j", "addr", "show", "eth0"], text=True))[0]["addr_info"]
    address = next(item for item in addresses if item["family"] == "inet")
    return str(ipaddress.ip_network(f"{address['local']}/{address['prefixlen']}", strict=False))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("read", "write", "network"))
    parser.add_argument("--file", type=Path, default=Path.home() / ".config/plca/credentials.json")
    args = parser.parse_args()
    if args.action == "read":
        print(json.dumps(read_credentials(args.file), ensure_ascii=True))
    elif args.action == "write":
        write_credentials(args.file, json.loads(sys.stdin.read().lstrip("\ufeff")))
    else:
        print(wsl_network())


if __name__ == "__main__":
    main()
