"""Verify extracted bundle or ZIP without changing files."""
import argparse
import hashlib
import json
import zipfile
from pathlib import Path

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--archive",type=Path)
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[1]
    if args.archive:
        with zipfile.ZipFile(args.archive) as archive:
            manifest=json.loads(archive.read("bundle_manifest.json"))
            for item in manifest["files"]:
                if Path(item["path"]).is_absolute() or ".." in Path(item["path"]).parts:
                    raise ValueError("Invalid bundle path")
                content=archive.read(item["path"])
                assert len(content)==item["bytes"] and hashlib.sha256(content).hexdigest()==item["sha256"],item["path"]
    else:
        manifest=json.loads((root/"bundle_manifest.json").read_text(encoding="utf-8"))
        for item in manifest["files"]:
            path=(root/item["path"]).resolve()
            if not path.is_relative_to(root):
                raise ValueError("Invalid bundle path")
            content=path.read_bytes()
            assert len(content)==item["bytes"] and hashlib.sha256(content).hexdigest()==item["sha256"],item["path"]
    print("Verified",len(manifest["files"]),"files;",manifest["deployment"])

if __name__=="__main__":
    main()
