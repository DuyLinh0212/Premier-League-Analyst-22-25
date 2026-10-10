"""Restore checksum-verified large raw files carried as gzip, not Git LFS."""
import gzip
import hashlib
import json
import shutil
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]


def main():
    manifest_path = ROOT / 'data/large_sources.json'
    if not manifest_path.exists():
        return
    for item in json.loads(manifest_path.read_text(encoding='utf-8')):
        target = (ROOT / item['source']).resolve()
        if not target.is_relative_to(ROOT):
            raise ValueError('Source path escapes project')
        if target.exists():
            if hashlib.sha256(target.read_bytes()).hexdigest() != item['sha256']:
                raise ValueError('Existing raw file checksum mismatch')
            continue
        compressed = ROOT / item['compressed']
        staging = target.with_suffix(target.suffix + '.part')
        target.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(compressed, 'rb') as source, staging.open('wb') as output:
            shutil.copyfileobj(source, output)
        if staging.stat().st_size != item['bytes'] or hashlib.sha256(staging.read_bytes()).hexdigest() != item['sha256']:
            raise ValueError('Expanded raw file checksum mismatch')
        staging.replace(target)


if __name__ == '__main__':
    main()
