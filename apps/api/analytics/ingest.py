"""Read and validate bounded lakehouse snapshots without persisting source rows."""
import csv
import hashlib
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def sha(path):
    checksum = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):
            checksum.update(block)
    return checksum.hexdigest()


def convert(value, kind):
    if value == '\\N':
        return None
    if kind in ('integer', 'long', 'short', 'byte'):
        return int(value)
    if kind in ('double', 'float'):
        return float(value)
    if kind == 'boolean':
        if value not in ('true', 'false', 'True', 'False'):
            raise ValueError('Invalid boolean: ' + value)
        return value.lower() == 'true'
    # DECIMAL remains a decimal string for exact round trips; dates remain ISO.
    return value


def silver_tables(directory=None):
    directory = Path(directory or os.getenv('SILVER_DATA_DIR', str(ROOT / 'data/lake/silver')))
    manifest = json.loads((directory / 'silver_manifest.json').read_text(encoding='utf-8'))
    for table in manifest['tables']:
        path = directory / table['csv']
        fields = table['schema']['fields']
        kinds = {f['name']: f['type'] for f in fields}
        with path.open(encoding='utf-8-sig', newline='') as stream:
            reader = csv.DictReader(stream)
            if set(reader.fieldnames) != set(kinds):
                raise ValueError('Schema differs: ' + table['table'])
            rows = [{k: convert(v, kinds[k]) for k, v in row.items()} for row in reader]
        if len(rows) != table['row_count']:
            raise ValueError('Row count differs: ' + table['table'])
        yield table['table'], rows, {**table, 'checksum': sha(path), 'source': path.relative_to(directory).as_posix()}


def import_sources(directory=None):
    """Source-row materialization in the serving database is intentionally unsupported."""
    raise RuntimeError("Raw, Bronze and Silver source rows stay in HDFS. Publish only analysis reports to Neon.")
