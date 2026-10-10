"""Read lakehouse snapshots; legacy graph row import is explicit opt-in only."""
import csv
import hashlib
import json
import os
from pathlib import Path
from . import store

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
    """Legacy raw-row importer, disabled for the serving pipeline.

    The production flow keeps raw/Silver data in HDFS.  Keeping this helper
    behind an explicit opt-in prevents an accidental multi-million-row graph
    load while retaining it for controlled migration/fixture work.
    """
    if os.getenv('STORE_SOURCE_ROWS_IN_NEO4J', '').lower() not in {'1', 'true', 'yes'}:
        raise RuntimeError('Raw source rows stay in HDFS; use publish_from_silver or explicitly opt in to STORE_SOURCE_ROWS_IN_NEO4J')
    from infrastructure.scripts.build.expand_raw import main as expand_raw
    expand_raw()
    fixture_limit = int(os.getenv('RAW_FIXTURE_LIMIT','0'))
    if fixture_limit < 0 or fixture_limit and not store.namespace().startswith('ci-'):
        raise ValueError('RAW_FIXTURE_LIMIT is reserved for explicit ci-* namespaces')
    silver = list(silver_tables(directory))
    inputs = []
    for folder in ('data/raw/fbref', 'data/raw/transfermarkt/source_tables'):
        for path in sorted((ROOT / folder).rglob('*.csv')):
            relative = path.relative_to(ROOT).as_posix()
            inputs.append((path, relative, sha(path)))
    datamb = ROOT / 'data/raw/datamb/players.json'
    metadata = {'silver': [{'name': n, 'rows': len(r), 'checksum': m['checksum'], 'schema': m['schema'], 'keys': m.get('keys',[])} for n, r, m in silver],
                'raw': [{'name': r, 'checksum': h} for p, r, h in inputs], 'datamb_checksum': sha(datamb), 'raw_fixture_limit':fixture_limit}
    dataset_id = 'dataset_' + store.digest(metadata)[:24]
    store.schema()
    dataset_key = store.begin_dataset(dataset_id, metadata)
    for name, rows, info in silver:
        store.import_table(dataset_key, name, rows, info)
    for path, relative, checksum in inputs:
        with path.open(encoding='utf-8-sig', newline='') as stream:
            reader = csv.reader(stream)
            header = next(reader, [])
            row_count = sum(1 for row in reader)
        original_count = row_count
        if fixture_limit:
            row_count = min(row_count,fixture_limit)
        with path.open(encoding='utf-8-sig', newline='') as stream:
            reader = csv.reader(stream)
            next(reader, [])
            rows = ({'line': i + 2, 'values': row} for i, row in enumerate(reader))
            if fixture_limit:
                from itertools import islice
                rows = islice(rows,fixture_limit)
            store.import_table(dataset_key, 'raw/' + relative, rows,
                               {'checksum': checksum, 'header': header, 'keys': ['line'], 'source': relative, 'row_count':row_count,'source_row_count':original_count,'is_fixture':bool(fixture_limit)})
        print(json.dumps({'imported':relative,'rows':row_count}), flush=True)
    players = json.loads(datamb.read_text(encoding='utf-8'))
    store.import_table(dataset_key, 'datamb/players', players, {'checksum': sha(datamb), 'keys': [], 'source': 'Danh/data_silver/datamb_players_db.json', 'note': 'Original IDs can repeat across club spells; preserve every source row by ordinal.'})
    store.finish_dataset(dataset_key, len(silver) + len(inputs) + 1)
    print(json.dumps({'dataset': dataset_key, 'silver_rows': sum(len(r) for n, r, m in silver), 'raw_tables': len(inputs), 'datamb_players': len(players)}), flush=True)
    return dataset_key
