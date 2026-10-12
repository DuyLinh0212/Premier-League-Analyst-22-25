"""One-time source integration; provenance captures inputs without modifying them."""
import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT.parent


def main():
    inputs = []
    checks = []
    for path in sorted((SOURCE / 'football_analysis_share/silver_import_20261008/csv').rglob('*.csv')):
        relative = path.relative_to(SOURCE / 'football_analysis_share/silver_import_20261008/csv')
        target = ROOT / 'data/lake/silver/csv' / relative
        with path.open(encoding='utf-8-sig', newline='') as a, target.open(encoding='utf-8-sig', newline='') as b:
            first, second = list(csv.DictReader(a)), list(csv.DictReader(b))
        if first != second:
            raise ValueError('Conflicting Silver source: ' + str(relative))
        checks.append({'table': relative.as_posix(), 'rows': len(first), 'identical_rows': True})
    for relative in ['football_analysis_share/src/football_data.py', 'football_analysis_share/src/player_outlook.py',
                     'football_analysis_share/src/club_phases.py', 'Danh/data_silver/datamb_players_db.json']:
        inputs.append({'source': relative, 'sha256': hashlib.sha256((SOURCE / relative).read_bytes()).hexdigest()})
    (ROOT / 'data/integration_manifest.json').write_text(json.dumps({'sources': inputs, 'silver_overlap': checks}, ensure_ascii=False, indent=2), encoding='utf-8')
    print('Verified', len(checks), 'identical overlapping Silver tables and retained analytics sources')


if __name__ == '__main__':
    main()
