"""One-time source integration; provenance captures inputs without modifying them."""
import ast
import csv
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT.parent


def main():
    sql = {}
    inputs = []
    for relative in ['Danh/processing/spark/step_05_spark_sql_analysis.py', 'Danh/processing/spark/step_07_save_analytics_hdfs.py',
                     'Danh/processing/spark/step_06_rfm_clustering.py', 'Danh/processing/spark/generate_visual_dashboard.py']:
        path = SOURCE / relative
        source = path.read_text(encoding='utf-8-sig')
        inputs.append({'source': relative, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                for target in node.targets:
                    if isinstance(target, ast.Name) and (target.id.startswith('sql_q') or target.id in ('net_spend_sql', 'roi_sql')):
                        sql[target.id] = node.value.value.strip()
    (ROOT / 'apps/api/analytics/transfer_queries.json').write_text(json.dumps(sql, ensure_ascii=False, indent=2), encoding='utf-8')
    simulator_path = SOURCE / 'Danh/streaming/fastapi_transfer_service.py'
    simulator_ast = ast.parse(simulator_path.read_text(encoding='utf-8-sig'))
    nodes = [n for n in simulator_ast.body if (isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in ('PL_CLUBS', 'FOREIGN_CLUBS', 'PLAYERS_CATALOG') for t in n.targets)) or (isinstance(n, ast.FunctionDef) and n.name == 'generate_simulated_transfers')]
    simulator = '\n\n'.join(ast.unparse(n) for n in nodes)
    simulator = simulator.replace('season: str=', 'season: str=')
    simulator = simulator.replace("season: str='2025-2026')", "season: str='2025-2026', seed: int=42)")
    simulator = simulator.replace('records = []', 'records = []\n    rng = random.Random(seed)').replace('random.choice(', 'rng.choice(').replace('random.randint(', 'rng.randint(').replace('random.uniform(', 'rng.uniform(')
    simulator = simulator.replace('datetime(2025, 6, 1)', "datetime(int(season[:4]), 6, 1)")
    (ROOT / 'apps/api/analytics/simulator.py').write_text('"""Transfer simulation adapted from Danh; seeded and isolated from historical data."""\nimport random\nfrom datetime import datetime, timedelta\nfrom typing import List\n\n' + simulator + '\n', encoding='utf-8')
    inputs.append({'source': 'Danh/streaming/fastapi_transfer_service.py', 'sha256': hashlib.sha256(simulator_path.read_bytes()).hexdigest()})
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
    print('Verified', len(checks), 'identical overlapping Silver tables; extracted', len(sql), 'SQL reports')


if __name__ == '__main__':
    main()
