"""Compute reports from the lakehouse and publish only results to Neo4j."""
import argparse
import csv
import json
import os
from pathlib import Path
from . import VERSION, store
from .ingest import ROOT, silver_tables
from .outlook.football_data import FootballData
from .outlook.player_outlook import evaluate, SEASONS
from .outlook.club_phases import analyze as phases


def _publish_with_reader(dataset_key, reader, run_id=None, spark=None):
    output = Path(os.getenv('MATCH_MODEL_DIR', str(ROOT / 'Generate_LeagueTable_Api/analystic/match_outcome/outputs')))
    knn = Path(os.getenv('KNN_OUTPUT_DIR', str(ROOT / 'Generate_LeagueTable_Api/analystic/knn/outputs')))
    forecast_artifact = json.loads((output / 'forecast_v2.json').read_text(encoding='utf-8'))
    run_id = run_id or 'bundle_' + store.digest({'dataset': dataset_key, 'version': VERSION, 'model': json.loads((output / 'model.json').read_text(encoding='utf-8'))['model_version'], 'forecast_v2':forecast_artifact['model_version']})[:24]
    run_key = store.begin_run(run_id, dataset_key)
    completed = store.query('MATCH (r:PLRun {key:$key,status:"complete"}) RETURN r.run_id AS id',{'key':run_key})
    if completed:
        return run_id
    try:
        data = FootballData(reader)
        reports = []
        for season in SEASONS:
            for window in (1, 2, 3):
                reports.append(('outlook', evaluate(data, season, window), {'season': season, 'window': window}))
        reports.append(('phases', phases(data), {}))
        from .transfers import analyze
        transfer_reports = analyze(reader, spark)
        from .dashboard import analyze as dashboard
        reports.append(('dashboard',dashboard(reader,transfer_reports),{}))
        reports += [('transfers', transfer_reports['sql'], {}), ('rfm_gmm', transfer_reports['gmm'], {}), ('rfm_rules', transfer_reports['rules'], {})]
        matches = [json.loads(line) for line in (output / 'matches.jsonl').read_text(encoding='utf-8').splitlines()]
        reports.append(('matches', matches, {}))
        reports.append(('clubs', [{'id': cid, 'name': name} for cid, name in sorted(data.clubs.items()) if any(cid in (m['home_team'], m['away_team']) for m in matches)], {}))
        reports.append(('model', json.loads((output / 'model.json').read_text(encoding='utf-8')), {}))
        reports.append(('model_metrics', json.loads((output / 'metrics.json').read_text(encoding='utf-8')), {}))
        reports.append(('forecast_v2', forecast_artifact, {}))
        reports.append(('forecast_v2_metrics', json.loads((output / 'forecast_v2_metrics.json').read_text(encoding='utf-8')), {}))
        for kind, filename in [('knn_predictions', 'knn_predictions.csv'), ('knn_neighbors', 'knn_neighbors.csv')]:
            with (knn / filename).open(encoding='utf-8-sig', newline='') as stream:
                reports.append((kind, list(csv.DictReader(stream)), {}))
        original_players = reader('datamb/players')
        from collections import Counter
        ids = Counter(p['id'] for p in original_players)
        serving_players = [{**p, 'source_id': p['id'], 'id': p['id'] if ids[p['id']] == 1 else p['id'] + '::' + str(i)} for i, p in enumerate(original_players)]
        from .identity import link_players
        serving_players = link_players(serving_players,data)
        reports.append(('players', serving_players, {}))
        for kind, payload, filters in reports:
            store.save_report(run_key, kind, payload, filters)
        store.finish_run(run_key, len(reports))
        print(json.dumps({'run_id': run_id, 'reports': len(reports), 'status': 'complete'}), flush=True)
        return run_id
    except Exception as exc:
        store.fail_run(run_key, exc)
        raise


def publish(dataset_key, run_id=None, spark=None):
    """Publish reports from a previously registered serving snapshot.

    Source rows are intentionally not copied into Neo4j by this function.
    They remain in HDFS/lakehouse; Neo4j receives only the run metadata and
    report payloads.
    """
    cache = {}

    def reader(name):
        if name not in cache:
            cache[name] = store.table_rows(name, dataset_key)
        return cache[name]

    return _publish_with_reader(dataset_key, reader, run_id, spark)


def publish_from_silver(directory, snapshot_key, run_id=None, spark=None):
    """Compute reports from a Silver snapshot without persisting its rows.

    ``directory`` is a local export of the already validated HDFS Silver
    manifest.  It is used as a bounded reader for the analysis stage only;
    Neo4j stores the resulting reports and provenance, never the source rows.
    """
    cache = {name: rows for name, rows, _ in silver_tables(directory)}
    players = json.loads((ROOT / 'data/datamb_players_db.json').read_text(encoding='utf-8'))

    def reader(name):
        if name == 'datamb/players':
            return players
        if name not in cache:
            raise LookupError('Silver snapshot does not contain ' + name)
        return cache[name]

    return _publish_with_reader(snapshot_key, reader, run_id, spark)


def publish_hdfs(ctx):
    """Export the just-validated HDFS snapshot, not the bundled historical seed."""
    from pl_pipeline.core import clean_json
    folder = ctx.local / 'serving_silver'
    manifest = {'tables': []}
    spark = ctx.session('MergedPublication')
    try:
        for table, info in ctx.manifest['tables'].items():
            if table.startswith('gold/'):
                continue
            frame = spark.read.parquet(info['path'])
            path = folder / 'csv' / (table + '.csv')
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('w', encoding='utf-8-sig', newline='') as stream:
                writer = csv.DictWriter(stream, frame.columns)
                writer.writeheader()
                for row in frame.toLocalIterator():
                    values = row.asDict(recursive=True)
                    writer.writerow({k: '\\N' if v is None else str(v).lower() if isinstance(v, bool) else str(v) if hasattr(v, 'as_tuple') else clean_json(v) for k, v in values.items()})
            manifest['tables'].append({'table': table, 'csv': 'csv/' + table + '.csv', 'keys': info.get('keys', []), 'row_count': info['row_count'], 'schema': info['schema'], 'source_hdfs_path': info['path']})
        (folder / 'silver_manifest.json').write_text(store.encode(manifest), encoding='utf-8')
        # The Silver export is the input boundary for serving.  Keep the
        # lakehouse in HDFS and publish only reports/provenance to Neo4j.
        dataset = store.namespace() + ':silver:' + ctx.run_id
        run_id = publish_from_silver(folder, dataset, ctx.run_id, spark)
        # Store Gold tables on the same existing HDFS, scoped to this pipeline run.
        for kind in ('transfers', 'rfm_gmm', 'rfm_rules'):
            payload, _ = store.report(kind, run_id=run_id)
            spark.createDataFrame([(store.encode(payload),)], 'payload_json string').coalesce(1).write.mode('overwrite').parquet(ctx.gold + '/serving/' + kind)
        return {'serving_run_id': run_id, 'dataset_key': dataset}
    finally:
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--silver-dir')
    args = parser.parse_args()
    if not args.silver_dir:
        raise SystemExit('--silver-dir is required; source data stays in the lakehouse')
    publish_from_silver(args.silver_dir, store.namespace() + ':silver:manual')
