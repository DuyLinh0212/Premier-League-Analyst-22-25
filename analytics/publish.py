"""Compute reports from HDFS Silver and publish only results to Neon."""
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


def _publish_with_reader(dataset_key, reader, run_id=None, spark=None, defer_neon=False, pending_path=None):
    output = Path(os.getenv('MATCH_MODEL_DIR', str(ROOT / 'Generate_LeagueTable_Api/analystic/match_outcome/outputs')))
    knn = Path(os.getenv('KNN_OUTPUT_DIR', str(ROOT / 'Generate_LeagueTable_Api/analystic/knn/outputs')))
    forecast_artifact = json.loads((output / 'forecast_v2.json').read_text(encoding='utf-8'))
    run_id = run_id or 'bundle_' + store.digest({'dataset': dataset_key, 'version': VERSION, 'model': json.loads((output / 'model.json').read_text(encoding='utf-8'))['model_version'], 'forecast_v2':forecast_artifact['model_version']})[:24]
    neon = None
    if os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL"):
        from . import neon_store as neon
        if neon.run_complete(run_id):
            return run_id
        if not defer_neon:
            neon.begin_run(run_id, dataset_key, source_manifest={"source": "HDFS Silver", "dataset": dataset_key})
    else:
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
        if neon:
            if defer_neon:
                if pending_path is None:
                    raise ValueError("A pending report path is required when Neon publication is deferred")
                Path(pending_path).write_text(json.dumps(reports, ensure_ascii=False, allow_nan=False), encoding="utf-8")
            else:
                neon.publish_reports(run_id, dataset_key, reports,
                                      {"source": "HDFS Silver", "dataset": dataset_key,
                                       "report_version": VERSION})
        else:
            for kind, payload, filters in reports:
                store.save_report(run_key, kind, payload, filters)
            store.finish_run(run_key, len(reports))
        print(json.dumps({'run_id': run_id, 'reports': len(reports), 'status': 'complete'}), flush=True)
        return run_id
    except Exception as exc:
        if neon and not defer_neon:
            with neon._connect() as connection, connection.cursor() as cursor:
                cursor.execute("UPDATE pl_analysis_runs SET status='failed',error=%s,completed_at=now() WHERE run_id=%s AND status<>'complete'", (str(exc)[:1000], run_id))
        elif not neon:
            store.fail_run(run_key, exc)
        raise


def publish(dataset_key, run_id=None, spark=None):
    """Publish reports from a previously registered serving snapshot.

    Source rows are intentionally not copied into a serving database by this
    function. They remain in HDFS/lakehouse; only run metadata and reports are
    published.
    """
    cache = {}

    def reader(name):
        if name not in cache:
            cache[name] = store.table_rows(name, dataset_key)
        return cache[name]

    return _publish_with_reader(dataset_key, reader, run_id, spark)


def publish_from_silver(directory, snapshot_key, run_id=None, spark=None, defer_neon=False):
    """Compute reports from a Silver snapshot without persisting its rows.

    ``directory`` is a local export of the already validated HDFS Silver
    manifest.  It is used as a bounded reader for the analysis stage only;
    Neon stores the resulting reports and provenance, never the source rows.
    """
    cache = {name: rows for name, rows, _ in silver_tables(directory)}
    players = json.loads((ROOT / 'data/datamb_players_db.json').read_text(encoding='utf-8'))

    def reader(name):
        if name == 'datamb/players':
            return players
        if name not in cache:
            raise LookupError('Silver snapshot does not contain ' + name)
        return cache[name]

    pending_path = Path(directory) / "_pending_neon_reports.json" if defer_neon else None
    return _publish_with_reader(snapshot_key, reader, run_id, spark, defer_neon, pending_path)


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
        # lakehouse in HDFS and publish only reports/provenance to Neon.
        dataset = store.namespace() + ':silver:' + ctx.run_id
        use_neon = bool(os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL"))
        run_id = publish_from_silver(folder, dataset, ctx.run_id, spark, defer_neon=use_neon)
        # Store all derived Gold reports on HDFS; Neon is the report serving database.
        from . import neon_store
        report_reader = neon_store.report if use_neon else store.report
        pending_file = folder / "_pending_neon_reports.json"
        pending_reports = None
        if use_neon and pending_file.exists():
            pending_reports = json.loads(pending_file.read_text(encoding="utf-8"))

        def read_report(kind, filters=None):
            if pending_reports is not None:
                filter_json = store.encode(filters or {})
                for saved_kind, payload, saved_filters in pending_reports:
                    if saved_kind == kind and store.encode(saved_filters or {}) == filter_json:
                        return payload, {"run_id": run_id, "version": VERSION, "status": "pending_hdfs_gold"}
                raise LookupError(f"Analysis payload was not staged for Gold: {kind}")
            return report_reader(kind, filters or {}, run_id)
        groups = {
            'outlook': ('player_performance', 'season_comparison'), 'players': 'player_performance',
            'phases': 'tactical_insights', 'dashboard': 'team_performance',
            'transfers': 'transfer_impact', 'rfm_gmm': 'transfer_impact', 'rfm_rules': 'transfer_impact',
            'matches': 'prediction_features', 'model': 'prediction_features', 'model_metrics': 'prediction_features',
            'forecast_v2': 'prediction_features', 'forecast_v2_metrics': 'prediction_features',
            'knn_predictions': 'league_standings', 'knn_neighbors': 'league_standings',
        }
        for kind, group in groups.items():
            if kind == 'outlook':
                for season in ('2022-2023','2023-2024','2024-2025'):
                    for window in (1, 2, 3):
                        payload, _ = read_report(kind, {'season': season, 'window': window})
                        for group in groups[kind]:
                            target = f"{ctx.gold}/{group}/{kind}/runs/{run_id}/season={season}/window={window}"
                            spark.createDataFrame([(store.encode(payload),)], 'payload_json string').coalesce(1).write.mode('overwrite').parquet(target)
                continue
            payload, _ = read_report(kind, {})
            target_group = groups[kind]
            target = f"{ctx.gold}/{target_group}/{kind}/runs/{run_id}"
            spark.createDataFrame([(store.encode(payload),)], 'payload_json string').coalesce(1).write.mode('overwrite').parquet(target)
        if pending_reports is not None:
            neon_store.publish_reports(run_id, dataset, pending_reports,
                                       {"source": "HDFS Silver", "dataset": dataset,
                                        "report_version": VERSION, "gold_path": ctx.gold})
            pending_file.unlink()
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
