import json
import math
import unittest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient
from match_outcome.schema import validate_stats, LEAKAGE_HEADERS
from match_outcome.inference import load_artifact, load_published_artifact, predict
from match_outcome.api import app

OUTPUT = Path(__file__).resolve().parents[1] / "outputs"

class OutcomeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifact = load_artifact(OUTPUT)
        cls.rows = [json.loads(line) for line in (OUTPUT / "matches.jsonl").read_text(encoding="utf-8").splitlines()]

    def test_all_matches_and_probability_contract(self):
        self.assertEqual(len(self.rows), 1140)
        self.assertEqual(len({r["match_id"] for r in self.rows}), 1140)
        for row in self.rows:
            p = list(predict(row, self.artifact)["probabilities"].values())
            self.assertTrue(all(math.isfinite(x) and 0 <= x <= 1 for x in p))
            self.assertAlmostEqual(sum(p), 1, places=12)

    def test_no_outcome_features(self):
        catalog = json.loads((OUTPUT / "feature_catalog.json").read_text(encoding="utf-8"))
        self.assertTrue(all(r["column"] not in LEAKAGE_HEADERS for r in catalog if r["role"] == "feature"))
        self.assertFalse(any(r["is_simulated"] for r in self.rows))
        report = json.loads((OUTPUT / "metrics.json").read_text(encoding="utf-8"))
        self.assertEqual(report["split_counts"], {"train": 380, "tune": 190, "calibration": 190, "test": 380})
        self.assertLessEqual(report["spark_numpy_max_absolute_error"], 1e-8)

    def test_algorithm_selection_uses_validation(self):
        report = json.loads((OUTPUT / "metrics.json").read_text(encoding="utf-8"))
        comparison = report["comparison"]
        self.assertEqual({r['algorithm'] for r in comparison}, {'logistic_regression','knn_classifier','random_forest'})
        selected = min(comparison,key=lambda r:r['validation_log_loss'])
        self.assertEqual(self.artifact['algorithm'],selected['algorithm'])
        self.assertAlmostEqual(report['selected_validation_log_loss'],selected['validation_log_loss'])
        if selected['algorithm']=='random_forest':
            self.assertEqual(self.artifact['hyperparameters'],selected['parameters'])
        self.assertNotIn('test_accuracy',selected)

    def test_unpublished_model_is_not_used(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            published=directory/'versions'/self.artifact['model_version']
            published.mkdir(parents=True)
            (published/'model.json').write_text(json.dumps(self.artifact),encoding='utf-8')
            future=dict(self.artifact,temperature=self.artifact['temperature']*2,model_version='wdl_'+'0'*20)
            (directory/'model.json').write_text(json.dumps(future),encoding='utf-8')
            (directory/'published.json').write_text(json.dumps({'model_version':self.artifact['model_version']}),encoding='utf-8')
            selected=predict(self.rows[0],load_published_artifact(directory))
            self.assertEqual(selected,predict(self.rows[0],self.artifact))
            self.assertNotEqual(selected['probabilities'],predict(self.rows[0],future)['probabilities'])

    def test_missing_and_invalid_statistics(self):
        row = dict(self.rows[0])
        row["home_stats"] = {"possession": 50, "shots": 10, "shots_on_target": 3}
        result = predict(row, self.artifact)
        self.assertIn("home.yellow_cards", result["imputed_fields"])
        for bad in [{"possession": 101, "shots": 10, "shots_on_target": 3},
                    {"possession": 50, "shots": 2, "shots_on_target": 3},
                    {"possession": 50, "shots": -1, "shots_on_target": 0},
                    {"possession": float("nan"), "shots": 1, "shots_on_target": 0},
                    {"possession": 50, "shots": 1.5, "shots_on_target": 0},
                    {"possession": 50, "shots": 1001, "shots_on_target": 0},
                    {"shots": 10, "shots_on_target": 3}]:
            with self.assertRaises(ValueError):
                validate_stats(bad)

    def test_api_ack_and_failure(self):
        row = self.rows[0]
        payload = {k: row[k] for k in ["home_team", "away_team", "home_stats", "away_stats"]}
        def report(kind,*args,**kwargs):
            if kind == 'model': value = self.artifact
            elif kind == 'clubs': value = [{'id':k,'name':k} for k in {r[side] for r in self.rows for side in ('home_team','away_team')}]
            else:
                import csv
                with (OUTPUT.parent.parent/'knn/outputs'/('knn_predictions.csv' if kind=='knn_predictions' else 'knn_neighbors.csv')).open(encoding='utf-8-sig') as f:
                    value = list(csv.DictReader(f))
            return value, {'run_id':'test_complete_run'}
        with patch('analytics.store.report', side_effect=report), patch('match_outcome.api.save_user_analysis', side_effect=lambda kind,params,result,sim: {**result,'persistence':'saved'}) as save, TestClient(app,raise_server_exceptions=False) as client:
            producer = Mock()
            producer.send.return_value.get.return_value = object()
            app.state.producer = producer
            response = client.post("/predict", json=payload)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()['persistence'], 'saved')
            save.assert_called_once()
            sent = producer.send.call_args.kwargs["value"]
            self.assertEqual(sent["model_version"], response.json()["model_version"])
            self.assertEqual(sent["kind"], "scenario")
            save.side_effect = ConnectionError('DB offline')
            self.assertEqual(client.post('/predict',json=payload).status_code,503)
            producer.send.side_effect = ConnectionError("offline")
            self.assertEqual(client.post("/predict", json=payload).status_code, 503)
            bad = dict(payload, away_team=payload["home_team"])
            self.assertEqual(client.post("/predict", json=bad).status_code, 422)
            self.assertEqual(len(client.get("/knn/predictions").json()), 20)
            self.assertEqual(len(client.get("/knn/neighbors/ARS").json()), 5)

if __name__ == "__main__":
    unittest.main()
