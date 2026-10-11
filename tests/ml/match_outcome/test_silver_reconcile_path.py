import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from match_outcome.dataset import reconcile_silver
from match_outcome.schema import METRICS, SOURCE_METRICS


class SilverReconciliationPathTests(unittest.TestCase):
    def test_reads_the_exact_run_scoped_match_table_path(self):
        match_path = "hdfs://namenode:9000/premier-league/silver/matches/fact_match_enriched/runs/run-123"
        record = {"match_id": "match-1", "home_goals": 2, "away_goals": 1,
                  "home_possession": 55, "away_possession": 45,
                  "home_shooting_shots": 10, "away_shooting_shots": 8,
                  "home_shooting_shots_on_target": 4, "away_shooting_shots_on_target": 3,
                  "home_shooting_sot_pct": 40, "away_shooting_sot_pct": 37.5,
                  "home_shooting_penalties_attempted": None, "away_shooting_penalties_attempted": None,
                  "home_formation": "4-3-3", "away_formation": "4-4-2"}
        for column in SOURCE_METRICS["miscellaneous_for"].values():
            record[f"home_misc_{column}"] = None
            record[f"away_misc_{column}"] = None

        class Reader:
            def __init__(self):
                self.path = None

            def parquet(self, path):
                self.path = path
                return SimpleNamespace(collect=lambda: [SimpleNamespace(match_id="match-1", asDict=lambda: record)])

        reader = Reader()
        spark = SimpleNamespace(read=reader)
        home_stats = {key: None for key in METRICS}
        home_stats.update(possession=55, shots=10, shots_on_target=4, shot_accuracy=40, formation="4-3-3")
        away_stats = {key: None for key in METRICS}
        away_stats.update(possession=45, shots=8, shots_on_target=3, shot_accuracy=37.5, formation="4-4-2")
        rows = [{"match_id": "match-1", "home_goals": 2, "away_goals": 1,
                 "home_stats": home_stats, "away_stats": away_stats}]

        with tempfile.TemporaryDirectory() as directory:
            result = reconcile_silver(spark, rows, match_path, directory)
            self.assertEqual(reader.path, match_path)
            self.assertEqual(result, rows)
            reconciliation = json.loads((Path(directory) / "silver_reconciliation.json").read_text(encoding="utf-8"))
            self.assertEqual(reconciliation["path"], match_path)
            self.assertTrue(reconciliation["all_features_and_labels_equal"])


if __name__ == "__main__":
    unittest.main()
